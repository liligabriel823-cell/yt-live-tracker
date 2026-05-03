const express = require('express');
const xml2js  = require('xml2js');
const fetch   = require('node-fetch');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;

const videos   = new Map();
const channels = new Map();
const logs     = [];
const TEN_MIN  = 10 * 60 * 1000;

function getBaseUrl() {
  if (process.env.RAILWAY_PUBLIC_DOMAIN) return `https://${process.env.RAILWAY_PUBLIC_DOMAIN}`;
  if (process.env.SERVER_URL) return process.env.SERVER_URL;
  return `http://localhost:${PORT}`;
}

function addLog(msg) {
  logs.unshift({ t: new Date().toISOString(), msg });
  if (logs.length > 100) logs.pop();
  console.log(msg);
}

function cleanExpired() {
  const now = Date.now();
  for (const [id, v] of videos) {
    if (v.expiresAt < now) { videos.delete(id); addLog(`Expired: ${v.title?.slice(0,40)}`); }
  }
}
setInterval(cleanExpired, 30_000);

// ── Middleware ─────────────────────────────────────────────────
// CORS — allow everything
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// Raw body for XML (PubSub)
app.use((req, res, next) => {
  const ct = req.headers['content-type'] || '';
  if (ct.includes('xml') || ct.includes('atom')) {
    let d = '';
    req.on('data', c => d += c);
    req.on('end', () => { req.xmlBody = d; next(); });
  } else next();
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── PubSub ─────────────────────────────────────────────────────
const HUB = 'https://pubsubhubbub.appspot.com/subscribe';

async function pubsub(channelId, mode = 'subscribe') {
  const base     = getBaseUrl();
  const callback = `${base}/pubsub`;
  const topic    = `https://www.youtube.com/xml/feeds/videos.xml?channel_id=${channelId}`;
  const body     = new URLSearchParams({
    'hub.callback': callback, 'hub.topic': topic,
    'hub.verify': 'async', 'hub.mode': mode,
    'hub.lease_seconds': '432000',
  });
  try {
    const r = await fetch(HUB, { method: 'POST', body });
    addLog(`PubSub ${mode} ${channelId} → ${r.status}`);
    return r.status === 202;
  } catch(e) {
    addLog(`PubSub error: ${e.message}`);
    return false;
  }
}

// Re-subscribe every 4 days
setInterval(async () => {
  for (const id of channels.keys()) { await pubsub(id); await new Promise(r=>setTimeout(r,200)); }
}, 4*24*60*60*1000);

// ── PubSub Webhook ─────────────────────────────────────────────
app.get('/pubsub', (req, res) => {
  const ch = req.query['hub.challenge'];
  if (ch) { addLog('PubSub verified ✓'); return res.send(ch); }
  res.sendStatus(200);
});

app.post('/pubsub', async (req, res) => {
  res.sendStatus(200);
  const body = req.xmlBody;
  if (!body) return;
  try {
    const p   = await new xml2js.Parser({ explicitArray: false }).parseStringPromise(body);
    const ent = p?.feed?.entry;
    if (!ent) return;
    const videoId   = ent['yt:videoId'] || '';
    const channelId = ent['yt:channelId'] || ent?.author?.uri?.split('/channel/')[1] || '';
    const title     = ent.title || 'Untitled';
    const published = ent.published || new Date().toISOString();
    const updated   = ent.updated   || published;
    if (!videoId) return;
    if (new Date(updated) - new Date(published) > 5*60*1000) return; // skip edits
    const ch = channels.get(channelId);
    videos.set(videoId, {
      videoId, channelId,
      channelName: ch?.name || channelId,
      title,
      url:        `https://www.youtube.com/watch?v=${videoId}`,
      thumb:      `https://img.youtube.com/vi/${videoId}/mqdefault.jpg`,
      publishedAt: published,
      receivedAt:  new Date().toISOString(),
      expiresAt:   Date.now() + TEN_MIN,
    });
    addLog(`NEW [${ch?.name||channelId}]: ${title.slice(0,60)}`);
  } catch(e) { addLog(`Parse error: ${e.message}`); }
});

// ── API ────────────────────────────────────────────────────────
app.get('/api/stats', (req, res) => {
  cleanExpired();
  res.json({ status:'running', channelsTracked:channels.size, videosLive:videos.size, uptime:Math.floor(process.uptime()), serverUrl:getBaseUrl(), logs:logs.slice(0,20) });
});

app.get('/api/channels', (req, res) => res.json([...channels.values()]));

app.post('/api/channels', async (req, res) => {
  const { channelId, name } = req.body;
  if (!channelId) return res.status(400).json({ error:'channelId required' });
  if (channels.has(channelId)) return res.status(409).json({ error:'Already tracked' });
  channels.set(channelId, { channelId, name:name||channelId, addedAt:new Date().toISOString() });
  const ok = await pubsub(channelId);
  if (!ok) { channels.delete(channelId); return res.status(500).json({ error:'PubSub subscribe failed' }); }
  res.json({ ok:true, channel:channels.get(channelId) });
});

app.delete('/api/channels/:id', async (req, res) => {
  const id = req.params.id;
  if (!channels.has(id)) return res.status(404).json({ error:'Not found' });
  channels.delete(id);
  for (const [vid,v] of videos) { if (v.channelId===id) videos.delete(vid); }
  await pubsub(id, 'unsubscribe');
  res.json({ ok:true });
});

app.post('/api/channels/bulk', async (req, res) => {
  const list = req.body.channels;
  if (!Array.isArray(list)) return res.status(400).json({ error:'channels[] required' });
  let added=0, skipped=0, failed=0;
  for (const { channelId, name } of list) {
    if (!channelId) continue;
    if (channels.has(channelId)) { skipped++; continue; }
    channels.set(channelId, { channelId, name:name||channelId, addedAt:new Date().toISOString() });
    const ok = await pubsub(channelId);
    if (ok) added++; else { channels.delete(channelId); failed++; }
    await new Promise(r=>setTimeout(r,150));
  }
  res.json({ added, skipped, failed });
});

app.get('/api/videos', (req, res) => {
  cleanExpired();
  const now  = Date.now();
  const list = [...videos.values()]
    .filter(v => v.expiresAt > now)
    .map(v => ({ ...v, minutesLeft:Math.ceil((v.expiresAt-now)/60000), ageSeconds:Math.floor((now-new Date(v.receivedAt).getTime())/1000) }))
    .sort((a,b) => new Date(b.receivedAt)-new Date(a.receivedAt));
  res.json({ count:list.length, videos:list });
});

// ── Dashboard (served from server itself — no CORS) ────────────
const DASHBOARD = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>YT Live Tracker</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
:root{--bg:#0c0c0c;--s1:#141414;--s2:#1c1c1c;--s3:#242424;--b1:#2a2a2a;--b2:#383838;--t1:#efefef;--t2:#999;--t3:#555;--red:#ff3333;--rdim:#1c0404;--green:#22c55e;--gdim:#061510;--yellow:#f59e0b;--blue:#3b82f6;--bdim:#060f1e;--r:6px;--rl:10px;}
body{font-family:'IBM Plex Sans',sans-serif;background:var(--bg);color:var(--t1);min-height:100vh;}
::-webkit-scrollbar{width:4px;}::-webkit-scrollbar-thumb{background:var(--b2);border-radius:2px;}
.nav{background:var(--s1);border-bottom:1px solid var(--b1);padding:0 18px;height:50px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100;}
.logo{display:flex;align-items:center;gap:9px;}
.logo-icon{width:24px;height:24px;background:var(--red);border-radius:4px;display:flex;align-items:center;justify-content:center;}
.logo-icon::after{content:'';border-left:8px solid #fff;border-top:5px solid transparent;border-bottom:5px solid transparent;margin-left:2px;}
.logo-text{font-family:'IBM Plex Mono',monospace;font-size:13px;font-weight:600;}
.logo-sub{font-size:10px;color:var(--t3);font-family:'IBM Plex Mono',monospace;}
.nav-mid{display:flex;align-items:center;gap:6px;}
.cdot{width:7px;height:7px;border-radius:50%;background:var(--green);animation:pulse 2s infinite;}
@keyframes pulse{0%,100%{opacity:1;}50%{opacity:.3;}}
.ctxt{font-size:11px;color:var(--t2);font-family:'IBM Plex Mono',monospace;}
.nav-right{display:flex;gap:6px;}
.btn{font-family:'IBM Plex Mono',monospace;font-size:11px;font-weight:600;padding:6px 12px;border-radius:var(--r);border:1px solid var(--b2);background:var(--s2);color:var(--t2);cursor:pointer;transition:all .15s;white-space:nowrap;}
.btn:hover{background:var(--s3);color:var(--t1);}
.btn-red{background:var(--rdim);color:var(--red);border-color:var(--red);}
.btn-red:hover{background:var(--red);color:#fff;}
.btn-green{background:var(--gdim);color:var(--green);border-color:var(--green);}
.btn-green:hover{background:var(--green);color:#000;}
.btn-blue{background:var(--bdim);color:var(--blue);border-color:var(--blue);}
.btn-blue:hover{background:var(--blue);color:#fff;}
.btn-sm{padding:5px 10px;font-size:10px;}
.strip{background:var(--s1);border-bottom:1px solid var(--b1);padding:5px 18px;display:flex;align-items:center;gap:16px;flex-wrap:wrap;}
.chip{font-size:11px;font-family:'IBM Plex Mono',monospace;color:var(--t3);display:flex;align-items:center;gap:4px;}
.chip b{color:var(--t1);}
.chip-r b{color:var(--red);}
.chip-y b{color:var(--yellow);}
.ml{margin-left:auto;display:flex;align-items:center;gap:7px;font-size:11px;font-family:'IBM Plex Mono',monospace;color:var(--t3);}
.rfbar{width:70px;height:2px;background:var(--b1);border-radius:1px;overflow:hidden;}
.rffill{height:100%;background:var(--red);transition:width 1s linear;}
.layout{display:flex;height:calc(100vh - 50px - 30px);}
.sb{width:285px;flex-shrink:0;background:var(--s1);border-right:1px solid var(--b1);display:flex;flex-direction:column;overflow:hidden;}
.sb-sec{padding:11px 13px;border-bottom:1px solid var(--b1);flex-shrink:0;}
.sb-lbl{font-size:10px;font-weight:600;color:var(--t3);text-transform:uppercase;letter-spacing:.07em;font-family:'IBM Plex Mono',monospace;margin-bottom:7px;}
.row{display:flex;gap:5px;}
.inp{flex:1;padding:7px 10px;font-size:11px;font-family:'IBM Plex Mono',monospace;border:1px solid var(--b2);border-radius:var(--r);background:var(--s2);color:var(--t1);}
.inp:focus{outline:none;border-color:var(--blue);}
.inp::placeholder{color:var(--t3);}
.hint{font-size:10px;color:var(--t3);font-family:'IBM Plex Mono',monospace;margin-top:5px;line-height:1.6;}
.hint b{color:var(--t2);}
.sb-hdr{padding:6px 13px;font-size:10px;font-weight:600;color:var(--t3);text-transform:uppercase;letter-spacing:.07em;font-family:'IBM Plex Mono',monospace;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--b1);flex-shrink:0;}
.badge{background:var(--s2);color:var(--t2);padding:1px 7px;border-radius:20px;font-size:10px;}
.sb-search{padding:7px 13px;border-bottom:1px solid var(--b1);flex-shrink:0;}
.ch-list{flex:1;overflow-y:auto;}
.ch-item{display:flex;align-items:center;gap:7px;padding:7px 13px;border-bottom:1px solid var(--b1);transition:background .1s;}
.ch-item:hover{background:var(--s2);}
.ch-av{width:26px;height:26px;border-radius:50%;background:var(--s3);display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:600;color:var(--t2);flex-shrink:0;}
.ch-info{flex:1;min-width:0;}
.ch-name{font-size:12px;font-weight:600;color:var(--t1);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.ch-id{font-size:9px;color:var(--t3);font-family:'IBM Plex Mono',monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.ch-del{width:20px;height:20px;border-radius:4px;background:none;border:none;color:var(--t3);cursor:pointer;font-size:11px;display:flex;align-items:center;justify-content:center;flex-shrink:0;}
.ch-del:hover{background:var(--rdim);color:var(--red);}
.main{flex:1;overflow-y:auto;display:flex;flex-direction:column;}
.feed-top{padding:10px 16px;border-bottom:1px solid var(--b1);display:flex;align-items:center;justify-content:space-between;background:var(--s1);position:sticky;top:0;z-index:2;flex-shrink:0;}
.ft-t{font-size:13px;font-weight:600;font-family:'IBM Plex Mono',monospace;}
.ft-m{font-size:10px;color:var(--t3);font-family:'IBM Plex Mono',monospace;margin-top:1px;}
.stats-bar{padding:10px 16px;border-bottom:1px solid var(--b1);display:flex;gap:20px;background:var(--s1);flex-shrink:0;}
.ss{text-align:center;}
.ss-v{font-size:20px;font-weight:600;font-family:'IBM Plex Mono',monospace;display:block;}
.ss-l{font-size:10px;color:var(--t3);text-transform:uppercase;letter-spacing:.05em;}
.fbar{padding:7px 16px;border-bottom:1px solid var(--b1);display:flex;gap:5px;align-items:center;flex-wrap:wrap;background:var(--s1);flex-shrink:0;}
.fc{font-size:11px;padding:3px 11px;border-radius:20px;border:1px solid var(--b2);background:none;color:var(--t2);cursor:pointer;font-family:'IBM Plex Mono',monospace;transition:all .15s;}
.fc:hover{color:var(--t1);}
.fc.on{background:var(--red);color:#fff;border-color:var(--red);}
.fr{margin-left:auto;}
.vsrch{padding:5px 10px;font-size:11px;font-family:'IBM Plex Mono',monospace;border:1px solid var(--b1);border-radius:var(--r);background:var(--s2);color:var(--t1);width:155px;}
.vsrch:focus{outline:none;}
.vsrch::placeholder{color:var(--t3);}
.vlist{padding:10px 14px;display:flex;flex-direction:column;gap:7px;}
.vc{background:var(--s1);border:1px solid var(--b1);border-radius:var(--rl);padding:11px 13px;display:flex;gap:11px;transition:border-color .15s;animation:fu .2s ease;}
.vc:hover{border-color:var(--b2);}
.vc.hot{border-left:3px solid var(--red);}
@keyframes fu{from{opacity:0;transform:translateY(4px);}to{opacity:1;transform:translateY(0);}}
.vc-thumb{flex-shrink:0;position:relative;width:118px;}
.vc-img{width:118px;height:66px;border-radius:5px;object-fit:cover;background:var(--s2);display:block;}
.vc-age{position:absolute;bottom:3px;right:3px;font-family:'IBM Plex Mono',monospace;font-size:9px;font-weight:600;padding:2px 5px;border-radius:3px;}
.vc-age.f{background:var(--red);color:#fff;}
.vc-age.o{background:rgba(0,0,0,.8);color:var(--t2);}
.vc-ttl{position:absolute;top:3px;left:3px;font-family:'IBM Plex Mono',monospace;font-size:9px;font-weight:600;background:rgba(0,0,0,.75);color:var(--yellow);padding:1px 5px;border-radius:3px;}
.vc-body{flex:1;min-width:0;}
.vc-ch{font-size:10px;color:var(--t3);font-family:'IBM Plex Mono',monospace;margin-bottom:3px;}
.new-tag{display:inline-block;font-size:9px;font-weight:700;background:var(--red);color:#fff;padding:2px 6px;border-radius:3px;margin-bottom:4px;font-family:'IBM Plex Mono',monospace;animation:fl 1s ease 5;}
@keyframes fl{0%,100%{opacity:1;}50%{opacity:.2;}}
.vc-title{font-size:13px;font-weight:600;color:var(--t1);line-height:1.35;margin-bottom:5px;}
.vc-title a{color:inherit;text-decoration:none;}
.vc-title a:hover{color:var(--blue);}
.vc-url{font-size:11px;color:var(--blue);font-family:'IBM Plex Mono',monospace;text-decoration:none;word-break:break-all;display:block;margin-bottom:5px;}
.vc-url:hover{text-decoration:underline;}
.vc-meta{display:flex;gap:12px;flex-wrap:wrap;}
.vm{font-size:10px;color:var(--t3);font-family:'IBM Plex Mono',monospace;}
.vm b{color:var(--t2);}
.vm.yw b{color:var(--yellow);}
.empty{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:10px;padding:2rem;text-align:center;}
.empty-icon{font-size:36px;}
.empty-title{font-size:14px;font-weight:600;font-family:'IBM Plex Mono',monospace;}
.empty-desc{font-size:11px;color:var(--t2);max-width:300px;line-height:1.7;font-family:'IBM Plex Mono',monospace;}
.tw{position:fixed;bottom:16px;right:16px;z-index:999;display:flex;flex-direction:column;gap:5px;}
.toast{font-size:12px;font-family:'IBM Plex Mono',monospace;padding:9px 14px;border-radius:var(--r);border:1px solid var(--b2);background:var(--s2);color:var(--t1);animation:tIn .2s ease;max-width:270px;}
.toast.ok{background:var(--gdim);border-color:var(--green);color:var(--green);}
.toast.err{background:var(--rdim);border-color:var(--red);color:var(--red);}
@keyframes tIn{from{opacity:0;transform:translateY(5px);}to{opacity:1;transform:translateY(0);}}
.mbg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.8);z-index:500;align-items:center;justify-content:center;}
.mbg.open{display:flex;}
.modal{background:var(--s1);border:1px solid var(--b2);border-radius:var(--rl);padding:22px;max-width:430px;width:100%;margin:1rem;}
.modal h3{font-size:14px;font-weight:600;font-family:'IBM Plex Mono',monospace;margin-bottom:8px;}
.modal p{font-size:12px;color:var(--t2);margin-bottom:14px;line-height:1.7;}
.mfoot{display:flex;gap:7px;justify-content:flex-end;margin-top:10px;}
.bta{width:100%;height:130px;padding:9px;font-size:11px;font-family:'IBM Plex Mono',monospace;border:1px solid var(--b2);border-radius:var(--r);background:var(--s2);color:var(--t1);resize:vertical;}
.bta:focus{outline:none;border-color:var(--blue);}
.bta::placeholder{color:var(--t3);}
.mnote{font-size:10px;color:var(--t3);font-family:'IBM Plex Mono',monospace;margin-top:5px;}
</style>
</head>
<body>
<nav class="nav">
  <div class="logo"><div class="logo-icon"></div><div><div class="logo-text">YT Live Tracker</div><div class="logo-sub">10-min push window</div></div></div>
  <div class="nav-mid"><div class="cdot"></div><span class="ctxt" id="ctxt">Connected</span></div>
  <div class="nav-right">
    <button class="btn btn-sm" onclick="openBulk()">⊕ Bulk Add</button>
    <button class="btn btn-sm btn-red" onclick="pollNow()">↻ Refresh</button>
  </div>
</nav>
<div class="strip">
  <div class="chip">Channels: <b id="scC">0</b></div>
  <div class="chip chip-r">Live now: <b id="scV">0</b></div>
  <div class="chip chip-y">Window: <b>10 min</b></div>
  <div class="ml"><span id="arTxt">—</span><div class="rfbar"><div class="rffill" id="rfF" style="width:100%"></div></div></div>
</div>
<div class="layout">
  <aside class="sb">
    <div class="sb-sec">
      <div class="sb-lbl">➕ Add Channel</div>
      <div class="row">
        <input class="inp" type="text" id="chInp" placeholder="UCxxxxxxxxxxxxxxxxxx" onkeydown="if(event.key==='Enter')addCh()">
        <button class="btn btn-sm btn-red" onclick="addCh()">Add</button>
      </div>
      <div class="hint"><b>Channel ID</b> starts with UC<br>YouTube → About → Share → Copy channel ID</div>
    </div>
    <div class="sb-hdr"><span>Channels</span><span class="badge" id="chB">0</span></div>
    <div class="sb-search"><input class="inp" style="width:100%" type="text" placeholder="Search…" oninput="renderChs(this.value)"></div>
    <div class="ch-list" id="chList"></div>
  </aside>
  <main class="main" id="main"></main>
</div>

<div class="mbg" id="bulkMod">
  <div class="modal">
    <h3>Bulk Add Channels</h3>
    <p>One per line: <code style="color:var(--yellow);background:var(--s2);padding:1px 5px;border-radius:3px;">UCxxxxxxxx,Name</code> (name optional)</p>
    <textarea class="bta" id="bulkTa" placeholder="UCddiUEpeqJcYeBxX1IVBKvQ,MrBeast&#10;UCX6OQ3DkcsbYNE6H8uQQuVA,MKBHD&#10;UCxxxxxxxxxxxxxxxxxxxxxx"></textarea>
    <div class="mnote" id="bulkNote">0 channels</div>
    <div class="mfoot"><button class="btn" onclick="closeBulk()">Cancel</button><button class="btn btn-blue" onclick="doBulk()">Add All</button></div>
  </div>
</div>
<div class="mbg" id="delMod">
  <div class="modal">
    <h3>Remove Channel?</h3>
    <p>Stop tracking <strong id="delName"></strong>?</p>
    <div class="mfoot"><button class="btn" onclick="closeDel()">Cancel</button><button class="btn btn-red" onclick="doDel()">Remove</button></div>
  </div>
</div>
<div class="tw" id="tw"></div>

<script>
const BASE = window.location.origin;
let chs=[],vids=[],timer=null,cd=30,fq='',delId=null;

async function api(path,opts={}){
  const r=await fetch(BASE+path,opts);
  const d=await r.json();
  if(!r.ok) throw new Error(d.error||'HTTP '+r.status);
  return d;
}

async function init(){
  document.getElementById('ctxt').textContent='Connected to '+BASE;
  await syncChs();
  startPoll();
}

async function syncChs(){
  try{
    chs=await api('/api/channels');
    renderChs();
    document.getElementById('chB').textContent=chs.length;
    document.getElementById('scC').textContent=chs.length;
  }catch(e){}
}

async function addCh(){
  let id=document.getElementById('chInp').value.trim();
  if(!id){toast('Enter Channel ID','err');return;}
  const m=id.match(/channel\\/(UC[A-Za-z0-9_-]+)/);
  if(m) id=m[1];
  if(!id.startsWith('UC')){toast('ID must start with UC','err');return;}
  document.getElementById('chInp').value='';
  toast('Adding…');
  try{
    await api('/api/channels',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({channelId:id,name:id})});
    await syncChs();
    toast('Added & subscribed!','ok');
    renderFeed();
  }catch(e){toast('Error: '+e.message,'err');}
}

function askDel(id,name){delId=id;document.getElementById('delName').textContent=name;document.getElementById('delMod').classList.add('open');}
function closeDel(){document.getElementById('delMod').classList.remove('open');delId=null;}
async function doDel(){
  if(!delId)return;
  try{await api('/api/channels/'+encodeURIComponent(delId),{method:'DELETE'});await syncChs();await pollNow();toast('Removed','ok');}
  catch(e){toast('Error: '+e.message,'err');}
  closeDel();
}

function openBulk(){document.getElementById('bulkMod').classList.add('open');document.getElementById('bulkTa').oninput=function(){const n=parseBulk(this.value).length;document.getElementById('bulkNote').textContent=n+' channels';};}
function closeBulk(){document.getElementById('bulkMod').classList.remove('open');}
function parseBulk(t){return t.split('\\n').map(l=>l.trim()).filter(Boolean).map(l=>{const[id,name]=l.split(',').map(s=>s.trim());return{channelId:id,name:name||id};}).filter(c=>c.channelId);}
async function doBulk(){
  const list=parseBulk(document.getElementById('bulkTa').value);
  if(!list.length){toast('No channels','err');return;}
  closeBulk();toast('Importing '+list.length+'…');
  try{
    const r=await api('/api/channels/bulk',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({channels:list})});
    await syncChs();toast('Added:'+r.added+' Skipped:'+r.skipped,'ok');renderFeed();
  }catch(e){toast('Error: '+e.message,'err');}
}

async function pollNow(){
  try{
    const d=await api('/api/videos');
    vids=d.videos||[];
    document.getElementById('scV').textContent=vids.length;
    renderFeed();cd=30;
  }catch(e){document.getElementById('ctxt').textContent='Lost connection…';}
}

function startPoll(){
  cd=30;if(timer)clearInterval(timer);
  pollNow();
  timer=setInterval(()=>{
    cd--;
    document.getElementById('rfF').style.width=((cd/30)*100)+'%';
    document.getElementById('arTxt').textContent='refresh in '+cd+'s';
    if(cd<=0){cd=30;pollNow();}
  },1000);
}

function renderChs(q=''){
  const el=document.getElementById('chList');
  const list=q?chs.filter(c=>(c.name+c.channelId).toLowerCase().includes(q.toLowerCase())):chs;
  if(!list.length){el.innerHTML='<div style="padding:18px;text-align:center;font-size:11px;color:var(--t3);font-family:IBM Plex Mono,monospace;">'+(chs.length?'No match':'No channels yet')+'</div>';return;}
  const hv=(id)=>vids.some(v=>v.channelId===id);
  el.innerHTML=list.map(c=>\`<div class="ch-item"><div class="ch-av">\${(c.name||c.channelId)[0].toUpperCase()}</div><div class="ch-info"><div class="ch-name">\${h(c.name||c.channelId)}</div><div class="ch-id">\${h(c.channelId)}</div>\${hv(c.channelId)?'<span style="font-size:9px;color:var(--red);font-family:IBM Plex Mono,monospace;">● live</span>':''}</div><button class="ch-del" onclick="askDel('\${h(c.channelId)}','\${h((c.name||c.channelId).replace(/'/g,''))}')">✕</button></div>\`).join('');
}

function renderFeed(){
  const el=document.getElementById('main');
  let show=vids.slice();
  if(fq==='fresh')show=vids.filter(v=>v.ageSeconds<180);
  else if(fq)show=vids.filter(v=>(v.title+v.channelName).toLowerCase().includes(fq.toLowerCase()));
  const fresh=vids.filter(v=>v.ageSeconds<180).length;
  el.innerHTML=\`<div class="feed-top"><div><div class="ft-t">Live Feed — Last 10 Minutes</div><div class="ft-m">\${chs.length} channels · PubSub · auto 30s</div></div><button class="btn btn-sm btn-red" onclick="pollNow()">↻</button></div>
  <div class="stats-bar"><div class="ss"><span class="ss-v" style="color:var(--red)">\${vids.length}</span><span class="ss-l">Live</span></div><div class="ss"><span class="ss-v" style="color:var(--red)">\${fresh}</span><span class="ss-l">&lt;3min</span></div><div class="ss"><span class="ss-v">\${chs.length}</span><span class="ss-l">Channels</span></div><div class="ss"><span class="ss-v" style="color:var(--yellow)">10m</span><span class="ss-l">Window</span></div></div>
  <div class="fbar"><button class="fc \${!fq||fq==='?'?'on':''}" onclick="fq='';renderFeed()">All (\${vids.length})</button><button class="fc \${fq==='fresh'?'on':''}" onclick="fq='fresh';renderFeed()">Just posted (\${fresh})</button><div class="fr"><input class="vsrch" type="text" placeholder="Search…" value="\${h(fq==='fresh'?'':fq)}" oninput="fq=this.value;renderFeed()"></div></div>
  <div class="vlist">\${show.length?show.map(vc).join(''):emptyState()}</div>\`;
}

function vc(v){
  const age=v.ageSeconds,isFresh=age<180;
  const as=age<60?age+'s ago':Math.floor(age/60)+'m '+age%60+'s ago';
  return \`<div class="vc \${isFresh?'hot':''}"><div class="vc-thumb"><img class="vc-img" src="\${h(v.thumb)}" loading="lazy" onerror="this.style.opacity='.2'"><div class="vc-age \${isFresh?'f':'o'}">\${as}</div><div class="vc-ttl">\${v.minutesLeft}m left</div></div><div class="vc-body">\${isFresh?'<div class="new-tag">🔴 JUST POSTED</div>':''}<div class="vc-ch">\${h(v.channelName||v.channelId)}</div><div class="vc-title"><a href="\${h(v.url)}" target="_blank">\${h(v.title)}</a></div><a class="vc-url" href="\${h(v.url)}" target="_blank">\${h(v.url)}</a><div class="vc-meta"><div class="vm">Published: <b>\${new Date(v.publishedAt).toLocaleTimeString()}</b></div><div class="vm">Received: <b>\${new Date(v.receivedAt).toLocaleTimeString()}</b></div><div class="vm yw">Expires: <b>\${v.minutesLeft}m</b></div></div></div></div>\`;
}

function emptyState(){
  if(!chs.length)return'<div class="empty"><div class="empty-icon">📭</div><div class="empty-title">No channels added</div><div class="empty-desc">Add Channel IDs in the sidebar.</div></div>';
  return'<div class="empty"><div class="empty-icon">😴</div><div class="empty-title">No new videos yet</div><div class="empty-desc">Watching '+chs.length+' channels. YouTube will push instantly when someone uploads.</div></div>';
}

function h(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function toast(msg,type=''){const w=document.getElementById('tw');const el=document.createElement('div');el.className='toast'+(type?' '+type:'');el.textContent=msg;w.appendChild(el);setTimeout(()=>el.remove(),3500);}

init();
</script>
</body>
</html>`;

app.get('/', (req, res) => res.send(DASHBOARD));

app.listen(PORT, '0.0.0.0', () => {
  addLog(`Server started on port ${PORT} — ${getBaseUrl()}`);
});
