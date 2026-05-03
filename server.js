const express = require('express');
const fetch   = require('node-fetch');
const xml2js  = require('xml2js');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── State ─────────────────────────────────────────────────────
const channels = new Map(); // channelId → {channelId, name, addedAt}
const videos   = new Map(); // videoId   → {videoId, channelId, channelName, title, url, thumb, publishedAt, receivedAt, expiresAt}
const logs     = [];

const TEN_MIN     = 10 * 60 * 1000;
const POLL_EVERY  = 60 * 1000;       // check RSS every 60 seconds
const BATCH_SIZE  = 20;              // process 20 channels at a time concurrently

let isPolling    = false;
let lastPollTime = null;
let pollCount    = 0;

// ── Helpers ───────────────────────────────────────────────────
function log(msg) {
  const entry = { t: new Date().toISOString(), msg };
  logs.unshift(entry);
  if (logs.length > 200) logs.pop();
  console.log(`[${entry.t}] ${msg}`);
}

function cleanExpired() {
  const now = Date.now();
  let removed = 0;
  for (const [id, v] of videos) {
    if (v.expiresAt < now) { videos.delete(id); removed++; }
  }
  if (removed > 0) log(`Cleaned ${removed} expired videos`);
}

function getBaseUrl() {
  if (process.env.RAILWAY_PUBLIC_DOMAIN) return `https://${process.env.RAILWAY_PUBLIC_DOMAIN}`;
  if (process.env.SERVER_URL) return process.env.SERVER_URL;
  return `http://localhost:${PORT}`;
}

// ── RSS Fetch for one channel ─────────────────────────────────
async function fetchChannelRSS(channelId) {
  const url = `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`;
  try {
    const res = await fetch(url, {
      timeout: 8000,
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; YTTracker/1.0)' }
    });
    if (!res.ok) return [];
    const xml  = await res.text();
    const data = await new xml2js.Parser({ explicitArray: false }).parseStringPromise(xml);
    const entries = data?.feed?.entry;
    if (!entries) return [];

    const list = Array.isArray(entries) ? entries : [entries];
    const cutoff = Date.now() - TEN_MIN;
    const results = [];

    for (const entry of list) {
      const published = entry.published || '';
      const pubTime   = new Date(published).getTime();
      if (isNaN(pubTime) || pubTime < cutoff) continue; // older than 10 min — skip

      const videoId = entry['yt:videoId'] || '';
      if (!videoId) continue;
      if (videos.has(videoId)) continue; // already tracked

      const ch = channels.get(channelId) || { name: channelId };
      results.push({
        videoId,
        channelId,
        channelName : ch.name,
        title       : entry.title || 'Untitled',
        url         : `https://www.youtube.com/watch?v=${videoId}`,
        thumb       : `https://img.youtube.com/vi/${videoId}/mqdefault.jpg`,
        publishedAt : published,
        receivedAt  : new Date().toISOString(),
        expiresAt   : Date.now() + TEN_MIN,
      });
    }
    return results;
  } catch (e) {
    return [];
  }
}

// ── Main Poll Loop ────────────────────────────────────────────
async function pollAll() {
  if (isPolling || channels.size === 0) return;
  isPolling    = true;
  lastPollTime = new Date();
  pollCount++;

  cleanExpired();

  const ids      = [...channels.keys()];
  let   newCount = 0;

  // Process in batches to avoid overwhelming
  for (let i = 0; i < ids.length; i += BATCH_SIZE) {
    const batch   = ids.slice(i, i + BATCH_SIZE);
    const results = await Promise.all(batch.map(id => fetchChannelRSS(id)));

    for (const found of results.flat()) {
      videos.set(found.videoId, found);
      newCount++;
      log(`NEW [${found.channelName}]: ${found.title.slice(0, 60)}`);
    }

    // Small delay between batches to be gentle on YouTube
    if (i + BATCH_SIZE < ids.length) {
      await new Promise(r => setTimeout(r, 500));
    }
  }

  if (newCount > 0) log(`Poll #${pollCount}: ${newCount} new videos from ${ids.length} channels`);
  isPolling = false;
}

// Start polling every 60 seconds
setInterval(pollAll, POLL_EVERY);
// Also run immediately on start after 5s delay
setTimeout(pollAll, 5000);

// ── Self-Ping (keep Railway awake) ────────────────────────────
setInterval(async () => {
  try {
    await fetch(`${getBaseUrl()}/api/ping`);
  } catch(e) {}
}, 5 * 60 * 1000); // ping self every 5 minutes

// ── Middleware ────────────────────────────────────────────────
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});
app.use(express.json());

// ── API ───────────────────────────────────────────────────────
app.get('/api/ping', (req, res) => res.json({ ok: true }));

app.get('/api/stats', (req, res) => {
  cleanExpired();
  res.json({
    status        : 'running',
    channelsCount : channels.size,
    videosLive    : videos.size,
    lastPoll      : lastPollTime,
    pollCount,
    isPolling,
    uptime        : Math.floor(process.uptime()),
    logs          : logs.slice(0, 30),
  });
});

app.get('/api/channels', (req, res) => {
  res.json([...channels.values()]);
});

app.post('/api/channels', (req, res) => {
  const { channelId, name } = req.body;
  if (!channelId) return res.status(400).json({ error: 'channelId required' });
  if (!channelId.startsWith('UC')) return res.status(400).json({ error: 'Channel ID must start with UC' });
  if (channels.has(channelId)) return res.status(409).json({ error: 'Already tracked' });
  channels.set(channelId, { channelId, name: name || channelId, addedAt: new Date().toISOString() });
  log(`Added channel: ${name || channelId}`);
  // Immediately check this new channel
  fetchChannelRSS(channelId).then(found => {
    for (const v of found) videos.set(v.videoId, v);
  });
  res.json({ ok: true });
});

app.delete('/api/channels/:id', (req, res) => {
  const id = decodeURIComponent(req.params.id);
  if (!channels.has(id)) return res.status(404).json({ error: 'Not found' });
  channels.delete(id);
  for (const [vid, v] of videos) { if (v.channelId === id) videos.delete(vid); }
  log(`Removed channel: ${id}`);
  res.json({ ok: true });
});

app.post('/api/channels/bulk', async (req, res) => {
  const list = req.body.channels;
  if (!Array.isArray(list)) return res.status(400).json({ error: 'channels[] required' });
  let added = 0, skipped = 0, invalid = 0;
  for (const { channelId, name } of list) {
    if (!channelId) continue;
    if (!channelId.startsWith('UC')) { invalid++; continue; }
    if (channels.has(channelId)) { skipped++; continue; }
    channels.set(channelId, { channelId, name: name || channelId, addedAt: new Date().toISOString() });
    added++;
  }
  log(`Bulk added: ${added} channels`);
  // Trigger poll for new channels
  setTimeout(pollAll, 1000);
  res.json({ added, skipped, invalid });
});

app.get('/api/videos', (req, res) => {
  cleanExpired();
  const now  = Date.now();
  const list = [...videos.values()]
    .filter(v => v.expiresAt > now)
    .map(v => ({
      ...v,
      minutesLeft : Math.ceil((v.expiresAt - now) / 60000),
      ageSeconds  : Math.floor((now - new Date(v.publishedAt).getTime()) / 1000),
    }))
    .sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
  res.json({ count: list.length, videos: list });
});

// Force immediate poll
app.post('/api/poll', async (req, res) => {
  res.json({ ok: true, message: 'Poll started' });
  await pollAll();
});

// ── Dashboard HTML (served from server — no CORS ever) ────────
const HTML = `<!DOCTYPE html>
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
.nav-right{display:flex;gap:6px;align-items:center;}
.cdot{width:7px;height:7px;border-radius:50%;background:var(--green);animation:pulse 2s infinite;}
.ctxt{font-size:11px;color:var(--t2);font-family:'IBM Plex Mono',monospace;margin-right:8px;}
@keyframes pulse{0%,100%{opacity:1;}50%{opacity:.3;}}
.btn{font-family:'IBM Plex Mono',monospace;font-size:11px;font-weight:600;padding:6px 12px;border-radius:var(--r);border:1px solid var(--b2);background:var(--s2);color:var(--t2);cursor:pointer;transition:all .15s;white-space:nowrap;}
.btn:hover{background:var(--s3);color:var(--t1);}
.btn-red{background:var(--rdim);color:var(--red);border-color:var(--red);}
.btn-red:hover{background:var(--red);color:#fff;}
.btn-blue{background:var(--bdim);color:var(--blue);border-color:var(--blue);}
.btn-blue:hover{background:var(--blue);color:#fff;}
.btn-sm{padding:5px 10px;font-size:10px;}
.strip{background:var(--s1);border-bottom:1px solid var(--b1);padding:5px 18px;display:flex;align-items:center;gap:16px;flex-wrap:wrap;}
.chip{font-size:11px;font-family:'IBM Plex Mono',monospace;color:var(--t3);display:flex;align-items:center;gap:4px;}
.chip b{color:var(--t1);}
.chip-r b{color:var(--red);}
.chip-g b{color:var(--green);}
.ml{margin-left:auto;display:flex;align-items:center;gap:7px;font-size:11px;font-family:'IBM Plex Mono',monospace;color:var(--t3);}
.rfbar{width:80px;height:3px;background:var(--b1);border-radius:1px;overflow:hidden;}
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
.hint a{color:var(--blue);}
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
.empty-desc{font-size:11px;color:var(--t2);max-width:320px;line-height:1.7;font-family:'IBM Plex Mono',monospace;}
.tw{position:fixed;bottom:16px;right:16px;z-index:999;display:flex;flex-direction:column;gap:5px;}
.toast{font-size:12px;font-family:'IBM Plex Mono',monospace;padding:9px 14px;border-radius:var(--r);border:1px solid var(--b2);background:var(--s2);color:var(--t1);animation:tIn .2s ease;max-width:280px;}
.toast.ok{background:var(--gdim);border-color:var(--green);color:var(--green);}
.toast.err{background:var(--rdim);border-color:var(--red);color:var(--red);}
@keyframes tIn{from{opacity:0;transform:translateY(5px);}to{opacity:1;transform:translateY(0);}}
.mbg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.8);z-index:500;align-items:center;justify-content:center;}
.mbg.open{display:flex;}
.modal{background:var(--s1);border:1px solid var(--b2);border-radius:var(--rl);padding:22px;max-width:440px;width:100%;margin:1rem;}
.modal h3{font-size:14px;font-weight:600;font-family:'IBM Plex Mono',monospace;margin-bottom:8px;}
.modal p{font-size:12px;color:var(--t2);margin-bottom:14px;line-height:1.7;}
.mfoot{display:flex;gap:7px;justify-content:flex-end;margin-top:10px;}
.bta{width:100%;height:140px;padding:9px;font-size:11px;font-family:'IBM Plex Mono',monospace;border:1px solid var(--b2);border-radius:var(--r);background:var(--s2);color:var(--t1);resize:vertical;}
.bta:focus{outline:none;border-color:var(--blue);}
.bta::placeholder{color:var(--t3);}
.mnote{font-size:10px;color:var(--t3);font-family:'IBM Plex Mono',monospace;margin-top:5px;}
.poll-anim{display:inline-block;animation:spin .8s linear infinite;}
@keyframes spin{to{transform:rotate(360deg);}}
</style>
</head>
<body>
<nav class="nav">
  <div class="logo"><div class="logo-icon"></div><div><div class="logo-text">YT Live Tracker</div><div class="logo-sub">RSS poll every 60s · 10-min window</div></div></div>
  <div class="nav-right">
    <div class="cdot"></div>
    <span class="ctxt" id="ctxt">Connected</span>
    <button class="btn btn-sm" onclick="openBulk()">⊕ Bulk Add</button>
    <button class="btn btn-sm btn-red" onclick="forcePoll()" id="pollBtn">↻ Poll Now</button>
  </div>
</nav>
<div class="strip">
  <div class="chip">Channels: <b id="scC">0</b></div>
  <div class="chip chip-r">Live videos: <b id="scV">0</b></div>
  <div class="chip chip-g">Window: <b>10 min</b></div>
  <div class="chip">Last poll: <b id="lastPoll">—</b></div>
  <div class="ml"><span id="arTxt">next poll in —</span><div class="rfbar"><div class="rffill" id="rfF" style="width:100%"></div></div></div>
</div>
<div class="layout">
  <aside class="sb">
    <div class="sb-sec">
      <div class="sb-lbl">➕ Add Channel</div>
      <div class="row">
        <input class="inp" type="text" id="chInp" placeholder="UCxxxxxxxxxxxxxxxxxx" onkeydown="if(event.key==='Enter')addCh()">
        <button class="btn btn-sm btn-red" onclick="addCh()">Add</button>
      </div>
      <div class="hint">
        <b>Channel ID</b> starts with UC<br>
        Find it: YouTube → channel page<br>
        → About → Share → <b>Copy channel ID</b><br>
        Or from URL: youtube.com/channel/<b>UCxxxx</b>
      </div>
    </div>
    <div class="sb-hdr"><span>Tracked Channels</span><span class="badge" id="chB">0</span></div>
    <div class="sb-search"><input class="inp" style="width:100%" type="text" placeholder="Search channels…" oninput="renderChs(this.value)"></div>
    <div class="ch-list" id="chList"></div>
  </aside>
  <main class="main" id="main">
    <div class="empty"><div class="empty-icon">📡</div><div class="empty-title">Loading…</div></div>
  </main>
</div>

<div class="mbg" id="bulkMod">
  <div class="modal">
    <h3>Bulk Add Channels</h3>
    <p>One per line. Format: <code style="color:var(--yellow);background:var(--s2);padding:1px 5px;border-radius:3px;">UCxxxxxxxx,Channel Name</code><br>Name is optional — just ID also works.</p>
    <textarea class="bta" id="bulkTa" placeholder="UCddiUEpeqJcYeBxX1IVBKvQ,MrBeast&#10;UCX6OQ3DkcsbYNE6H8uQQuVA,MKBHD&#10;UCBcRF18a7Qf58cCRy5xuWwQ&#10;...paste 1000+ here"></textarea>
    <div class="mnote" id="bulkNote">0 channels detected</div>
    <div class="mfoot">
      <button class="btn" onclick="closeBulk()">Cancel</button>
      <button class="btn btn-blue" onclick="doBulk()">Add All</button>
    </div>
  </div>
</div>
<div class="mbg" id="delMod">
  <div class="modal">
    <h3>Remove Channel?</h3>
    <p>Stop tracking <strong id="delName"></strong>? Their videos will be removed from feed.</p>
    <div class="mfoot">
      <button class="btn" onclick="closeDel()">Cancel</button>
      <button class="btn btn-red" onclick="doDel()">Remove</button>
    </div>
  </div>
</div>
<div class="tw" id="tw"></div>

<script>
const BASE = window.location.origin;
let chs=[],vids=[],uiTimer=null,uiCd=60,fq='',delId=null;

async function api(path,opts={}){
  const r=await fetch(BASE+path,{...opts,signal:AbortSignal.timeout(10000)});
  const d=await r.json();
  if(!r.ok) throw new Error(d.error||'HTTP '+r.status);
  return d;
}

async function init(){
  await syncChs();
  await refreshVids();
  startUiTimer();
}

async function syncChs(){
  try{
    chs=await api('/api/channels');
    renderChs();
    document.getElementById('chB').textContent=chs.length;
    document.getElementById('scC').textContent=chs.length;
  }catch(e){}
}

async function refreshVids(){
  try{
    const d=await api('/api/videos');
    vids=d.videos||[];
    document.getElementById('scV').textContent=vids.length;
    const stats=await api('/api/stats');
    if(stats.lastPoll){
      const ago=Math.floor((Date.now()-new Date(stats.lastPoll).getTime())/1000);
      document.getElementById('lastPoll').textContent=ago<60?ago+'s ago':Math.floor(ago/60)+'m ago';
    }
    document.getElementById('ctxt').textContent='Connected · poll #'+stats.pollCount;
    renderFeed();
  }catch(e){document.getElementById('ctxt').textContent='Reconnecting…';}
}

function startUiTimer(){
  uiCd=60;
  if(uiTimer)clearInterval(uiTimer);
  uiTimer=setInterval(async()=>{
    uiCd--;
    document.getElementById('rfF').style.width=((uiCd/60)*100)+'%';
    document.getElementById('arTxt').textContent='next poll in '+uiCd+'s';
    if(uiCd<=0){uiCd=60;await refreshVids();}
  },1000);
}

async function forcePoll(){
  const btn=document.getElementById('pollBtn');
  btn.innerHTML='<span class="poll-anim">↻</span> Polling…';
  btn.disabled=true;
  try{
    await api('/api/poll',{method:'POST'});
    await new Promise(r=>setTimeout(r,3000));
    await refreshVids();
    toast('Poll complete!','ok');
  }catch(e){toast('Error: '+e.message,'err');}
  btn.innerHTML='↻ Poll Now';
  btn.disabled=false;
  uiCd=60;
}

async function addCh(){
  let id=document.getElementById('chInp').value.trim();
  if(!id){toast('Enter Channel ID','err');return;}
  const m=id.match(/channel\\/(UC[A-Za-z0-9_-]+)/);
  if(m)id=m[1];
  if(!id.startsWith('UC')){toast('ID must start with UC\\nFind it: YouTube → About → Share → Copy channel ID','err');return;}
  document.getElementById('chInp').value='';
  toast('Adding…');
  try{
    await api('/api/channels',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({channelId:id,name:id})});
    await syncChs();
    setTimeout(refreshVids,4000);
    toast('Added! Checking for recent videos…','ok');
  }catch(e){toast('Error: '+e.message,'err');}
}

function askDel(id,name){delId=id;document.getElementById('delName').textContent=name;document.getElementById('delMod').classList.add('open');}
function closeDel(){document.getElementById('delMod').classList.remove('open');delId=null;}
async function doDel(){
  if(!delId)return;
  try{await api('/api/channels/'+encodeURIComponent(delId),{method:'DELETE'});await syncChs();await refreshVids();toast('Removed','ok');}
  catch(e){toast('Error: '+e.message,'err');}
  closeDel();
}

function openBulk(){
  document.getElementById('bulkMod').classList.add('open');
  document.getElementById('bulkTa').oninput=function(){
    const n=parseBulk(this.value).length;
    document.getElementById('bulkNote').textContent=n+' channels detected';
  };
}
function closeBulk(){document.getElementById('bulkMod').classList.remove('open');}
function parseBulk(t){return t.split('\\n').map(l=>l.trim()).filter(Boolean).map(l=>{const[id,name]=l.split(',').map(s=>s.trim());return{channelId:id,name:name||id};}).filter(c=>c.channelId&&c.channelId.startsWith('UC'));}
async function doBulk(){
  const list=parseBulk(document.getElementById('bulkTa').value);
  if(!list.length){toast('No valid UC... IDs found','err');return;}
  closeBulk();toast('Importing '+list.length+' channels…');
  try{
    const r=await api('/api/channels/bulk',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({channels:list})});
    await syncChs();
    toast('Added:'+r.added+' Skipped:'+r.skipped+' Invalid:'+r.invalid,'ok');
    setTimeout(refreshVids,4000);
  }catch(e){toast('Error: '+e.message,'err');}
}

function renderChs(q=''){
  const el=document.getElementById('chList');
  const list=q?chs.filter(c=>(c.name+c.channelId).toLowerCase().includes(q.toLowerCase())):chs;
  if(!list.length){el.innerHTML='<div style="padding:18px;text-align:center;font-size:11px;color:var(--t3);font-family:IBM Plex Mono,monospace;">'+(chs.length?'No match':'No channels yet — add above')+'</div>';return;}
  const hv=(id)=>vids.some(v=>v.channelId===id);
  el.innerHTML=list.map(c=>'<div class="ch-item"><div class="ch-av">'+(c.name||c.channelId)[0].toUpperCase()+'</div><div class="ch-info"><div class="ch-name">'+h(c.name||c.channelId)+'</div><div class="ch-id">'+h(c.channelId)+'</div>'+(hv(c.channelId)?'<span style="font-size:9px;color:var(--red);font-family:IBM Plex Mono,monospace;">● live</span>':'')+'</div><button class="ch-del" onclick="askDel(\''+h(c.channelId)+'\',\''+h((c.name||c.channelId).replace(/'/g,''))+'\')">\u2715</button></div>').join('');
}

function renderFeed(){
  const el=document.getElementById('main');
  let show=vids.slice();
  if(fq==='fresh')show=vids.filter(v=>v.ageSeconds<180);
  else if(fq)show=vids.filter(v=>(v.title+v.channelName).toLowerCase().includes(fq.toLowerCase()));
  const fresh=vids.filter(v=>v.ageSeconds<180).length;
  el.innerHTML=
    '<div class="feed-top"><div><div class="ft-t">Live Feed — Last 10 Minutes</div><div class="ft-m">'+chs.length+' channels · RSS poll every 60s · auto-refresh</div></div><button class="btn btn-sm btn-red" onclick="forcePoll()" id="pollBtn">&#8635; Poll Now</button></div>'+
    '<div class="stats-bar"><div class="ss"><span class="ss-v" style="color:var(--red)">'+vids.length+'</span><span class="ss-l">Live Now</span></div><div class="ss"><span class="ss-v" style="color:var(--red)">'+fresh+'</span><span class="ss-l">&lt;3 min</span></div><div class="ss"><span class="ss-v">'+chs.length+'</span><span class="ss-l">Channels</span></div><div class="ss"><span class="ss-v" style="color:var(--yellow)">10m</span><span class="ss-l">Window</span></div></div>'+
    '<div class="fbar"><button class="fc '+(fq===''?'on':'')+'" onclick="fq=\'\';renderFeed()">All ('+vids.length+')</button><button class="fc '+(fq==='fresh'?'on':'')+'" onclick="fq=\'fresh\';renderFeed()">Just posted ('+fresh+')</button><div class="fr"><input class="vsrch" type="text" placeholder="Search title / channel…" value="'+h(fq==='fresh'?'':fq)+'" oninput="fq=this.value;renderFeed()"></div></div>'+
    '<div class="vlist">'+(show.length?show.map(vcCard).join(''):emptyState())+'</div>';
}

function vcCard(v){
  const age=v.ageSeconds,isFresh=age<180;
  const as=age<60?age+'s ago':Math.floor(age/60)+'m '+age%60+'s ago';
  return '<div class="vc '+(isFresh?'hot':'')+'"><div class="vc-thumb"><img class="vc-img" src="'+h(v.thumb)+'" loading="lazy" onerror="this.style.opacity=\'.15\'"><div class="vc-age '+(isFresh?'f':'o')+'">'+as+'</div><div class="vc-ttl">'+v.minutesLeft+'m left</div></div><div class="vc-body">'+(isFresh?'<div class="new-tag">&#128308; JUST POSTED</div>':'')+'<div class="vc-ch">'+h(v.channelName||v.channelId)+'</div><div class="vc-title"><a href="'+h(v.url)+'" target="_blank">'+h(v.title)+'</a></div><a class="vc-url" href="'+h(v.url)+'" target="_blank">'+h(v.url)+'</a><div class="vc-meta"><div class="vm">Published: <b>'+new Date(v.publishedAt).toLocaleTimeString()+'</b></div><div class="vm yw">Expires: <b>'+v.minutesLeft+'m</b></div></div></div></div>';
}

function emptyState(){
  if(!chs.length)return '<div class="empty"><div class="empty-icon">&#128237;</div><div class="empty-title">No channels added</div><div class="empty-desc">Add Channel IDs (UCxxxxxx) in the sidebar. Find them on any YouTube channel page → About → Share → Copy channel ID.</div></div>';
  return '<div class="empty"><div class="empty-icon">&#128564;</div><div class="empty-title">No videos in last 10 minutes</div><div class="empty-desc">Watching '+chs.length+' channels. RSS checked every 60 seconds. Click ↻ Poll Now to check immediately.</div></div>';
}

function h(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function toast(msg,type=''){const w=document.getElementById('tw');const el=document.createElement('div');el.className='toast'+(type?' '+type:'');el.textContent=msg;w.appendChild(el);setTimeout(()=>el.remove(),4000);}

init();
</script>
</body>
</html>`;

app.get('/', (req, res) => res.send(HTML));

app.listen(PORT, '0.0.0.0', () => {
  log(`Server started on port ${PORT} — ${getBaseUrl()}`);
});
