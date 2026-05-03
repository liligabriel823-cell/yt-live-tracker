const express = require('express');
const cors    = require('cors');
const xml2js  = require('xml2js');
const fetch   = require('node-fetch');

const app  = express();
const PORT = process.env.PORT || 3000;

// Railway automatically sets RAILWAY_PUBLIC_DOMAIN
function getServerUrl() {
  if (process.env.SERVER_URL) return process.env.SERVER_URL;
  if (process.env.RAILWAY_PUBLIC_DOMAIN) return `https://${process.env.RAILWAY_PUBLIC_DOMAIN}`;
  return `http://localhost:${PORT}`;
}

const videos   = new Map();
const channels = new Map();
const logs     = [];
const TEN_MIN  = 10 * 60 * 1000;

function addLog(msg) {
  const entry = { time: new Date().toISOString(), msg };
  logs.unshift(entry);
  if (logs.length > 100) logs.pop();
  console.log(`[${entry.time}] ${msg}`);
}

function cleanExpired() {
  const now = Date.now();
  for (const [id, v] of videos) {
    if (v.expiresAt < now) { videos.delete(id); addLog(`Expired: ${v.title?.slice(0,40)}`); }
  }
}
setInterval(cleanExpired, 30_000);

// ── CORS ──────────────────────────────────────────────────────
app.use(cors({ origin: '*', methods: ['GET','POST','DELETE','OPTIONS'], allowedHeaders: ['Content-Type'] }));
app.options('*', cors());

// ── Body parsers ──────────────────────────────────────────────
app.use((req, res, next) => {
  const ct = req.headers['content-type'] || '';
  if (ct.includes('xml') || ct.includes('atom')) {
    let data = '';
    req.on('data', chunk => data += chunk);
    req.on('end', () => { req.rawBody = data; next(); });
  } else next();
});
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── PubSub ────────────────────────────────────────────────────
const PUBSUB = 'https://pubsubhubbub.appspot.com/subscribe';

async function subscribe(channelId, mode = 'subscribe') {
  const callback = `${getServerUrl()}/pubsub`;
  const topic    = `https://www.youtube.com/xml/feeds/videos.xml?channel_id=${channelId}`;
  const body     = new URLSearchParams({
    'hub.callback'      : callback,
    'hub.topic'         : topic,
    'hub.verify'        : 'async',
    'hub.mode'          : mode,
    'hub.lease_seconds' : '432000',
  });
  try {
    const r = await fetch(PUBSUB, { method: 'POST', body });
    addLog(`${mode} ${channelId} → ${r.status}`);
    return r.status === 202;
  } catch(e) {
    addLog(`${mode} error: ${e.message}`);
    return false;
  }
}

// Re-subscribe every 4 days
setInterval(async () => {
  addLog(`Re-subscribing ${channels.size} channels`);
  for (const id of channels.keys()) {
    await subscribe(id);
    await new Promise(r => setTimeout(r, 200));
  }
}, 4 * 24 * 60 * 60 * 1000);

// ── PubSub Webhook ────────────────────────────────────────────
app.get('/pubsub', (req, res) => {
  const ch = req.query['hub.challenge'];
  if (ch) { addLog('PubSub verified'); return res.send(ch); }
  res.sendStatus(200);
});

app.post('/pubsub', async (req, res) => {
  res.sendStatus(200);
  const body = req.rawBody;
  if (!body) return;
  try {
    const p     = await new xml2js.Parser({ explicitArray: false }).parseStringPromise(body);
    const entry = p?.feed?.entry;
    if (!entry) return;

    const videoId   = entry['yt:videoId']   || '';
    const channelId = entry['yt:channelId'] || entry?.author?.uri?.split('/channel/')[1] || '';
    const title     = entry.title || 'Untitled';
    const published = entry.published || new Date().toISOString();
    const updated   = entry.updated   || published;

    if (!videoId) return;

    // Skip edits (published + updated differ by >5min = edit not new upload)
    if (new Date(updated) - new Date(published) > 5 * 60 * 1000) return;

    const ch = channels.get(channelId);
    videos.set(videoId, {
      videoId,
      channelId,
      channelName : ch?.name || channelId,
      title,
      url         : `https://www.youtube.com/watch?v=${videoId}`,
      thumb       : `https://img.youtube.com/vi/${videoId}/mqdefault.jpg`,
      publishedAt : published,
      receivedAt  : new Date().toISOString(),
      expiresAt   : Date.now() + TEN_MIN,
    });
    addLog(`NEW VIDEO [${ch?.name || channelId}]: ${title.slice(0,60)}`);
  } catch(e) { addLog(`Parse error: ${e.message}`); }
});

// ── Channels API ──────────────────────────────────────────────
app.get('/api/channels', (req, res) => res.json([...channels.values()]));

app.post('/api/channels', async (req, res) => {
  const { channelId, name } = req.body;
  if (!channelId) return res.status(400).json({ error: 'channelId required' });
  if (channels.has(channelId)) return res.status(409).json({ error: 'Already tracked' });
  channels.set(channelId, { channelId, name: name || channelId, addedAt: new Date().toISOString() });
  const ok = await subscribe(channelId);
  if (!ok) { channels.delete(channelId); return res.status(500).json({ error: 'PubSub subscribe failed' }); }
  res.json({ ok: true, channel: channels.get(channelId) });
});

app.delete('/api/channels/:id', async (req, res) => {
  const id = req.params.id;
  if (!channels.has(id)) return res.status(404).json({ error: 'Not found' });
  channels.delete(id);
  for (const [vid, v] of videos) { if (v.channelId === id) videos.delete(vid); }
  await subscribe(id, 'unsubscribe');
  res.json({ ok: true });
});

app.post('/api/channels/bulk', async (req, res) => {
  const list = req.body.channels;
  if (!Array.isArray(list)) return res.status(400).json({ error: 'channels[] required' });
  let added = 0, skipped = 0, failed = 0;
  for (const { channelId, name } of list) {
    if (!channelId) continue;
    if (channels.has(channelId)) { skipped++; continue; }
    channels.set(channelId, { channelId, name: name || channelId, addedAt: new Date().toISOString() });
    const ok = await subscribe(channelId);
    if (ok) added++; else { channels.delete(channelId); failed++; }
    await new Promise(r => setTimeout(r, 150));
  }
  res.json({ added, skipped, failed });
});

// ── Videos API ────────────────────────────────────────────────
app.get('/api/videos', (req, res) => {
  cleanExpired();
  const now  = Date.now();
  const list = [...videos.values()]
    .filter(v => v.expiresAt > now)
    .map(v => ({ ...v, minutesLeft: Math.ceil((v.expiresAt - now) / 60000), ageSeconds: Math.floor((now - new Date(v.receivedAt).getTime()) / 1000) }))
    .sort((a, b) => new Date(b.receivedAt) - new Date(a.receivedAt));
  res.json({ count: list.length, videos: list });
});

app.get('/api/stats', (req, res) => {
  cleanExpired();
  res.json({ status: 'running', channelsTracked: channels.size, videosLive: videos.size, serverUrl: getServerUrl(), uptime: Math.floor(process.uptime()), logs: logs.slice(0, 30) });
});

app.get('/', (req, res) => res.json({ status: 'running', channels: channels.size, videos: videos.size, message: 'YouTube Live Tracker — OK' }));

app.listen(PORT, '0.0.0.0', () => {
  addLog(`Server started — port ${PORT} — ${getServerUrl()}`);
});
