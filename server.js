// ============================================================
//  YouTube Live Tracker — Server
//  Deploy on Glitch.com (free)
//  Receives YouTube PubSub push notifications instantly
// ============================================================

const express  = require('express');
const cors     = require('cors');
const xml2js   = require('xml2js');
const fetch    = require('node-fetch');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── In-memory stores ─────────────────────────────────────────
//  videos   : { videoId → { videoId, channelId, channelName, title, url, thumb, publishedAt, expiresAt } }
//  channels : { channelId → { channelId, name, subscribedAt } }
//  logs     : last 50 events for debugging

const videos   = new Map();   // live videos (auto-expire 10 min)
const channels = new Map();   // tracked channels
const logs     = [];

const TEN_MIN  = 10 * 60 * 1000;   // ms
const LOG_MAX  = 50;

// ── Middleware ────────────────────────────────────────────────
app.use(cors({ origin: '*' }));
app.use(express.text({ type: 'application/atom+xml', limit: '2mb' }));
app.use(express.text({ type: 'text/xml',             limit: '2mb' }));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── Helpers ───────────────────────────────────────────────────
function addLog(msg) {
  logs.unshift({ time: new Date().toISOString(), msg });
  if (logs.length > LOG_MAX) logs.pop();
}

function cleanExpired() {
  const now = Date.now();
  for (const [id, v] of videos) {
    if (v.expiresAt < now) {
      videos.delete(id);
      addLog(`Expired: ${v.title?.slice(0, 50)}`);
    }
  }
}

// Clean every 60 seconds
setInterval(cleanExpired, 60_000);

// ── YouTube PubSub Subscription ───────────────────────────────
const PUBSUB_HUB = 'https://pubsubhubbub.appspot.com/subscribe';

async function subscribeChannel(channelId) {
  const callbackUrl = `${process.env.SERVER_URL || `https://${process.env.PROJECT_DOMAIN}.glitch.me`}/pubsub`;
  const topicUrl    = `https://www.youtube.com/xml/feeds/videos.xml?channel_id=${channelId}`;

  const params = new URLSearchParams({
    'hub.callback'      : callbackUrl,
    'hub.topic'         : topicUrl,
    'hub.verify'        : 'async',
    'hub.mode'          : 'subscribe',
    'hub.lease_seconds' : '432000',   // 5 days — re-subscribe before expiry
  });

  try {
    const r = await fetch(PUBSUB_HUB, { method: 'POST', body: params });
    if (r.status === 202) {
      addLog(`Subscribed: ${channelId}`);
      return { ok: true };
    } else {
      const text = await r.text();
      addLog(`Subscribe failed ${channelId}: ${r.status} ${text}`);
      return { ok: false, error: text };
    }
  } catch (e) {
    addLog(`Subscribe error ${channelId}: ${e.message}`);
    return { ok: false, error: e.message };
  }
}

async function unsubscribeChannel(channelId) {
  const callbackUrl = `${process.env.SERVER_URL || `https://${process.env.PROJECT_DOMAIN}.glitch.me`}/pubsub`;
  const topicUrl    = `https://www.youtube.com/xml/feeds/videos.xml?channel_id=${channelId}`;

  const params = new URLSearchParams({
    'hub.callback' : callbackUrl,
    'hub.topic'    : topicUrl,
    'hub.verify'   : 'async',
    'hub.mode'     : 'unsubscribe',
  });

  try {
    await fetch(PUBSUB_HUB, { method: 'POST', body: params });
    addLog(`Unsubscribed: ${channelId}`);
  } catch (e) {
    addLog(`Unsubscribe error ${channelId}: ${e.message}`);
  }
}

// Auto re-subscribe every 4 days (before 5-day lease expires)
setInterval(async () => {
  addLog(`Re-subscribing ${channels.size} channels...`);
  for (const channelId of channels.keys()) {
    await subscribeChannel(channelId);
    await new Promise(r => setTimeout(r, 200)); // small delay
  }
}, 4 * 24 * 60 * 60 * 1000);

// ── PubSub Webhook ────────────────────────────────────────────

// GET — YouTube verifies subscription
app.get('/pubsub', (req, res) => {
  const challenge = req.query['hub.challenge'];
  if (challenge) {
    addLog(`Verification challenge received`);
    return res.send(challenge);
  }
  res.sendStatus(200);
});

// POST — YouTube sends new video notification
app.post('/pubsub', async (req, res) => {
  res.sendStatus(200); // acknowledge immediately

  try {
    const body = req.body;
    if (!body) return;

    const parser = new xml2js.Parser({ explicitArray: false });
    const parsed = await parser.parseStringPromise(body);

    const entry = parsed?.feed?.entry;
    if (!entry) return; // deletion notification, ignore

    const videoId   = entry['yt:videoId']   || entry.id?.replace('yt:video:', '');
    const channelId = entry['yt:channelId'] || entry.author?.uri?.split('/channel/')[1];
    const title     = entry.title;
    const published = entry.published;
    const updated   = entry.updated;

    if (!videoId) return;

    // Only care about NEW videos (published ≈ updated = new upload)
    // If updated >> published it's just a title/desc edit — ignore
    const pubTime = new Date(published).getTime();
    const updTime = new Date(updated).getTime();
    if (updTime - pubTime > 5 * 60 * 1000) return; // edit, not new upload

    const channelInfo = channels.get(channelId) || { channelId, name: channelId };

    const video = {
      videoId,
      channelId,
      channelName : channelInfo.name || channelId,
      title       : title || 'Untitled',
      url         : `https://www.youtube.com/watch?v=${videoId}`,
      thumb       : `https://img.youtube.com/vi/${videoId}/mqdefault.jpg`,
      publishedAt : published || new Date().toISOString(),
      receivedAt  : new Date().toISOString(),
      expiresAt   : Date.now() + TEN_MIN,
    };

    videos.set(videoId, video);
    addLog(`NEW VIDEO: [${channelInfo.name}] ${title?.slice(0, 60)}`);

  } catch (e) {
    addLog(`Parse error: ${e.message}`);
  }
});

// ── API: Channels ─────────────────────────────────────────────

// GET all channels
app.get('/api/channels', (req, res) => {
  res.json([...channels.values()]);
});

// POST add channel
app.post('/api/channels', async (req, res) => {
  const { channelId, name } = req.body;
  if (!channelId) return res.status(400).json({ error: 'channelId required' });

  if (channels.has(channelId)) return res.status(409).json({ error: 'Already tracked' });

  const entry = { channelId, name: name || channelId, subscribedAt: new Date().toISOString() };
  channels.set(channelId, entry);

  const result = await subscribeChannel(channelId);
  if (!result.ok) {
    channels.delete(channelId);
    return res.status(500).json({ error: 'Subscribe failed: ' + result.error });
  }

  res.json({ ok: true, channel: entry });
});

// DELETE remove channel
app.delete('/api/channels/:channelId', async (req, res) => {
  const { channelId } = req.params;
  if (!channels.has(channelId)) return res.status(404).json({ error: 'Not found' });

  channels.delete(channelId);
  // Remove that channel's videos too
  for (const [vid, v] of videos) {
    if (v.channelId === channelId) videos.delete(vid);
  }

  await unsubscribeChannel(channelId);
  res.json({ ok: true });
});

// POST bulk add channels
app.post('/api/channels/bulk', async (req, res) => {
  const { channels: list } = req.body; // [{channelId, name}]
  if (!Array.isArray(list)) return res.status(400).json({ error: 'channels array required' });

  const results = { added: 0, skipped: 0, errors: [] };

  for (const item of list) {
    const { channelId, name } = item;
    if (!channelId) continue;
    if (channels.has(channelId)) { results.skipped++; continue; }

    channels.set(channelId, { channelId, name: name || channelId, subscribedAt: new Date().toISOString() });
    const r = await subscribeChannel(channelId);
    if (r.ok) {
      results.added++;
    } else {
      channels.delete(channelId);
      results.errors.push({ channelId, error: r.error });
    }

    await new Promise(r => setTimeout(r, 150)); // avoid rate limit
  }

  res.json(results);
});

// ── API: Videos (live 10-min window) ─────────────────────────
app.get('/api/videos', (req, res) => {
  cleanExpired();
  const now   = Date.now();
  const list  = [...videos.values()]
    .filter(v => v.expiresAt > now)
    .map(v => ({
      ...v,
      minutesLeft : Math.ceil((v.expiresAt - now) / 60000),
      ageSeconds  : Math.floor((now - new Date(v.receivedAt).getTime()) / 1000),
    }))
    .sort((a, b) => new Date(b.receivedAt) - new Date(a.receivedAt));

  res.json({ count: list.length, videos: list });
});

// ── API: Stats & Logs ─────────────────────────────────────────
app.get('/api/stats', (req, res) => {
  cleanExpired();
  res.json({
    channelsTracked : channels.size,
    videosLive      : videos.size,
    uptime          : process.uptime(),
    logs            : logs.slice(0, 20),
  });
});

// ── Health check ──────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({
    status   : 'running',
    channels : channels.size,
    videos   : videos.size,
    message  : 'YouTube Live Tracker Server',
  });
});

app.listen(PORT, () => {
  console.log(`✅ Server running on port ${PORT}`);
  addLog(`Server started on port ${PORT}`);
});
