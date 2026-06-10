// ============================================================
// SOCIAL NODE — v0.4.0 — TikTok-style UI
// ============================================================
// up/down  → move between nodes in the network
// left/right → move between posts on the current node
// creator mode: tap the + button 5 times to unlock.
// photos and videos stored in cloudflare r2 (NODE_MEDIA bucket).
// install to home screen via browser's "add to home screen".
// ============================================================

import { DurableObject } from 'cloudflare:workers';

const PROTOCOL_VERSION = '0.8.0';

// ============================================================
// NETWORK ROOT OF TRUST
// ------------------------------------------------------------
// The network is defined by a signed member registry served by the
// root node. Every node builds its graph from that registry, so the
// code can be forked but the *network* (the member set) cannot — a
// fork can't mint members that verify against this root.
// ============================================================
const NETWORK_ROOT_HOST   = 'social.tuliptown.ca';
const NETWORK_ROOT_PUBKEY = 'YAdBZsfg9vrufnqe3JEFgQpUARoYdYNRR7yiKsusY6E=';

export default {
  async fetch(request, env, ctx) {
    // Multi-tenant: each hostname is its own creator. State is keyed by hostname,
    // so one deploy hosts many creators (alex.host.com, bob.host.com, …).
    const tenant = new URL(request.url).hostname;
    if (tenant === NETWORK_ROOT_HOST) await migrateLegacyToRoot(env);
    const storage = makeStorage(env, tenant);
    return await handleRequest(request, storage, env, ctx);
  },
};

// ============================================================
// LIVE ROOM — Durable Object
// ------------------------------------------------------------
// One instance per node (named 'live'). Single source of truth for:
//   • live session state (active / publisherSessionId / trackNames)
//   • real-time chat (broadcast over WebSockets, not KV polling)
//   • viewer presence / count (= open viewer WebSockets)
// Replaces the old KV 'live:session' / 'live:chat' keys, which lost
// concurrent writes (KV has no atomic read-modify-write).
// ============================================================

const LIVE_STALE_MS = 45000;       // browser broadcaster considered dead after this w/o heartbeat
const CHAT_KEEP      = 100;         // messages retained for late joiners
const CHAT_MIN_GAP_MS = 800;       // light per-connection chat throttle
const PREROLL_GRACE_MS = 8 * 60 * 1000; // don't re-show the pre-roll to the same viewer within this window

export class LiveRoom extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    const seg = url.pathname.split('/').pop();

    if (seg === 'ws') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('expected websocket', { status: 426 });
      }
      const role = url.searchParams.get('role') === 'broadcaster' ? 'broadcaster' : 'viewer';
      const name = (url.searchParams.get('name') || 'viewer').slice(0, 30);
      // sid = short hash of the client's viewer id — stable mute target that doesn't expose the raw vid
      const sid = await this.sidOf(url.searchParams.get('vid') || '');
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.serializeAttachment({ role, name, sid });
      this.ctx.acceptWebSocket(server);
      try {
        const status = await this.statusObj();
        server.send(JSON.stringify({
          t: 'init',
          status,
          // chat belongs to the live stream — joiners after it ended get a clean slate
          chat: status.active ? await this.recentChat() : [],
          // broadcaster's text/image overlay (stream-scoped, like chat) so joiners see it
          overlay: status.active ? ((await this.ctx.storage.get('overlay')) || null) : null,
          viewers: this.viewerCount(),
        }));
      } catch {}
      await this.sampleViewers();
      this.broadcastViewers();
      return new Response(null, { status: 101, webSocket: client });
    }

    if (seg === 'status') {
      return Response.json(await this.statusObj());
    }

    if (seg === 'publish' && request.method === 'POST') {
      const { publisherSessionId, trackNames, via } = await request.json().catch(() => ({}));
      // a re-publish over a still-active session (e.g. browser crashed mid-stream) closes out the old log entry
      const prev = await this.ctx.storage.get('session');
      if (prev && prev.active) await this.finalizeSession(prev, prev.heartbeatAt || Date.now());
      const now = Date.now();
      await this.ctx.storage.put('session', {
        active: true,
        publisherSessionId: publisherSessionId || null,
        trackNames: trackNames || [],
        startedAt: new Date().toISOString(),
        heartbeatAt: now,
        via: via || 'browser',
        peakViewers: this.viewerCount(),
        viewerSec: 0,
        lastSampleAt: now,
        lastViewers: this.viewerCount(),
      });
      await this.ctx.storage.put('chat', []); // fresh chat per stream
      // overlay deliberately NOT cleared here — broadcasters stage it before going live;
      // it clears on stream END (stream-scoped on the way out, not the way in)
      this.broadcast({ t: 'live', status: await this.statusObj() });
      return Response.json({ ok: true });
    }

    // broadcaster's text/image overlay — stored for joiners, broadcast to live viewers.
    // txt/img are placement objects {x,y,s} (screen-fraction coords + scale), sanitized
    // at the worker route. Auth happens there too; the DO trusts its caller.
    if (seg === 'overlay' && request.method === 'POST') {
      const { text, imageUrl, linkUrl, txt, img } = await request.json().catch(() => ({}));
      const overlay = (text || imageUrl)
        ? { text: text || '', imageUrl: imageUrl || '', linkUrl: linkUrl || '', txt: txt || null, img: img || null }
        : null;
      if (overlay) await this.ctx.storage.put('overlay', overlay);
      else await this.ctx.storage.delete('overlay');
      this.broadcast({ t: 'overlay', overlay });
      return Response.json({ ok: true, overlay });
    }

    if (seg === 'heartbeat' && request.method === 'POST') {
      const s = await this.ctx.storage.get('session');
      if (s && s.active) {
        s.heartbeatAt = Date.now();
        this.sampleInto(s);
        await this.ctx.storage.put('session', s);
      }
      return Response.json({ ok: true });
    }

    if (seg === 'end' && request.method === 'POST') {
      const s = await this.ctx.storage.get('session');
      if (s) {
        const wasActive = s.active;
        s.active = false; s.endedAt = new Date().toISOString();
        await this.ctx.storage.put('session', s);
        if (wasActive) await this.finalizeSession(s, Date.now());
      }
      await this.ctx.storage.delete('overlay');
      this.broadcast({ t: 'ended' });
      return Response.json({ ok: true });
    }

    if (seg === 'history') {
      const log = (await this.ctx.storage.get('streamLog')) || [];
      return Response.json({ streams: log.slice().reverse() });
    }

    // ---- who's connected (viewer list) + chat moderation ----
    if (seg === 'who') {
      const muted = (await this.ctx.storage.get('muted')) || {};
      const people = [];
      for (const ws of this.ctx.getWebSockets()) {
        try {
          const a = ws.deserializeAttachment() || {};
          people.push({ name: a.name || 'viewer', sid: a.sid || '', role: a.role || 'viewer', muted: !!(a.sid && muted[a.sid]) });
        } catch {}
      }
      return Response.json({
        viewers: people,
        mutedList: Object.entries(muted).map(([sid, m]) => ({ sid, name: m.name || 'viewer' })),
      });
    }
    if (seg === 'mute' && request.method === 'POST') {
      const { sid, name, muted: wantMuted } = await request.json().catch(() => ({}));
      if (!sid) return Response.json({ error: 'sid required' }, { status: 400 });
      const muted = (await this.ctx.storage.get('muted')) || {};
      if (wantMuted === false) {
        delete muted[sid];
      } else {
        muted[sid] = { name: String(name || 'viewer').slice(0, 30), at: new Date().toISOString() };
        const keys = Object.keys(muted);
        if (keys.length > 200) { keys.sort((a, b) => (muted[a].at < muted[b].at ? -1 : 1)); delete muted[keys[0]]; }
      }
      await this.ctx.storage.put('muted', muted);
      return Response.json({ ok: true });
    }

    if (seg === 'chat' && request.method === 'POST') {
      // HTTP chat (the no-WS fallback) gets the same per-sender throttle as the WS path —
      // unauthenticated and previously unlimited, it was the cheapest spam vector.
      const ip = request.headers.get('X-Chat-Ip') || 'unknown';
      const now = Date.now();
      if (!this._httpRate) this._httpRate = new Map();
      if (this._httpRate.size > 500) this._httpRate.clear();
      if (now - (this._httpRate.get(ip) || 0) < CHAT_MIN_GAP_MS) {
        return Response.json({ error: 'slow down' }, { status: 429 });
      }
      this._httpRate.set(ip, now);
      const { text, name, vid } = await request.json().catch(() => ({}));
      const sid = await this.sidOf(vid || '');
      const muted = (await this.ctx.storage.get('muted')) || {};
      if (sid && muted[sid]) return Response.json({ ok: true }); // shadow-mute: silently dropped
      const msg = await this.addChat(text, name, sid);
      return Response.json(msg ? { ok: true } : { error: 'empty' }, { status: msg ? 200 : 400 });
    }

    if (seg === 'chat') {
      const status = await this.statusObj();
      return Response.json({ messages: status.active ? await this.recentChat() : [] });
    }

    // ---- pre-roll ad frequency cap + counters ----
    if (seg === 'ad-allowed') {
      return Response.json({ allowed: await this.adAllowed(url.searchParams.get('vid') || '') });
    }
    if (seg === 'ad-seen' && request.method === 'POST') {
      const { vid } = await request.json().catch(() => ({}));
      await this.adSeen(vid, request.headers.get('X-Real-Ip') || '');
      return Response.json({ ok: true });
    }
    if (seg === 'ad-click' && request.method === 'POST') {
      const c = (await this.ctx.storage.get('adClicks')) || 0;
      await this.ctx.storage.put('adClicks', c + 1);
      return Response.json({ ok: true });
    }
    if (seg === 'ad-stats') {
      return Response.json({
        impressions: (await this.ctx.storage.get('adImpressions')) || 0,
        clicks: (await this.ctx.storage.get('adClicks')) || 0,
      });
    }

    // ---- verified impression measurement (root-side; DO named 'measure:<host>') ----
    // The viewed node's own meter pays creators; THESE deduped counts face advertisers.
    if (seg === 'm-imp' && request.method === 'POST') {
      const { vid } = await request.json().catch(() => ({}));
      if (!vid) return Response.json({ error: 'vid required' }, { status: 400 });
      const ip = request.headers.get('X-Real-Ip') || 'unknown';
      const now = Date.now();
      const seen = (await this.ctx.storage.get('m:seen')) || {};
      // one verified impression per viewer AND per IP per grace window
      if (now - (seen['v:' + vid] || 0) < PREROLL_GRACE_MS) return Response.json({ ok: true, dup: true });
      if (now - (seen['i:' + ip] || 0) < PREROLL_GRACE_MS) return Response.json({ ok: true, dup: true });
      seen['v:' + vid] = now; seen['i:' + ip] = now;
      for (const k of Object.keys(seen)) if (now - seen[k] > PREROLL_GRACE_MS * 4) delete seen[k];
      await this.ctx.storage.put('m:seen', seen);
      const day = new Date().toISOString().slice(0, 10);
      const days = (await this.ctx.storage.get('m:days')) || {};
      days[day] = (days[day] || 0) + 1;
      const dayKeys = Object.keys(days).sort();
      while (dayKeys.length > 30) delete days[dayKeys.shift()];
      await this.ctx.storage.put('m:days', days);
      await this.ctx.storage.put('m:total', ((await this.ctx.storage.get('m:total')) || 0) + 1);
      return Response.json({ ok: true });
    }
    if (seg === 'm-stats') {
      return Response.json({
        total: (await this.ctx.storage.get('m:total')) || 0,
        days: (await this.ctx.storage.get('m:days')) || {},
      });
    }

    // ---- generic sliding-window rate limit (auth brute-force guard) ----
    if (seg === 'rl' && request.method === 'POST') {
      const { key, max, windowMs } = await request.json().catch(() => ({}));
      if (!key) return Response.json({ allowed: false });
      const now = Date.now();
      const win = windowMs || 600000;
      const rl = (await this.ctx.storage.get('rl')) || {};
      const list = (rl[key] || []).filter(t => now - t < win);
      if (list.length >= (max || 10)) {
        rl[key] = list; await this.ctx.storage.put('rl', rl);
        return Response.json({ allowed: false });
      }
      list.push(now); rl[key] = list;
      for (const k of Object.keys(rl)) if (!rl[k].length || now - rl[k][rl[k].length - 1] > 3600000) delete rl[k];
      await this.ctx.storage.put('rl', rl);
      return Response.json({ allowed: true });
    }

    // ---- likes (deduped by viewer id) ----
    if (seg === 'like' && request.method === 'POST') {
      const { postId, vid, liked } = await request.json().catch(() => ({}));
      return Response.json(await this.likeToggle(postId, vid, !!liked));
    }
    if (seg === 'likes') {
      return Response.json(await this.likeGet(url.searchParams.get('postId') || '', url.searchParams.get('vid') || ''));
    }

    // ---- comments ----
    if (seg === 'comments') {
      return Response.json({ comments: await this.commentsGet(url.searchParams.get('postId') || '') });
    }
    if (seg === 'comment' && request.method === 'POST') {
      const { postId, text, name } = await request.json().catch(() => ({}));
      const c = await this.commentAdd(postId, text, name);
      return Response.json(c ? { ok: true, comment: c } : { error: 'empty' }, { status: c ? 200 : 400 });
    }
    if (seg === 'comment-del' && request.method === 'POST') {
      const { postId, id } = await request.json().catch(() => ({}));
      return Response.json(await this.commentDel(postId, id));
    }

    return new Response('not found', { status: 404 });
  }

  async likeToggle(postId, vid, liked) {
    if (!postId || !vid) return { count: 0, liked: false };
    const key = 'like:' + postId;
    const set = (await this.ctx.storage.get(key)) || [];
    const i = set.indexOf(vid);
    if (liked && i === -1) set.push(vid);
    else if (!liked && i !== -1) set.splice(i, 1);
    await this.ctx.storage.put(key, set);
    return { count: set.length, liked: set.includes(vid) };
  }

  async likeGet(postId, vid) {
    const set = (await this.ctx.storage.get('like:' + postId)) || [];
    return { count: set.length, liked: vid ? set.includes(vid) : false };
  }

  async commentsGet(postId) {
    return (await this.ctx.storage.get('cmt:' + postId)) || [];
  }

  async commentAdd(postId, text, name) {
    if (!postId || !text || !String(text).trim()) return null;
    const c = {
      id: crypto.randomUUID(),
      name: String(name || 'viewer').slice(0, 30),
      text: String(text).slice(0, 300),
      at: new Date().toISOString(),
    };
    const list = (await this.ctx.storage.get('cmt:' + postId)) || [];
    list.push(c);
    if (list.length > 500) list.splice(0, list.length - 500);
    await this.ctx.storage.put('cmt:' + postId, list);
    return c;
  }

  async commentDel(postId, id) {
    const list = (await this.ctx.storage.get('cmt:' + postId)) || [];
    await this.ctx.storage.put('cmt:' + postId, list.filter(c => c.id !== id));
    return { ok: true };
  }

  async adAllowed(vid) {
    if (!vid) return true;
    const seen = (await this.ctx.storage.get('adseen')) || {};
    return (Date.now() - (seen[vid] || 0)) > PREROLL_GRACE_MS;
  }

  async adSeen(vid, ip) {
    if (!vid) return;
    const now = Date.now();
    const seen = (await this.ctx.storage.get('adseen')) || {};
    // per-IP floor (30s, softer than the per-vid window — NAT'd venues share IPs):
    // blunts curl-loop inflation of the local meter without starving real viewers
    if (ip && now - (seen['ip:' + ip] || 0) < 30000) return;
    seen[vid] = now;
    if (ip) seen['ip:' + ip] = now;
    // bound the map: drop entries well past the grace window
    for (const k of Object.keys(seen)) if (now - seen[k] > PREROLL_GRACE_MS * 4) delete seen[k];
    await this.ctx.storage.put('adseen', seen);
    await this.ctx.storage.put('adImpressions', ((await this.ctx.storage.get('adImpressions')) || 0) + 1);
  }

  async statusObj() {
    const s = await this.ctx.storage.get('session');
    if (!s || !s.active) return { active: false, viewers: this.viewerCount() };
    // Browser broadcasters send heartbeats; if they stop, treat as ended.
    // WHIP (OBS/Larix) has no heartbeat — it ends via explicit DELETE.
    const stale = s.via !== 'whip' && s.heartbeatAt && (Date.now() - s.heartbeatAt > LIVE_STALE_MS);
    if (stale) {
      // a dead broadcaster never calls /end — close out the session log here
      s.active = false; s.endedAt = new Date(s.heartbeatAt).toISOString();
      await this.ctx.storage.put('session', s);
      await this.finalizeSession(s, s.heartbeatAt);
      return { active: false, viewers: this.viewerCount() };
    }
    return {
      active: true,
      publisherSessionId: s.publisherSessionId,
      trackNames: s.trackNames,
      startedAt: s.startedAt,
      via: s.via,
      viewers: this.viewerCount(),
    };
  }

  // Accumulate viewer-seconds + peak into the active session (mutates s; caller persists).
  // atMs caps the credited interval — pass the real end time when closing a stale session.
  sampleInto(s, atMs) {
    const now = atMs || Date.now();
    const n = this.viewerCount();
    if (s.lastSampleAt && now > s.lastSampleAt) {
      s.viewerSec = (s.viewerSec || 0) + ((now - s.lastSampleAt) / 1000) * (s.lastViewers || 0);
    }
    s.lastSampleAt = now;
    s.lastViewers = n;
    if (n > (s.peakViewers || 0)) s.peakViewers = n;
  }

  // Called whenever the viewer count changes mid-stream.
  async sampleViewers() {
    const s = await this.ctx.storage.get('session');
    if (!s || !s.active) return;
    this.sampleInto(s);
    await this.ctx.storage.put('session', s);
  }

  // Append the finished session to the per-tenant stream log (newest last, capped).
  async finalizeSession(s, endedAtMs) {
    this.sampleInto(s, endedAtMs); // capture the tail interval, capped at the real end time
    const startMs = Date.parse(s.startedAt) || endedAtMs;
    const rec = {
      startedAt: s.startedAt,
      endedAt: new Date(endedAtMs).toISOString(),
      durationSec: Math.max(0, Math.round((endedAtMs - startMs) / 1000)),
      peakViewers: s.peakViewers || 0,
      viewerSec: Math.round(s.viewerSec || 0),
      via: s.via || 'browser',
    };
    const log = (await this.ctx.storage.get('streamLog')) || [];
    log.push(rec);
    if (log.length > 50) log.splice(0, log.length - 50);
    await this.ctx.storage.put('streamLog', log);
    // chat belongs to the stream — don't let it linger for late visitors after the stream ends
    await this.ctx.storage.put('chat', []);
  }

  viewerCount() {
    let n = 0;
    for (const ws of this.ctx.getWebSockets()) {
      try { if (ws.deserializeAttachment()?.role === 'viewer') n++; } catch {}
    }
    return n;
  }

  async recentChat() {
    return (await this.ctx.storage.get('chat')) || [];
  }

  // sid is a short SHA-256 prefix of the sender's viewer id — safe to broadcast, useless to spoofers
  async sidOf(vid) {
    if (!vid) return '';
    const d = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(String(vid)));
    return [...new Uint8Array(d)].slice(0, 6).map(b => b.toString(16).padStart(2, '0')).join('');
  }

  async addChat(text, name, sid) {
    if (!text || !String(text).trim()) return null;
    const msg = {
      id: crypto.randomUUID(),
      text: String(text).slice(0, 200),
      name: String(name || 'viewer').slice(0, 30),
      sid: sid || '',
      at: new Date().toISOString(),
    };
    const chat = (await this.ctx.storage.get('chat')) || [];
    chat.push(msg);
    if (chat.length > CHAT_KEEP) chat.splice(0, chat.length - CHAT_KEEP);
    await this.ctx.storage.put('chat', chat);
    this.broadcast({ t: 'chat', msg });
    return msg;
  }

  broadcast(obj) {
    const data = JSON.stringify(obj);
    for (const ws of this.ctx.getWebSockets()) {
      try { ws.send(data); } catch {}
    }
  }

  broadcastViewers() {
    this.broadcast({ t: 'viewers', viewers: this.viewerCount() });
  }

  async webSocketMessage(ws, message) {
    let data;
    try { data = JSON.parse(message); } catch { return; }
    // a guest who picks a display name mid-stream tells us — the connection attachment
    // is what the broadcaster's viewer list reads, so keep it current
    if (data.t === 'name') {
      const nm = String(data.name || '').slice(0, 30);
      if (nm) { const att = ws.deserializeAttachment() || {}; att.name = nm; ws.serializeAttachment(att); }
      return;
    }
    if (data.t === 'chat') {
      // light per-connection throttle to blunt spam (anonymous WS)
      if (!this._rate) this._rate = new WeakMap();
      const now = Date.now();
      const last = this._rate.get(ws) || 0;
      if (now - last < CHAT_MIN_GAP_MS) return;
      this._rate.set(ws, now);
      const att = ws.deserializeAttachment() || {};
      // chat carries the freshest name — sync the attachment so the viewer list matches
      const nm = String(data.name || '').slice(0, 30);
      if (nm && nm !== att.name) { att.name = nm; ws.serializeAttachment(att); }
      const muted = (await this.ctx.storage.get('muted')) || {};
      if (att.sid && muted[att.sid]) return; // shadow-mute: silently dropped
      await this.addChat(data.text, data.name || att.name, att.sid);
    }
  }

  async webSocketClose(ws, code, reason) {
    try { ws.close(code, reason); } catch {}
    await this.sampleViewers();
    this.broadcastViewers();
  }

  async webSocketError() {
    await this.sampleViewers();
    this.broadcastViewers();
  }
}

// One live room per creator (keyed by hostname), so likes/comments/live are isolated per tenant.
function liveStub(env, tenant) { return env.LIVE_ROOM.getByName(tenant); }
function liveDO(env, tenant, path, init) { return liveStub(env, tenant).fetch('https://live.do' + path, init); }

// ============================================================
// STORAGE  (multi-tenant: keys are namespaced per hostname)
// ============================================================

function makeStorage(env, tenant) {
  // tenant falsy → global namespace (used for the network-wide registry).
  const prefix = tenant ? 't:' + tenant + ':' : '';
  return {
    async get(key) {
      const raw = await env.NODE_STATE.get(prefix + key);
      return raw ? JSON.parse(raw) : null;
    },
    async set(key, value) {
      await env.NODE_STATE.put(prefix + key, JSON.stringify(value));
    },
    async delete(key) {
      await env.NODE_STATE.delete(prefix + key);
    },
  };
}

// The original single-tenant deploy stored these under bare keys. Copy them once into
// the root host's tenant slot WITHOUT regenerating anything (identity must be preserved,
// or the baked NETWORK_ROOT_PUBKEY breaks). Copy-only + guarded → idempotent + rollback-safe.
const TENANT_KEYS = ['identity', 'feed', 'peers', 'ancestors', 'profile', 'inbox:notifications', 'preroll'];

async function migrateLegacyToRoot(env) {
  const guard = 't:' + NETWORK_ROOT_HOST + ':_migrated';
  if (await env.NODE_STATE.get(guard)) return;
  const legacyIdentity = await env.NODE_STATE.get('identity');
  if (legacyIdentity == null) { await env.NODE_STATE.put(guard, '1'); return; } // fresh deploy, nothing to migrate
  for (const k of TENANT_KEYS) {
    const raw = await env.NODE_STATE.get(k); // raw string — copied verbatim, no re-encode
    if (raw == null) continue;
    const tk = 't:' + NETWORK_ROOT_HOST + ':' + k;
    if ((await env.NODE_STATE.get(tk)) == null) await env.NODE_STATE.put(tk, raw);
  }
  await env.NODE_STATE.put(guard, '1');
}

// ============================================================
// CRYPTO
// ============================================================

async function generateIdentity() {
  const keyPair = await crypto.subtle.generateKey(
    { name: 'Ed25519', namedCurve: 'Ed25519' },
    true,
    ['sign', 'verify']
  );
  const publicKey = await crypto.subtle.exportKey('raw', keyPair.publicKey);
  const privateKey = await crypto.subtle.exportKey('pkcs8', keyPair.privateKey);
  return {
    publicKey: bufferToBase64(publicKey),
    privateKey: bufferToBase64(privateKey),
    createdAt: new Date().toISOString(),
  };
}

async function sign(privateKeyB64, data) {
  const privateKey = await crypto.subtle.importKey(
    'pkcs8', base64ToBuffer(privateKeyB64),
    { name: 'Ed25519', namedCurve: 'Ed25519' }, false, ['sign']
  );
  const sig = await crypto.subtle.sign('Ed25519', privateKey, new TextEncoder().encode(data));
  return bufferToBase64(sig);
}

async function verify(publicKeyB64, signatureB64, data) {
  try {
    const publicKey = await crypto.subtle.importKey(
      'raw', base64ToBuffer(publicKeyB64),
      { name: 'Ed25519', namedCurve: 'Ed25519' }, false, ['verify']
    );
    return await crypto.subtle.verify('Ed25519', publicKey, base64ToBuffer(signatureB64), new TextEncoder().encode(data));
  } catch (e) { return false; }
}

function bufferToBase64(buf) { return btoa(String.fromCharCode(...new Uint8Array(buf))); }
function base64ToBuffer(b64) { return Uint8Array.from(atob(b64), c => c.charCodeAt(0)).buffer; }

// constant-time string compare — avoids leaking token length/contents via timing
function ctEq(a, b) {
  const ea = new TextEncoder().encode(a), eb = new TextEncoder().encode(b);
  if (ea.length !== eb.length) return false;
  let d = 0; for (let i = 0; i < ea.length; i++) d |= ea[i] ^ eb[i];
  return d === 0;
}
async function sha256hex(s) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
}
function randomTokenHex() {
  return [...crypto.getRandomValues(new Uint8Array(32))].map(b => b.toString(16).padStart(2, '0')).join('');
}
function bytesToHex(bytes) { return [...bytes].map(b => b.toString(16).padStart(2, '0')).join(''); }
function hexToBytes(hex) { const a = new Uint8Array(hex.length / 2); for (let i = 0; i < a.length; i++) a[i] = parseInt(hex.substr(i * 2, 2), 16); return a; }
// PBKDF2-SHA256 password hashing (native WebCrypto) — for per-creator passwords (Phase 2 auth)
async function pbkdf2(password, saltHex, iterations) {
  const km = await crypto.subtle.importKey('raw', new TextEncoder().encode(password), 'PBKDF2', false, ['deriveBits']);
  const bits = await crypto.subtle.deriveBits({ name: 'PBKDF2', salt: hexToBytes(saltHex), iterations, hash: 'SHA-256' }, km, 256);
  return bytesToHex(new Uint8Array(bits));
}
// Stateless session token: base64(payload).base64(sig), signed by the tenant's own Ed25519 key.
// No server-side session store; verified by re-checking the signature + expiry against the serving tenant.
async function makeSession(identity, host, days = 30) {
  const payload = btoa(JSON.stringify({ h: host, exp: Math.floor(Date.now() / 1000) + days * 86400 }));
  return payload + '.' + (await sign(identity.privateKey, payload));
}
async function verifySession(identity, host, tokenStr) {
  const dot = tokenStr.indexOf('.');
  if (dot < 0) return false;
  const payload = tokenStr.slice(0, dot), sig = tokenStr.slice(dot + 1);
  if (!(await verify(identity.publicKey, sig, payload))) return false;
  try { const p = JSON.parse(atob(payload)); return p.h === host && p.exp > Date.now() / 1000; } catch (e) { return false; }
}

// ============================================================
// NODE STATE
// ============================================================

async function ensureIdentity(storage) {
  let identity = await storage.get('identity');
  if (!identity) {
    identity = await generateIdentity();
    await storage.set('identity', identity);
  }
  return identity;
}

async function getPeers(storage) { return (await storage.get('peers')) || []; }
async function getAncestors(storage) { return (await storage.get('ancestors')) || []; }
// ---- feed storage: sharded across KV chunk keys ----
// The old layout was ONE unbounded array under 'feed' — capped at KV's 25MB value limit
// and fully rewritten on every publish/delete. Now: 'feedIndex' holds a small manifest
// { v:1, total, chunks:[{n,count},...] } (newest-first, n descending) and each
// 'feed:c:<n>' holds up to FEED_CHUNK_SIZE posts, newest-first. Chunks are anchored at
// the OLDEST end (chunk 0 = the oldest posts), so old chunks' contents are position-stable
// and a publish only ever rewrites the head chunk + index. The feed.json contract is
// unchanged — this is internal layout only.
const FEED_CHUNK_SIZE = 200; // worst-case post ~6KB (5000-char transcript) → ~1.2MB/chunk

function chunkFeed(feed) {
  const total = feed.length;
  const chunks = []; // [{n, items}] newest-first (n = top..0)
  for (let n = total ? Math.floor((total - 1) / FEED_CHUNK_SIZE) : -1; n >= 0; n--) {
    const end = total - n * FEED_CHUNK_SIZE;
    chunks.push({ n, items: feed.slice(Math.max(0, end - FEED_CHUNK_SIZE), end) });
  }
  return chunks;
}

async function getFeed(storage) {
  const idx = await storage.get('feedIndex');
  if (idx && Array.isArray(idx.chunks)) {
    const parts = await Promise.all(idx.chunks.map(c => storage.get('feed:c:' + c.n)));
    // Snapshot each chunk's JSON at read time (before callers mutate items in place) so
    // saveFeed can diff and write only the chunks that actually changed.
    const snap = {}, feed = [];
    idx.chunks.forEach((c, i) => {
      const items = parts[i] || [];
      snap[c.n] = JSON.stringify(items);
      for (const it of items) feed.push(it);
    });
    storage._feedSnap = snap;
    return feed;
  }
  const legacy = (await storage.get('feed')) || [];
  // Lazy one-time migration to the sharded layout. Idempotent (concurrent first reads
  // write identical chunks) and fail-safe: on error the legacy key is untouched and
  // still serves. The pre-sharding value is kept as a rollback backup.
  try { await saveFeed(storage, legacy, { migrating: true }); } catch (e) {}
  return legacy;
}

async function saveFeed(storage, feed, opts) {
  const chunks = chunkFeed(feed);
  const snap = storage._feedSnap || {}; // empty snap (no prior chunked read) → write everything
  const newSnap = {};
  for (const c of chunks) {
    const json = JSON.stringify(c.items);
    newSnap[c.n] = json;
    if (snap[c.n] !== json) await storage.set('feed:c:' + c.n, c.items);
  }
  await storage.set('feedIndex', { v: 1, total: feed.length, chunks: chunks.map(c => ({ n: c.n, count: c.items.length })) });
  // Stale chunks (bulk delete shrank the feed) go AFTER the index write, so a racing
  // reader holding the old index never dereferences a deleted chunk.
  for (const n of Object.keys(snap)) {
    if (!(n in newSnap)) await storage.delete('feed:c:' + n);
  }
  storage._feedSnap = newSnap;
  if (opts && opts.migrating) {
    if (feed.length) await storage.set('feed:presharding-backup', feed);
    await storage.delete('feed');
  }
}

// Serve a window of the feed reading only the chunks that cover it (feed.json ?limit=&offset=
// is every client's boot path — a 30-post page shouldn't load a 10,000-post history).
async function getFeedPage(storage, offset, limit) {
  const idx = await storage.get('feedIndex');
  if (!idx || !Array.isArray(idx.chunks)) {
    const feed = await getFeed(storage); // legacy (triggers migration)
    return { total: feed.length, items: feed.slice(offset, offset + limit) };
  }
  const wanted = [];
  let pos = 0;
  for (const c of idx.chunks) {
    if (pos < offset + limit && pos + c.count > offset) wanted.push({ n: c.n, pos });
    pos += c.count;
  }
  const parts = await Promise.all(wanted.map(c => storage.get('feed:c:' + c.n)));
  let items = [];
  for (const p of parts) items = items.concat(p || []);
  const start = wanted.length ? wanted[0].pos : 0;
  return { total: idx.total || 0, items: items.slice(offset - start, offset - start + limit) };
}

async function getFeedTotal(storage) {
  const idx = await storage.get('feedIndex');
  if (idx && Array.isArray(idx.chunks)) return idx.total || 0;
  return (await getFeed(storage)).length;
}

async function addPeer(storage, peer) {
  const peers = await getPeers(storage);
  if (peers.find(p => p.publicKey === peer.publicKey)) return peers;
  peers.push({ ...peer, addedAt: new Date().toISOString() });
  await storage.set('peers', peers);
  return peers;
}

// Mark a join_request inbox notification as resolved once it's been approved/denied,
// so the inbox stops rendering its Approve/Deny buttons. storage = the root tenant's KV.
async function resolveJoinNotif(storage, pubkey, outcome) {
  const notifs = (await storage.get('inbox:notifications')) || [];
  let changed = false;
  for (const n of notifs) {
    if (n.type === 'join_request' && n.publicKey === pubkey && !n.resolved) {
      n.resolved = outcome; n.read = true; changed = true;
    }
  }
  if (changed) await storage.set('inbox:notifications', notifs);
}

// ---- network registry (root of trust) ----
function isRoot(identity) { return identity.publicKey === NETWORK_ROOT_PUBKEY; }

// The registry is network-wide, not per-creator, so it always uses the GLOBAL namespace.
async function getRegistryMembers(env, identity) {
  const g = makeStorage(env);
  let members = (await g.get('registry:members')) || [];
  // the root always counts itself as the first member
  if (isRoot(identity) && !members.find(m => m.pubkey === identity.publicKey)) {
    members = [{ subdomain: NETWORK_ROOT_HOST, pubkey: identity.publicKey, addedAt: new Date().toISOString() }, ...members];
    await g.set('registry:members', members);
  }
  return members;
}

async function registrySigned(env, identity, subdomain) {
  const g = makeStorage(env);
  const members = await getRegistryMembers(env, identity);
  const updatedAt = (await g.get('registry:updatedAt')) || new Date().toISOString();
  const payload = updatedAt + '.' + JSON.stringify(members);
  const signature = await sign(identity.privateKey, payload);
  return { root: { subdomain, pubkey: identity.publicKey }, members, updatedAt, signature };
}

// ============================================================
// ROUTING
// ============================================================

// Per-isolate memory cache for hot, rarely-changing KV values (provisioned list, tenant
// identities). Isolates persist across requests, so this turns a KV read per request into
// one per TTL per isolate — the difference between blowing the KV budget and ignoring it.
const _memCache = new Map();
function memGet(key, ttlMs) {
  const e = _memCache.get(key);
  return e && Date.now() - e.at < ttlMs ? e.v : undefined;
}
function memSet(key, v) {
  if (_memCache.size > 500) _memCache.clear(); // crude bound; entries are tiny
  _memCache.set(key, { v, at: Date.now() });
}

async function handleRequest(request, storage, env, ctx) {
  const url = new URL(request.url);
  const path = url.pathname;
  const subdomain = url.hostname;
  const gstore = makeStorage(env); // global namespace — for the network-wide registry only

  // CORS preflight
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  // ---- media serving (public, FAST PATH — before the gate and ensureIdentity) ----
  // Media uses neither KV nor identity (R2 keys are immutable UUIDs), and video playback
  // fires storms of range requests — keeping it above the gate means zero KV reads here.
  // Range support is required for iOS Safari video playback/seeking; full responses are
  // edge-cached so repeat views don't hit R2 or burn Worker CPU.
  if (path.startsWith('/media/') && request.method === 'GET') {
    if (!env.NODE_MEDIA) return new Response('R2 not configured', { status: 503 });
    const key = path.slice(7);
    if (!key) return new Response('not found', { status: 404 });
    const rangeHeader = request.headers.get('Range');
    const cacheKey = new Request(url.origin + path);
    // Edge cache serves FULL requests only. Range requests go straight to R2's native
    // range reads — slicing a cached full object through JS meant reading and discarding
    // up to the whole file per request (iOS asks for high offsets on MP4s with trailing
    // metadata), which stalled large videos and burned CPU.
    if (!rangeHeader) {
      try { const hit = await caches.default.match(cacheKey); if (hit) return hit; } catch (e) {}
    }
    let object;
    try {
      object = await env.NODE_MEDIA.get(key, rangeHeader ? { range: request.headers } : undefined);
    } catch (e) {
      object = await env.NODE_MEDIA.get(key); // unsatisfiable Range → serve the full object
    }
    if (!object) return new Response('not found', { status: 404 });
    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');
    headers.set('Accept-Ranges', 'bytes');
    headers.set('ETag', object.httpEtag);
    // Warm the full-object edge cache in the background so the next FULL request is
    // served from cache. Skip very large files to bound the background read.
    if (ctx && !rangeHeader && object.size <= 64 * 1024 * 1024) {
      ctx.waitUntil((async () => {
        try {
          const full = await env.NODE_MEDIA.get(key);
          if (!full) return;
          const fh = new Headers();
          full.writeHttpMetadata(fh);
          fh.set('Cache-Control', 'public, max-age=31536000, immutable');
          fh.set('Accept-Ranges', 'bytes');
          fh.set('ETag', full.httpEtag);
          fh.set('Content-Length', String(full.size));
          await caches.default.put(cacheKey, new Response(full.body, { headers: fh }));
        } catch (e) {}
      })());
    }
    if (rangeHeader && object.range) {
      const size = object.size;
      let offset = 0, length = size;
      if (object.range.suffix != null) { length = Math.min(object.range.suffix, size); offset = size - length; }
      else { offset = object.range.offset ?? 0; length = object.range.length ?? (size - offset); }
      headers.set('Content-Range', 'bytes ' + offset + '-' + (offset + length - 1) + '/' + size);
      headers.set('Content-Length', String(length));
      return new Response(object.body, { status: 206, headers });
    }
    headers.set('Content-Length', String(object.size));
    return new Response(object.body, { headers });
  }

  // Static app files — no KV/identity needed, also served before the gate.
  if (path === '/sw.js') {
    return new Response(renderServiceWorker(), {
      headers: { 'Content-Type': 'application/javascript' },
    });
  }
  if (path === '/icon.svg') {
    return new Response(renderIcon(), {
      headers: { 'Content-Type': 'image/svg+xml' },
    });
  }
  if (path === '/manifest.json') {
    return json(renderManifest(subdomain));
  }

  // ---- provisioning gate (wildcard auto-create protection) ----
  // A wildcard host auto-creates a tenant on first hit, which is an open mint surface.
  // Opt-in: while the provisioned list is EMPTY the gate is off (backward compatible —
  // simple/forked deploys keep auto-create). Once any host is provisioned, only allowed
  // hosts serve/auto-create. Always allowed: the network root + any registry member
  // (real nodes never get locked out). Unknown hosts 404 BEFORE ensureIdentity → no KV write.
  // The list is isolate-cached 60s: it changes rarely, and reading it from KV on every
  // request was the bulk of the worker's KV bill. Provision/unprovision can take up to
  // 60s per isolate to be seen — acceptable.
  let provisioned = memGet('prov', 60000);
  if (provisioned === undefined) {
    provisioned = (await gstore.get('provisioned')) || [];
    memSet('prov', provisioned);
  }
  if (provisioned.length > 0 && subdomain !== NETWORK_ROOT_HOST && !provisioned.includes(subdomain)) {
    const regMembers = (await gstore.get('registry:members')) || [];
    if (!regMembers.find(m => m.subdomain === subdomain)) {
      return html(renderUnclaimed(subdomain), 404);
    }
  }

  // Identity is immutable once minted → cache it per tenant in isolate memory (5 min)
  // instead of a KV read per request. Only successful identities are cached, so a new
  // tenant's first-visit creation still runs.
  let identity = memGet('id:' + subdomain, 300000);
  if (!identity) {
    identity = await ensureIdentity(storage);
    if (identity && identity.publicKey) memSet('id:' + subdomain, identity);
  }

  // ---- PWA files ----
  if (path === '/' || path === '/index.html') {
    const profile = (await storage.get('profile')) || {};
    const feed = await getFeed(storage);
    return html(renderApp({ identity, subdomain, seo: buildSeo({ origin: url.origin, subdomain, profile, feed }) }));
  }
  // Per-post permalink: same app, but with post-specific OG meta so shares unfurl,
  // plus a crawler-readable noscript body. The client deep-links to the post on boot.
  if (path.startsWith('/p/') && request.method === 'GET') {
    const feed = await getFeed(storage);
    const post = feed.find(i => i.id === path.slice(3));
    if (!post) return Response.redirect(url.origin + '/', 302); // deleted/stale link → profile
    const profile = (await storage.get('profile')) || {};
    return html(renderApp({ identity, subdomain, seo: buildSeo({ origin: url.origin, subdomain, profile, feed, post }) }));
  }
  if (path === '/robots.txt') {
    return new Response('User-agent: *\nAllow: /\nDisallow: /admin/\nDisallow: /auth/\n\nSitemap: ' + url.origin + '/sitemap.xml\n', {
      headers: { 'Content-Type': 'text/plain', 'Cache-Control': 'public, max-age=3600' },
    });
  }
  if (path === '/sitemap.xml') {
    const feed = await getFeed(storage);
    const urls = ['<url><loc>' + url.origin + '/</loc></url>']
      .concat(feed.map(i => '<url><loc>' + url.origin + '/p/' + i.id + '</loc>'
        + (i.createdAt ? '<lastmod>' + i.createdAt.slice(0, 10) + '</lastmod>' : '') + '</url>'));
    return new Response('<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' + urls.join('') + '</urlset>', {
      headers: { 'Content-Type': 'application/xml', 'Cache-Control': 'public, max-age=600' },
    });
  }
  // Node policies: acceptable use, reporting/takedown process, privacy, import rights.
  // Served per-node so every fork ships its operator's obligations with the software.
  if (path === '/legal') {
    const profile = (await storage.get('profile')) || {};
    return html(renderLegalPage(subdomain, profile.displayName || subdomain.split('.')[0]));
  }
  // ---- protocol surface ----
  if (path === '/.well-known/node.json') {
    return jsonCors({
      protocol: PROTOCOL_VERSION,
      subdomain,
      publicKey: identity.publicKey,
      createdAt: identity.createdAt,
      ancestors: await getAncestors(storage),
    });
  }
  if (path === '/.well-known/feed.json') {
    const profile = (await storage.get('profile')) || {};
    // Optional pagination (?limit=&offset=) so big accounts don't ship their whole
    // history on every boot. No params → full feed (backward compatible). Paged
    // requests only read the KV chunks that cover the window.
    const limit = Math.min(Math.max(parseInt(url.searchParams.get('limit')) || 0, 0), 100);
    const offset = Math.max(parseInt(url.searchParams.get('offset')) || 0, 0);
    let total, items;
    if (limit) {
      ({ total, items } = await getFeedPage(storage, offset, limit));
    } else {
      items = await getFeed(storage);
      total = items.length;
    }
    return jsonCors({
      subdomain,
      publicKey: identity.publicKey,
      displayName: profile.displayName || '',
      avatarUrl: profile.avatarUrl || null,
      total,
      offset,
      items,
    });
  }
  if (path === '/.well-known/peers.json') {
    return jsonCors({ subdomain, peers: await getPeers(storage) });
  }
  // signed member registry — the canonical network is whatever the root signs here.
  // Signed once per MUTATION (stored blob) instead of per request, and edge-cached 60s:
  // every client in the network fetches this on boot, and all of that lands on the root.
  if (path === '/.well-known/registry.json') {
    try { const hit = await caches.default.match(request); if (hit) return hit; } catch (e) {}
    let signed = await gstore.get('registry:signed');
    if (!signed || !signed.members) {
      signed = await registrySigned(env, identity, subdomain);
      // only persist a root-signed blob — other tenants must not poison the global key
      if (isRoot(identity)) await gstore.set('registry:signed', signed);
    }
    const resp = jsonCors(signed, 200, { 'Cache-Control': 'public, max-age=60' });
    try { await caches.default.put(request, resp.clone()); } catch (e) {}
    return resp;
  }
  if (path === '/.well-known/source.json') {
    return json({
      protocol: PROTOCOL_VERSION,
      bundledAt: new Date().toISOString(),
      ancestor: { publicKey: identity.publicKey, subdomain },
      instructions: [
        'copy the worker.js source from your ancestor and paste it into a new cloudflare worker',
        'create a KV namespace called NODE_STATE and bind it to your worker',
        'create an R2 bucket called NODE_MEDIA and bind it to your worker',
        'point your subdomain at the worker',
        'announce yourself: curl -H "Authorization: Bearer <YOUR_ADMIN_TOKEN>" "https://<your-node>/admin/inherit?from=' + subdomain + '"',
      ],
    });
  }

  // /protocol/announce + /protocol/peers REMOVED 2026-06-10 (legacy pre-registry bootstrap,
  // unauthenticated writers — see PROTOCOL.md legacy section). Membership = the signed registry.

  // ---- network join request (sent to the root) ----
  if (path === '/protocol/join-request' && request.method === 'POST') {
    if (!isRoot(identity)) return jsonCors({ error: 'this node is not the network root' }, 403);
    const body = await request.json().catch(() => ({}));
    let msg; try { msg = JSON.parse(body.message); } catch { return jsonCors({ error: 'bad request' }, 400); }
    const valid = await verify(msg.pubkey, body.signature, body.message);
    if (!valid) return jsonCors({ error: 'invalid signature' }, 400);
    const members = await getRegistryMembers(env, identity);
    if (members.find(m => m.pubkey === msg.pubkey)) return jsonCors({ ok: true, alreadyMember: true });
    const pending = (await gstore.get('registry:pending')) || [];
    if (!pending.find(p => p.pubkey === msg.pubkey)) {
      pending.unshift({ subdomain: msg.subdomain, pubkey: msg.pubkey, at: new Date().toISOString() });
      await gstore.set('registry:pending', pending);
      const notifs = (await storage.get('inbox:notifications')) || [];
      notifs.unshift({ id: crypto.randomUUID(), type: 'join_request', subdomain: msg.subdomain, publicKey: msg.pubkey, at: new Date().toISOString(), read: false });
      if (notifs.length > 100) notifs.splice(100);
      await storage.set('inbox:notifications', notifs);
    }
    return jsonCors({ ok: true, pending: true });
  }

  // ---- public content report (moderation) ----
  // Anyone can flag content on this node. The report lands in THIS node's inbox (the
  // creator/host must act on notice — Canadian notice regimes) AND is escalated to the
  // network root's inbox (root operator's visibility + registry-ejection power; for CSAM
  // the operator has a statutory reporting duty). Rate-limited; no auth by design.
  if (path === '/report' && request.method === 'POST') {
    if (!(await rlAllowed('report', 5, 600000))) return jsonCors({ error: 'rate limited' }, 429);
    const b = await request.json().catch(() => ({}));
    const notif = {
      id: crypto.randomUUID(), type: 'report',
      reason: REPORT_REASONS.includes(b.reason) ? b.reason : 'other',
      postId: String(b.postId || '').slice(0, 60),
      details: String(b.details || '').slice(0, 1000),
      subdomain, at: new Date().toISOString(), read: false,
    };
    const notifs = (await storage.get('inbox:notifications')) || [];
    notifs.unshift(notif);
    if (notifs.length > 100) notifs.splice(100);
    await storage.set('inbox:notifications', notifs);
    if (!isRoot(identity)) {
      // Same-worker root → write its inbox directly (a self-fetch would 522, see request-join).
      const rootStore = makeStorage(env, NETWORK_ROOT_HOST);
      const rootIdentity = await rootStore.get('identity');
      if (rootIdentity && isRoot(rootIdentity)) {
        const rn = (await rootStore.get('inbox:notifications')) || [];
        rn.unshift(notif);
        if (rn.length > 100) rn.splice(100);
        await rootStore.set('inbox:notifications', rn);
      } else {
        try {
          await fetch('https://' + NETWORK_ROOT_HOST + '/protocol/report', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ host: subdomain, postId: notif.postId, reason: notif.reason, details: notif.details }),
          });
        } catch (e) {}
      }
    }
    return jsonCors({ ok: true });
  }
  // Root-side ingest for reports escalated from separate-worker nodes.
  if (path === '/protocol/report' && request.method === 'POST') {
    if (!isRoot(identity)) return jsonCors({ error: 'this node is not the network root' }, 403);
    if (!(await rlAllowed('report', 10, 600000))) return jsonCors({ error: 'rate limited' }, 429);
    const b = await request.json().catch(() => ({}));
    const notifs = (await storage.get('inbox:notifications')) || [];
    notifs.unshift({
      id: crypto.randomUUID(), type: 'report',
      reason: REPORT_REASONS.includes(b.reason) ? b.reason : 'other',
      postId: String(b.postId || '').slice(0, 60),
      details: String(b.details || '').slice(0, 1000),
      subdomain: String(b.host || '').slice(0, 100).toLowerCase(),
      at: new Date().toISOString(), read: false,
    });
    if (notifs.length > 100) notifs.splice(100);
    await storage.set('inbox:notifications', notifs);
    return jsonCors({ ok: true });
  }

  // ---- public profile ----
  if (path === '/profile.json') {
    const profile = (await storage.get('profile')) || {};
    // "peerCount" historically came from the dead local `peers` list (only ever written
    // by the legacy /protocol/announce bootstrap). In the registry era real membership is
    // the global signed registry, so report the network size instead — always > 0 for a member.
    let networkSize = (await getRegistryMembers(env, identity)).length;
    // Separate-worker nodes keep an empty local registry — the canonical one lives at the root.
    if (networkSize === 0 && !isRoot(identity)) {
      try {
        const r = await fetch('https://' + NETWORK_ROOT_HOST + '/.well-known/registry.json', { cf: { cacheTtl: 300, cacheEverything: true } });
        networkSize = ((await r.json()).members || []).length;
      } catch (e) {}
    }
    return jsonCors({
      subdomain,
      displayName: profile.displayName || subdomain.split('.')[0],
      bio: profile.bio || '',
      avatarUrl: profile.avatarUrl || null,
      postCount: await getFeedTotal(storage),
      peerCount: networkSize,
      networkSize,
      adsEnabled: !!(((await storage.get('ads')) || {}).enabled),
    });
  }

  // ---- admin: auth check ----
  // Authorize a request. Two tiers, fails closed if neither matches:
  //  1. host master token (env.ADMIN_TOKEN) — works on EVERY tenant of this Worker.
  //  2. this tenant's own per-creator token — hash stored at t:<host>:auth, scoped to this host only.
  async function checkAuth(req, presentedOverride) {
    let presented = presentedOverride || '';
    if (!presented) {
      const auth = req.headers.get('Authorization') || '';
      if (!auth.startsWith('Bearer ')) return false;
      presented = auth.slice(7);
    }
    if (!presented) return false;
    if (env.ADMIN_TOKEN && ctEq(presented, env.ADMIN_TOKEN)) return true;
    const tenantAuth = await storage.get('auth');
    if (tenantAuth && tenantAuth.tokenHash && ctEq(await sha256hex(presented), tenantAuth.tokenHash)) return true;
    // Phase 2: a signed session token (from /auth/login or /auth/claim) scoped to this host
    if (presented.includes('.') && await verifySession(identity, subdomain, presented)) return true;
    return false;
  }
  // Host master ONLY — per-creator tokens cannot manage credentials/provisioning.
  function isMaster(req) {
    const auth = req.headers.get('Authorization') || '';
    return auth.startsWith('Bearer ') && !!env.ADMIN_TOKEN && ctEq(auth.slice(7), env.ADMIN_TOKEN);
  }

  // ---- admin: verify token ----
  if (path === '/admin/verify' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ ok: false }, 401);
    return json({ ok: true, master: isMaster(request) });
  }

  // ---- admin: upload media to R2 ----
  if (path === '/admin/upload' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    if (!env.NODE_MEDIA) return json({ error: 'R2 bucket NODE_MEDIA not configured' }, 503);

    const formData = await request.formData();
    const file = formData.get('file');
    if (!file) return json({ error: 'no file provided' }, 400);
    if (file.size > 256 * 1024 * 1024) return json({ error: 'file too large (max 256MB)' }, 413); // same cap as import-url

    const ext = (file.name || '').split('.').pop().toLowerCase() || 'bin';
    const key = crypto.randomUUID() + '.' + ext;

    await env.NODE_MEDIA.put(key, file.stream(), {
      httpMetadata: { contentType: file.type || 'application/octet-stream' },
    });

    return json({ url: '/media/' + key, contentType: file.type, key });
  }

  // ---- admin: import a video by URL (TikTok data-export links etc.) ----
  // The Worker pulls the file server-side into R2 — the creator never touches the bytes.
  // Items keep their ORIGINAL date and the feed stays sorted, so imports slot into history
  // instead of burying native posts. Idempotent per source URL.
  if (path === '/admin/import-url' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    if (!env.NODE_MEDIA) return json({ error: 'R2 bucket NODE_MEDIA not configured' }, 503);
    const b = await request.json().catch(() => ({}));
    const srcUrl = (b.url || '').trim();
    if (!srcUrl.startsWith('http://') && !srcUrl.startsWith('https://')) return json({ error: 'url required' }, 400);
    const feed = await getFeed(storage);
    const dup = feed.find(it => it.importedSrc === srcUrl);
    if (dup) {
      // re-running an import can repair metadata (e.g. the 'N/A' titles an earlier parser kept)
      const newTitle = String(b.title || '').slice(0, 300);
      if (newTitle !== (dup.title || '')) { dup.title = newTitle; await saveFeed(storage, feed); }
      return json({ ok: true, dup: true, id: dup.id });
    }

    let res;
    try { res = await fetch(srcUrl, { redirect: 'follow' }); }
    catch (e) { return json({ error: 'fetch failed: ' + e.message }, 502); }
    if (!res.ok) return json({ error: 'source returned ' + res.status + ' (export links expire — request a fresh export)' }, 502);
    const ct = (res.headers.get('Content-Type') || '').split(';')[0].trim() || 'video/mp4';
    if (!ct.startsWith('video/') && ct !== 'application/octet-stream') {
      return json({ error: 'not a video (' + ct + ') — the link may have expired into an error page' }, 415);
    }
    const len = parseInt(res.headers.get('Content-Length') || '0', 10);
    if (len > 256 * 1024 * 1024) return json({ error: 'file too large' }, 413);
    const key = crypto.randomUUID() + '.mp4';
    try {
      if (len > 0) {
        await env.NODE_MEDIA.put(key, res.body, { httpMetadata: { contentType: ct } });
      } else {
        // unknown length — buffer (capped) so R2 gets a sized body
        const buf = await res.arrayBuffer();
        if (buf.byteLength > 256 * 1024 * 1024) return json({ error: 'file too large' }, 413);
        await env.NODE_MEDIA.put(key, buf, { httpMetadata: { contentType: ct } });
      }
    } catch (e) { return json({ error: 'storage failed: ' + e.message }, 502); }

    const when = b.createdAt && !isNaN(Date.parse(b.createdAt)) ? new Date(Date.parse(b.createdAt)).toISOString() : new Date().toISOString();
    const item = {
      id: crypto.randomUUID(),
      type: 'video',
      title: String(b.title || '').slice(0, 300),
      body: '',
      mediaUrl: '/media/' + key,
      mediaContentType: ct,
      importedFrom: String(b.source || 'tiktok').slice(0, 30),
      importedSrc: srcUrl,
      createdAt: when,
      authorPublicKey: identity.publicKey,
    };
    feed.push(item);
    feed.sort((a, c) => (c.createdAt || '').localeCompare(a.createdAt || '')); // newest first
    await saveFeed(storage, feed);
    return json({ ok: true, published: item });
  }

  // ---- admin: publish ----
  if (path === '/admin/publish' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const content = await request.json();
    const feed = await getFeed(storage);
    const item = {
      id: crypto.randomUUID(),
      type: content.type || 'writing',
      title: content.title || '',
      body: content.body || '',
      mediaUrl: content.mediaUrl || null,
      mediaContentType: content.mediaContentType || null,
      transcript: String(content.transcript || '').slice(0, 5000), // searchable text for media posts (OCR/captions later)
      importedFrom: content.importedFrom || 'native',
      createdAt: new Date().toISOString(),
      authorPublicKey: identity.publicKey,
    };
    feed.unshift(item);
    await saveFeed(storage, feed);
    return json({ published: item });
  }

  // ---- admin: delete an item ----
  if (path === '/admin/delete' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    // single {id} or bulk {ids:[...]} — one feed rewrite either way
    const b = await request.json();
    const ids = Array.isArray(b.ids) ? b.ids.slice(0, 500) : (b.id ? [b.id] : []);
    if (!ids.length) return json({ error: 'id or ids required' }, 400);
    const idSet = new Set(ids);
    const feed = await getFeed(storage);
    for (const item of feed) {
      if (idSet.has(item.id) && item.mediaUrl && env.NODE_MEDIA) {
        await env.NODE_MEDIA.delete(item.mediaUrl.replace('/media/', '')).catch(() => {});
      }
    }
    const filtered = feed.filter(i => !idSet.has(i.id));
    await saveFeed(storage, filtered);
    return json({ deleted: feed.length - filtered.length });
  }

  // ---- admin: inherit from ancestor ----
  if (path === '/admin/inherit') {
    if (!(await checkAuth(request))) return new Response('unauthorized — send Authorization: Bearer <ADMIN_TOKEN>', { status: 401 });
    const from = url.searchParams.get('from');
    if (!from) return new Response('missing ?from=', { status: 400 });

    const ancestors = await getAncestors(storage);
    if (ancestors.find(a => a.subdomain === from)) {
      return new Response('already inherited from ' + from);
    }

    try {
      const res = await fetch('https://' + from + '/.well-known/node.json');
      const ancestor = await res.json();
      ancestors.push({
        publicKey: ancestor.publicKey,
        subdomain: ancestor.subdomain,
        inheritedAt: new Date().toISOString(),
      });
      await storage.set('ancestors', ancestors);

      const message = JSON.stringify({
        publicKey: identity.publicKey,
        subdomain,
        timestamp: new Date().toISOString(),
      });
      const signature = await sign(identity.privateKey, message);
      await fetch('https://' + from + '/protocol/announce', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message, signature }),
      });
      return new Response('inherited from ' + from + ' and announced.');
    } catch (e) {
      return new Response('failed: ' + e.message, { status: 500 });
    }
  }

  // ---- live: realtime WebSocket (chat + presence + status push) ----
  if (path === '/live/ws') {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('expected websocket', { status: 426 });
    }
    return liveStub(env, subdomain).fetch(request);
  }

  // ---- live: public status (DO-backed; includes viewer count) ----
  if (path === '/live/status.json') {
    const r = await liveDO(env, subdomain, '/status');
    return jsonCors(await r.json());
  }

  // ---- live: public viewer list (names + sids of connected sockets) ----
  if (path === '/live/who.json') {
    // who's-in-the-room is the broadcaster's moderation view — viewers only get the count
    // (via WS/status.json). Names of people in a room are not public data.
    if (!(await checkAuth(request))) return jsonCors({ error: 'unauthorized' }, 401);
    const r = await liveDO(env, subdomain, '/who');
    return jsonCors(await r.json());
  }

  // ---- live: public chat (DO-backed; WS is preferred, these are fallbacks) ----
  if (path === '/live/chat.json') {
    const r = await liveDO(env, subdomain, '/chat');
    return jsonCors(await r.json());
  }
  if (path === '/live/chat' && request.method === 'POST') {
    const body = await request.text();
    const r = await liveDO(env, subdomain, '/chat', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Chat-Ip': request.headers.get('CF-Connecting-IP') || '' }, body });
    return jsonCors(await r.json().catch(() => ({})), r.status);
  }

  // ---- live: pre-roll ad gate (public) ----
  // REVENUE MODEL: the ad slot belongs to the node being VIEWED. The worker-wide HOST ad
  // (global 'hostad', master-set) serves on every tenant; impressions count in the viewed
  // tenant's own ledger so the host can pay each creator their share %.
  if (path === '/live/preroll' && request.method === 'GET') {
    const cfg = await gstore.get('hostad');
    if (!cfg || !cfg.enabled || !cfg.mediaUrl) return jsonCors({ show: false });
    const vid = url.searchParams.get('vid') || '';
    const r = await liveDO(env, subdomain, '/ad-allowed?vid=' + encodeURIComponent(vid));
    const { allowed } = await r.json();
    if (!allowed) return jsonCors({ show: false });
    return jsonCors({ show: true, ad: {
      mediaUrl: cfg.mediaUrl,
      sponsorName: cfg.sponsorName || '',
      clickUrl: cfg.clickUrl || '',
      durationSec: cfg.durationSec || 15,
      category: cfg.category || 'general',
    }});
  }
  if (path === '/live/preroll/seen' && request.method === 'POST') {
    const { vid } = await request.json().catch(() => ({}));
    await liveDO(env, subdomain, '/ad-seen', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Real-Ip': request.headers.get('CF-Connecting-IP') || '' }, body: JSON.stringify({ vid }) });
    return jsonCors({ ok: true });
  }

  // ---- network measurement (served by the ROOT only) ----
  // "Don't grade your own homework": the viewed node's local meter pays creators, but
  // advertiser-facing numbers come from these root-verified, deduped counts. Clients
  // beacon here in parallel with the local seen-call. Per-host DO 'measure:<host>'.
  if (path === '/measure/impression' && request.method === 'POST') {
    if (!isRoot(identity)) return jsonCors({ error: 'not the network root' }, 403);
    const b = await request.json().catch(() => ({}));
    const host = (b.host || '').trim().toLowerCase();
    const vid = (b.vid || '').slice(0, 64);
    if (!host || !vid) return jsonCors({ error: 'host and vid required' }, 400);
    const members = (await gstore.get('registry:members')) || [];
    if (!members.find(m => m.subdomain === host)) return jsonCors({ error: 'not a network member' }, 400);
    const r = await env.LIVE_ROOM.getByName('measure:' + host).fetch('https://live.do/m-imp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Real-Ip': request.headers.get('CF-Connecting-IP') || '' },
      body: JSON.stringify({ vid }),
    });
    return jsonCors(await r.json(), r.status);
  }
  // Public, auditable: anyone (a sponsor, a host, a skeptic) can read the verified counts.
  if (path === '/measure/stats.json') {
    if (!isRoot(identity)) return jsonCors({ error: 'not the network root' }, 403);
    const host = (url.searchParams.get('host') || '').trim().toLowerCase();
    if (!host) return jsonCors({ error: 'host required' }, 400);
    const r = await env.LIVE_ROOM.getByName('measure:' + host).fetch('https://live.do/m-stats');
    return jsonCors({ host, ...(await r.json()) });
  }
  if (path === '/live/preroll/click' && request.method === 'POST') {
    await liveDO(env, subdomain, '/ad-click', { method: 'POST' });
    return jsonCors({ ok: true });
  }

  // ---- post interactions: likes + comments (public) ----
  if (path === '/post/likes' && request.method === 'GET') {
    const r = await liveDO(env, subdomain, '/likes?postId=' + encodeURIComponent(url.searchParams.get('postId') || '') + '&vid=' + encodeURIComponent(url.searchParams.get('vid') || ''));
    return jsonCors(await r.json());
  }
  if (path === '/post/like' && request.method === 'POST') {
    const body = await request.text();
    const r = await liveDO(env, subdomain, '/like', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
    return jsonCors(await r.json(), r.status);
  }
  if (path === '/post/comments' && request.method === 'GET') {
    const r = await liveDO(env, subdomain, '/comments?postId=' + encodeURIComponent(url.searchParams.get('postId') || ''));
    return jsonCors(await r.json());
  }
  if (path === '/post/comment' && request.method === 'POST') {
    const body = await request.text();
    const r = await liveDO(env, subdomain, '/comment', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
    return jsonCors(await r.json().catch(() => ({})), r.status);
  }

  // ---- live: viewer subscribe session ----
  // The three viewer endpoints below mint/drive PAID Calls sessions on the operator's
  // account, with no auth (viewers are anonymous) — so they get a per-IP rate limit.
  // A real join = 1 subscribe + 1 tracks + ~1 renegotiate; 30/min shared leaves headroom
  // for reconnects while killing mint-in-a-loop abuse.
  if (path === '/live/subscribe' && request.method === 'POST') {
    if (!(await rlAllowed('live', 30, 60000))) return jsonCors({ error: 'rate limited' }, 429);
    const creds = await getCallsCreds(env, storage);
    if (!creds) return jsonCors({ error: 'not configured' }, 503);
    try {
      const data = await callsApi(creds, 'POST', '/sessions/new', undefined);
      return jsonCors({ sessionId: data.sessionId });
    } catch(e) {
      return jsonCors({ error: e.message }, 502);
    }
  }

  // ---- live: viewer subscribe to tracks ----
  if (path === '/live/tracks' && request.method === 'POST') {
    if (!(await rlAllowed('live', 30, 60000))) return jsonCors({ error: 'rate limited' }, 429);
    const creds = await getCallsCreds(env, storage);
    if (!creds) return jsonCors({ error: 'not configured' }, 503);
    try {
      const { subscriberSessionId, tracks } = await request.json();
      const data = await callsApi(creds, 'POST', `/sessions/${subscriberSessionId}/tracks/new`, { tracks });
      return jsonCors(data);
    } catch(e) {
      return jsonCors({ error: e.message }, 502);
    }
  }

  // ---- live: viewer renegotiate ----
  if (path === '/live/renegotiate' && request.method === 'POST') {
    if (!(await rlAllowed('live', 30, 60000))) return jsonCors({ error: 'rate limited' }, 429);
    const creds = await getCallsCreds(env, storage);
    if (!creds) return jsonCors({ error: 'not configured' }, 503);
    const { subscriberSessionId, sessionDescription } = await request.json();
    const data = await callsApi(creds, 'PUT', `/sessions/${subscriberSessionId}/renegotiate`, { sessionDescription });
    return jsonCors(data);
  }

  // ---- live: WHIP ingest (Larix / OBS → Cloudflare Realtime) ----
  if (path === '/live/whip' || path.startsWith('/live/whip/')) {
    const whipCreds = await getCallsCreds(env, storage);
    if (!whipCreds) return new Response('not configured', { status: 503 });

    // WHIP POST — Larix sends SDP offer, we proxy to Cloudflare Realtime WHIP endpoint
    if (request.method === 'POST') {
      // Broadcasting is creator-only. WHIP clients vary in auth support, so accept the
      // creator token either as Authorization: Bearer or as a ?token= query param.
      if (!(await checkAuth(request, url.searchParams.get('token') || undefined))) {
        return new Response('unauthorized — supply your creator token (Authorization: Bearer <token> or ?token=)', { status: 401 });
      }
      const sdpOffer = await request.text();
      const whipRes = await fetch(
        `https://rtc.live.cloudflare.com/v1/apps/${whipCreds.appId}/sessions/whip`,
        {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${whipCreds.appSecret}`,
            'Content-Type': 'application/sdp',
          },
          body: sdpOffer,
        }
      );
      if (!whipRes.ok) {
        const err = await whipRes.text();
        return new Response('WHIP upstream error: ' + err, { status: 502 });
      }
      const sdpAnswer = await whipRes.text();
      const location = whipRes.headers.get('Location') || '';
      // Extract sessionId from Location header: /v1/apps/{appId}/sessions/{sessionId}
      const sessionId = location.split('/sessions/')[1]?.split('/')[0] || '';
      // Extract track names from SDP answer (a=ssrc lines or mid lines)
      const trackNames = [...sdpAnswer.matchAll(/^a=mid:(.+)$/gm)].map(m => m[1].trim());

      // Store in the live room DO — same structure as browser broadcast
      if (sessionId) {
        await liveDO(env, subdomain, '/publish', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ publisherSessionId: sessionId, trackNames, via: 'whip' }),
        });
      }

      return new Response(sdpAnswer, {
        status: 201,
        headers: {
          'Content-Type': 'application/sdp',
          'Location': `/live/whip/${sessionId}`,
          'Access-Control-Allow-Origin': '*',
        },
      });
    }

    // WHIP DELETE — Larix ended the stream. The sessionId in the path acts as a
    // capability: only the publisher that created the session knows it.
    if (request.method === 'DELETE') {
      const sessionId = path.split('/live/whip/')[1];
      const st = await liveDO(env, subdomain, '/status').then(r => r.json()).catch(() => ({}));
      if (!sessionId || st.publisherSessionId !== sessionId) {
        return new Response('unknown session', { status: 403, headers: { 'Access-Control-Allow-Origin': '*' } });
      }
      await liveDO(env, subdomain, '/end', { method: 'POST' });
      // Forward DELETE to Cloudflare Realtime
      if (sessionId) {
        await fetch(
          `https://rtc.live.cloudflare.com/v1/apps/${whipCreds.appId}/sessions/${sessionId}`,
          { method: 'DELETE', headers: { 'Authorization': `Bearer ${whipCreds.appSecret}` } }
        ).catch(() => {});
      }
      return new Response(null, { status: 200, headers: { 'Access-Control-Allow-Origin': '*' } });
    }

    // WHIP OPTIONS (CORS preflight)
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'POST, DELETE, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        },
      });
    }

    return new Response('method not allowed', { status: 405 });
  }

  // ---- live: admin — create publisher session ----
  if (path === '/admin/live/start' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const creds = await getCallsCreds(env, storage);
    if (!creds) return json({ error: 'Calls credentials not set' }, 503);
    try {
      const data = await callsApi(creds, 'POST', '/sessions/new', undefined);
      return json({ sessionId: data.sessionId });
    } catch(e) {
      return json({ error: e.message }, 502);
    }
  }

  // ---- live: admin — add publisher tracks ----
  if (path === '/admin/live/tracks' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const creds = await getCallsCreds(env, storage);
    if (!creds) return json({ error: 'Calls credentials not set' }, 503);
    const { sessionId, tracks, sessionDescription } = await request.json();
    const data = await callsApi(creds, 'POST', `/sessions/${sessionId}/tracks/new`, { tracks, sessionDescription });
    return json(data);
  }

  // ---- live: admin — mark as live (store session + tracks in DO) ----
  if (path === '/admin/live/publish' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const { sessionId, trackNames } = await request.json();
    await liveDO(env, subdomain, '/publish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ publisherSessionId: sessionId, trackNames, via: 'browser' }),
    });
    return json({ ok: true });
  }

  // ---- live: admin — heartbeat (keeps a browser stream from going stale) ----
  if (path === '/admin/live/heartbeat' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    await liveDO(env, subdomain, '/heartbeat', { method: 'POST' });
    return json({ ok: true });
  }

  // ---- live: admin — end ----
  if (path === '/admin/live/end' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    await liveDO(env, subdomain, '/end', { method: 'POST' });
    return json({ ok: true });
  }

  // ---- live: admin — chat moderation (persistent shadow-mute by sid) ----
  if (path === '/admin/live/mute' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const body = await request.text();
    const r = await liveDO(env, subdomain, '/mute', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
    return json(await r.json().catch(() => ({})), r.status);
  }

  // ---- live: admin — broadcast overlay (text/image drawn over the stream client-side;
  // the WebRTC video track is untouched, so latency stays sub-second) ----
  if (path === '/admin/live/overlay' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const b = await request.json().catch(() => ({}));
    const text = String(b.text || '').slice(0, 120);
    let imageUrl = String(b.imageUrl || '').slice(0, 300);
    // own-node media or https only — nothing else reaches viewers' DOM as an img src
    if (imageUrl && !imageUrl.startsWith('/media/') && !imageUrl.startsWith('https://')) imageUrl = '';
    // placement = {x,y} screen fractions + scale, clamped so nothing renders off-screen or absurd
    const place = (o) => (o && typeof o === 'object') ? {
      x: Math.min(Math.max(Number(o.x) || 0.5, 0), 1),
      y: Math.min(Math.max(Number(o.y) || 0.5, 0), 1),
      s: Math.min(Math.max(Number(o.s) || 1, 0.2), 4),
    } : null;
    const r = await liveDO(env, subdomain, '/overlay', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        text, imageUrl,
        // https only — this becomes a tap target on every viewer's screen
        linkUrl: String(b.linkUrl || '').startsWith('https://') ? String(b.linkUrl).slice(0, 300) : '',
        txt: place(b.txt), img: place(b.img),
      }),
    });
    return json(await r.json().catch(() => ({})), r.status);
  }

  // ---- live: admin — past stream sessions (creator analytics) ----
  if (path === '/admin/live/history') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const r = await liveDO(env, subdomain, '/history');
    return json(await r.json());
  }

  // ---- admin: update profile ----
  if (path === '/admin/profile' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const { displayName, bio, avatarUrl } = await request.json();
    const existing = (await storage.get('profile')) || {};
    const updated = {
      ...existing,
      ...(displayName !== undefined ? { displayName: displayName.slice(0, 50) } : {}),
      ...(bio !== undefined ? { bio: bio.slice(0, 200) } : {}),
      ...(avatarUrl !== undefined ? { avatarUrl } : {}),
      updatedAt: new Date().toISOString(),
    };
    await storage.set('profile', updated);
    return json({ ok: true, profile: updated });
  }

  // ---- admin: delete a comment on your own post ----
  if (path === '/admin/comment/delete' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const body = await request.text();
    const r = await liveDO(env, subdomain, '/comment-del', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
    return json(await r.json());
  }

  // ---- admin: pre-roll sponsor ad config ----
  // The node HOST (master) chooses the ad for the whole node and collects the revenue;
  // creators are paid via their share % (/admin/ads). One config per worker.
  if (path === '/admin/preroll' && request.method === 'POST') {
    if (!isMaster(request)) return json({ error: 'master token required — the node host chooses the ad' }, 401);
    const b = await request.json().catch(() => ({}));
    // Normalize the click-through URL instead of silently discarding it: people type
    // "joescoffee.com" — prepend https:// and keep it if it parses as a real URL.
    let clickUrl = (b.clickUrl || '').trim();
    if (clickUrl && !/^https?:\/\//i.test(clickUrl)) clickUrl = 'https://' + clickUrl;
    try { const u = new URL(clickUrl); if (!u.hostname.includes('.')) clickUrl = ''; } catch (e) { clickUrl = ''; }
    const cfg = {
      enabled: !!b.enabled,
      mediaUrl: b.mediaUrl || null,
      contentType: b.contentType || null,
      sponsorName: (b.sponsorName || '').slice(0, 60),
      clickUrl: clickUrl.slice(0, 300),
      durationSec: Math.min(Math.max(parseInt(b.durationSec) || 15, 3), 60),
      cpm: Math.max(0, Math.min(parseFloat(b.cpm) || 0, 1000)), // host's rate, $ per 1000 views
      category: (b.category || 'general').trim().toLowerCase().slice(0, 30), // self-declared — mis-declaring is a membership violation
      source: 'self', // 'self' = host's own sponsor. 'network' reserved for the network ad pool later.
      updatedAt: new Date().toISOString(),
    };
    await gstore.set('hostad', cfg);
    return json({ ok: true, preroll: cfg });
  }
  if (path === '/admin/preroll.json') {
    if (!isMaster(request)) return json({ error: 'master token required' }, 401);
    // prefill fallback: pre-rev-share deploys stored the config per-tenant
    const cfg = (await gstore.get('hostad')) || (await storage.get('preroll')) || { enabled: false };
    const r = await liveDO(env, subdomain, '/ad-stats');
    const stats = await r.json();
    const earnings = +(((stats.impressions || 0) / 1000) * (cfg.cpm || 0)).toFixed(2);
    return json({ preroll: cfg, stats: { ...stats, earnings } });
  }
  // Host ledger: lifetime ad views per tenant × the host's CPM × each creator's share %.
  // This IS the rev-share product even before a payment rail exists: "you owe creator X $Y".
  if (path === '/admin/ads-ledger') {
    if (!isMaster(request)) return json({ error: 'master token required' }, 401);
    const cfg = (await gstore.get('hostad')) || {};
    const cpm = cfg.cpm || 0;
    const hosts = [...new Set([subdomain, ...((await gstore.get('provisioned')) || [])])];
    const rows = [];
    let totalImp = 0, totalClicks = 0, totalOwed = 0;
    for (const h of hosts) {
      const st = await liveDO(env, h, '/ad-stats').then(r => r.json()).catch(() => ({}));
      const imp = st.impressions || 0;
      const sharePct = (((await makeStorage(env, h).get('ads')) || {}).sharePct) || 0;
      const owed = +(((imp / 1000) * cpm * sharePct) / 100).toFixed(2);
      totalImp += imp; totalClicks += st.clicks || 0; totalOwed += owed;
      rows.push({ host: h, impressions: imp, clicks: st.clicks || 0, sharePct, owed });
    }
    const gross = +((totalImp / 1000) * cpm).toFixed(2);
    const networkFee = +(gross * 0.02).toFixed(2); // 2% genesis: 1% maintenance, 1% contributors
    return json({ cpm, rows, totals: {
      impressions: totalImp, clicks: totalClicks, gross,
      owedToCreators: +totalOwed.toFixed(2), networkFee,
      hostNet: +(gross - totalOwed - networkFee).toFixed(2),
    }});
  }
  // A creator's own earnings view: their content's ad views × host CPM × their share %.
  if (path === '/admin/my-earnings') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const cfg = (await gstore.get('hostad')) || {};
    const ads = (await storage.get('ads')) || {};
    const st = await liveDO(env, subdomain, '/ad-stats').then(r => r.json()).catch(() => ({}));
    const gross = ((st.impressions || 0) / 1000) * (cfg.cpm || 0);
    return json({
      impressions: st.impressions || 0, clicks: st.clicks || 0,
      sharePct: ads.sharePct || 0,
      owed: +((gross * (ads.sharePct || 0)) / 100).toFixed(2),
    });
  }

  // ---- admin: inbox ----
  if (path === '/admin/inbox.json') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const notifs = (await storage.get('inbox:notifications')) || [];
    return json({ notifications: notifs, unread: notifs.filter(n => !n.read).length });
  }

  if (path === '/admin/inbox/read' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    const notifs = (await storage.get('inbox:notifications')) || [];
    notifs.forEach(n => { n.read = true; });
    await storage.set('inbox:notifications', notifs);
    return json({ ok: true });
  }

  // ---- network: this node asks the root to join ----
  if (path === '/admin/request-join' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    if (isRoot(identity)) return json({ ok: true, alreadyMember: true });
    // Same-worker short-circuit: if the network root is a tenant on THIS Worker
    // (e.g. scotty.tuliptown.ca joining root social.tuliptown.ca), a fetch to the
    // root host is a Worker self-subrequest and Cloudflare returns 522 (whose body
    // "error code: 522" then fails r.json() → "token 'e' is not valid JSON"). The
    // registry:pending list is global (gstore) and the root inbox is just another
    // tenant's KV, so write the join request directly instead of fetching ourselves.
    const rootStore = makeStorage(env, NETWORK_ROOT_HOST);
    const rootIdentity = await rootStore.get('identity');
    if (rootIdentity && isRoot(rootIdentity)) {
      const members = await getRegistryMembers(env, rootIdentity);
      if (members.find(m => m.pubkey === identity.publicKey)) return json({ ok: true, alreadyMember: true });
      const pending = (await gstore.get('registry:pending')) || [];
      if (!pending.find(p => p.pubkey === identity.publicKey)) {
        pending.unshift({ subdomain, pubkey: identity.publicKey, at: new Date().toISOString() });
        await gstore.set('registry:pending', pending);
        const notifs = (await rootStore.get('inbox:notifications')) || [];
        notifs.unshift({ id: crypto.randomUUID(), type: 'join_request', subdomain, publicKey: identity.publicKey, at: new Date().toISOString(), read: false });
        if (notifs.length > 100) notifs.splice(100);
        await rootStore.set('inbox:notifications', notifs);
      }
      return json({ ok: true, pending: true });
    }
    // Cross-worker (e.g. social.bigdumbvan.com → social.tuliptown.ca): normal signed POST.
    const message = JSON.stringify({ subdomain, pubkey: identity.publicKey, timestamp: new Date().toISOString() });
    const signature = await sign(identity.privateKey, message);
    try {
      const r = await fetch('https://' + NETWORK_ROOT_HOST + '/protocol/join-request', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message, signature }),
      });
      return json(await r.json());
    } catch(e) { return json({ error: e.message }, 502); }
  }

  // ---- network registry admin (root only) ----
  if (path === '/admin/registry.json') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    if (!isRoot(identity)) return json({ error: 'not the network root' }, 403);
    return json({ members: await getRegistryMembers(env, identity), pending: (await gstore.get('registry:pending')) || [] });
  }
  if ((path === '/admin/registry/add' || path === '/admin/registry/approve') && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    if (!isRoot(identity)) return json({ error: 'not the network root' }, 403);
    const b = await request.json().catch(() => ({}));
    if (!b.pubkey) return json({ error: 'pubkey required' }, 400);
    const pending = (await gstore.get('registry:pending')) || [];
    const pIdx = pending.findIndex(p => p.pubkey === b.pubkey);
    const sd = b.subdomain || (pIdx !== -1 ? pending[pIdx].subdomain : '');
    if (!sd) return json({ error: 'subdomain required' }, 400);
    if (pIdx !== -1) { pending.splice(pIdx, 1); await gstore.set('registry:pending', pending); }
    const members = await getRegistryMembers(env, identity);
    if (!members.find(m => m.pubkey === b.pubkey)) members.push({ subdomain: sd, pubkey: b.pubkey, addedAt: new Date().toISOString() });
    await gstore.set('registry:members', members);
    await gstore.set('registry:updatedAt', new Date().toISOString());
    await gstore.set('registry:signed', await registrySigned(env, identity, subdomain));
    try { await caches.default.delete(new Request('https://' + subdomain + '/.well-known/registry.json')); } catch (e) {}
    await resolveJoinNotif(storage, b.pubkey, 'approved');
    return json({ ok: true, members });
  }
  if (path === '/admin/registry/deny' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    if (!isRoot(identity)) return json({ error: 'not the network root' }, 403);
    const { pubkey } = await request.json().catch(() => ({}));
    const pending = ((await gstore.get('registry:pending')) || []).filter(p => p.pubkey !== pubkey);
    await gstore.set('registry:pending', pending);
    await resolveJoinNotif(storage, pubkey, 'denied');
    return json({ ok: true });
  }
  if (path === '/admin/registry/remove' && request.method === 'POST') {
    if (!(await checkAuth(request))) return json({ error: 'unauthorized' }, 401);
    if (!isRoot(identity)) return json({ error: 'not the network root' }, 403);
    const { pubkey } = await request.json().catch(() => ({}));
    if (pubkey === NETWORK_ROOT_PUBKEY) return json({ error: 'cannot remove the root' }, 400);
    const members = (await getRegistryMembers(env, identity)).filter(m => m.pubkey !== pubkey);
    await gstore.set('registry:members', members);
    await gstore.set('registry:updatedAt', new Date().toISOString());
    await gstore.set('registry:signed', await registrySigned(env, identity, subdomain));
    try { await caches.default.delete(new Request('https://' + subdomain + '/.well-known/registry.json')); } catch (e) {}
    return json({ ok: true, members });
  }

  // ---- provisioning (local host-operator: gate the wildcard auto-create surface) ----
  // Auth = host MASTER token only. checkAuth would also accept per-creator sessions,
  // which must not be able to mint new tenants on this Worker.
  if (path === '/admin/provisioned' && request.method === 'GET') {
    if (!isMaster(request)) return json({ error: 'master token required' }, 401);
    const list = (await gstore.get('provisioned')) || [];
    const ads = {};
    for (const h of list) ads[h] = (((await makeStorage(env, h).get('ads')) || {}).sharePct) || 0;
    return json({ provisioned: list, ads });
  }
  // Host sets a creator's revenue share % (the "earnings share" — 0 = no share).
  if (path === '/admin/ads' && request.method === 'POST') {
    if (!isMaster(request)) return json({ error: 'master token required' }, 401);
    const b = await request.json().catch(() => ({}));
    const host = (b.host || '').trim().toLowerCase();
    if (!host) return json({ error: 'host required' }, 400);
    const sharePct = Math.max(0, Math.min(parseFloat(b.sharePct) || 0, 100));
    await makeStorage(env, host).set('ads', { enabled: sharePct > 0, sharePct, setAt: new Date().toISOString() });
    return json({ ok: true, host, sharePct });
  }
  if ((path === '/admin/provision' || path === '/admin/unprovision') && request.method === 'POST') {
    if (!isMaster(request)) return json({ error: 'master token required' }, 401);
    const b = await request.json().catch(() => ({}));
    const host = (b.host || '').trim().toLowerCase();
    if (!host) return json({ error: 'host required' }, 400);
    let list = (await gstore.get('provisioned')) || [];
    if (path === '/admin/provision') {
      if (!list.includes(host)) list.push(host);
    } else {
      list = list.filter(h => h !== host);
    }
    await gstore.set('provisioned', list);
    memSet('prov', list); // refresh this isolate immediately (others converge within 60s)
    return json({ ok: true, provisioned: list });
  }

  // ---- creator credentials (host master only): per-tenant login tokens ----
  // Mint a token scoped to one host; only its hash is stored. The plaintext is
  // returned ONCE — the host hands it to the creator, who pastes it to unlock
  // creator mode on that host only. The host master token still works everywhere.
  if (path === '/admin/creator/mint-token' && request.method === 'POST') {
    if (!isMaster(request)) return json({ error: 'master token required' }, 401);
    const b = await request.json().catch(() => ({}));
    const host = (b.host || '').trim().toLowerCase();
    if (!host) return json({ error: 'host required' }, 400);
    const creatorToken = randomTokenHex();
    await makeStorage(env, host).set('auth', { tokenHash: await sha256hex(creatorToken), setAt: new Date().toISOString() });
    return json({ ok: true, host, creatorToken, note: 'shown once — unlocks creator mode only on ' + host });
  }
  if (path === '/admin/creator/clear-token' && request.method === 'POST') {
    if (!isMaster(request)) return json({ error: 'master token required' }, 401);
    const b = await request.json().catch(() => ({}));
    const host = (b.host || '').trim().toLowerCase();
    if (!host) return json({ error: 'host required' }, 400);
    await makeStorage(env, host).delete('auth');
    return json({ ok: true, host, cleared: true });
  }
  // Per-tenant Realtime/Calls credentials (host master only): a streamer supplies their own
  // Cloudflare Realtime app so THEIR account is billed for their streaming, not the operator's.
  if (path === '/admin/calls-creds' && request.method === 'POST') {
    if (!isMaster(request)) return json({ error: 'master token required' }, 401);
    const b = await request.json().catch(() => ({}));
    const host = (b.host || '').trim().toLowerCase();
    if (!host) return json({ error: 'host required' }, 400);
    const ts = makeStorage(env, host);
    if (b.clear) { await ts.delete('calls'); return json({ ok: true, host, cleared: true }); }
    if (!b.appId || !b.appSecret) return json({ error: 'appId and appSecret required' }, 400);
    await ts.set('calls', { appId: String(b.appId), appSecret: String(b.appSecret), setAt: new Date().toISOString() });
    return json({ ok: true, host, note: 'this host now streams on its own Cloudflare Realtime app (its own bill)' });
  }
  // Host mints a one-time claim code for a handle; the creator redeems it at /auth/claim to set a password.
  if (path === '/admin/creator/mint-claim' && request.method === 'POST') {
    if (!isMaster(request)) return json({ error: 'master token required' }, 401);
    const b = await request.json().catch(() => ({}));
    const host = (b.host || '').trim().toLowerCase();
    if (!host) return json({ error: 'host required' }, 400);
    const ts = makeStorage(env, host);
    const auth = (await ts.get('auth')) || {};
    const claimCode = randomTokenHex();
    auth.claimHash = await sha256hex(claimCode);
    auth.claimed = false;
    auth.setAt = new Date().toISOString();
    await ts.set('auth', auth);
    return json({ ok: true, host, claimCode, claimUrl: 'https://' + host + '/', note: 'give the creator this code (shown once); they claim it at the URL and set their own password' });
  }

  // Generic per-IP sliding-window limiter, tracked in this tenant's DO. Fails open
  // (availability > lockout). Hoisted — callable from routes above this definition.
  async function rlAllowed(prefix, max, windowMs) {
    const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
    try {
      const r = await liveDO(env, subdomain, '/rl', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key: prefix + ':' + ip, max, windowMs }),
      });
      return (await r.json()).allowed !== false;
    } catch (e) { return true; }
  }
  // Brute-force guard for the password endpoints: 10 attempts / 10 min / IP. Each attempt
  // also burns 100k PBKDF2 iterations of OUR cpu — as much a cost cap as a security one.
  function authAllowed() { return rlAllowed('auth', 10, 600000); }

  // ---- creator self-service auth (Phase 2): claim a handle + password login → signed session ----
  if (path === '/auth/claim' && request.method === 'POST') {
    if (!(await authAllowed())) return jsonCors({ error: 'too many attempts — try again in a few minutes' }, 429);
    const b = await request.json().catch(() => ({}));
    const code = (b.code || '').trim();
    const password = b.password || '';
    if (!code || password.length < 8) return jsonCors({ error: 'code and password (min 8 chars) required' }, 400);
    const auth = (await storage.get('auth')) || {};
    if (!auth.claimHash || auth.claimed) return jsonCors({ error: 'no pending claim for this handle' }, 400);
    if (!ctEq(await sha256hex(code), auth.claimHash)) return jsonCors({ error: 'invalid claim code' }, 401);
    const salt = randomTokenHex();
    auth.pwSalt = salt; auth.pwIter = 100000; auth.pwHash = await pbkdf2(password, salt, 100000);
    auth.claimed = true; delete auth.claimHash; auth.setAt = new Date().toISOString();
    await storage.set('auth', auth);
    return jsonCors({ ok: true, session: await makeSession(identity, subdomain) });
  }
  if (path === '/auth/login' && request.method === 'POST') {
    if (!(await authAllowed())) return jsonCors({ error: 'too many attempts — try again in a few minutes' }, 429);
    const b = await request.json().catch(() => ({}));
    const auth = (await storage.get('auth')) || {};
    if (!auth.pwHash) return jsonCors({ error: 'no password set for this handle' }, 400);
    if (!ctEq(await pbkdf2(b.password || '', auth.pwSalt, auth.pwIter || 100000), auth.pwHash)) return jsonCors({ error: 'wrong password' }, 401);
    return jsonCors({ ok: true, session: await makeSession(identity, subdomain) });
  }

  return new Response('not found', { status: 404 });
}

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

function json(data, status = 200, extra = {}) {
  return new Response(JSON.stringify(data, null, 2), {
    status, headers: { 'Content-Type': 'application/json', ...extra },
  });
}
function jsonCors(data, status = 200, extra = {}) { return json(data, status, { ...CORS_HEADERS, ...extra }); }

// Per-tenant Cloudflare Realtime (Calls) credentials — lets each streamer's traffic bill
// their OWN Cloudflare account instead of the operator's. A tenant with creds stored at
// t:<host>:calls uses those; otherwise falls back to the Worker-wide secrets (operator pays).
async function getCallsCreds(env, storage) {
  const own = await storage.get('calls');
  if (own && own.appId && own.appSecret) return { appId: own.appId, appSecret: own.appSecret };
  if (env.CALLS_APP_ID && env.CALLS_APP_SECRET) return { appId: env.CALLS_APP_ID, appSecret: env.CALLS_APP_SECRET };
  return null;
}

async function callsApi(creds, method, path, body) {
  const res = await fetch(
    `https://rtc.live.cloudflare.com/v1/apps/${creds.appId}${path}`,
    {
      method,
      headers: {
        'Authorization': `Bearer ${creds.appSecret}`,
        'Content-Type': 'application/json',
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    }
  );
  const text = await res.text();
  if (!res.ok) throw new Error(`Calls API ${method} ${path} → ${res.status}: ${text}`);
  return JSON.parse(text);
}
function html(content, status = 200) {
  return new Response(content, {
    status,
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      // The SPA is one inline <script>/<style>, so 'unsafe-inline' stays — the win here is
      // blocking foreign script/object injection, framing, and base/form hijacks. Media and
      // avatars load cross-node (https:), upload previews use blob:, chat may use wss:.
      'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src https: data: blob:; media-src https: data: blob:; connect-src https: wss:; worker-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
    },
  });
}
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

// ============================================================
// SEO / SHARE-UNFURL
// ============================================================
// The app is a JS black box to link-unfurlers and non-JS crawlers, so the server
// injects real OG/Twitter meta into <head> and a <noscript> body they can read.
// Used for both the node page (/) and per-post permalinks (/p/<id>).

function absUrl(origin, u) {
  if (!u) return null;
  return u.startsWith('http') ? u : origin + u;
}

function buildSeo({ origin, subdomain, profile, feed, post }) {
  const name = profile.displayName || subdomain.split('.')[0];
  const nodeDesc = (profile.bio || name + ' on Social Node — a sovereign social network').slice(0, 300);
  const avatar = absUrl(origin, profile.avatarUrl) || origin + '/icon.svg';
  const tags = [];
  const tag  = (p, c) => tags.push('<meta property="' + p + '" content="' + escapeHtml(c) + '">');
  const ntag = (n, c) => tags.push('<meta name="' + n + '" content="' + escapeHtml(c) + '">');
  let title, desc, canonical, noscript;
  if (post) {
    const text = [post.title, post.body, post.transcript].filter(Boolean).join(' — ');
    title = (post.title || (post.body || '').slice(0, 70) || 'A post') + ' — ' + name;
    desc = (text || nodeDesc).slice(0, 300);
    canonical = origin + '/p/' + post.id;
    const media = absUrl(origin, post.mediaUrl);
    if (post.type === 'video' && media) {
      // No poster frames in storage — og:video carries the unfurl (Discord/Telegram/WhatsApp
      // render the mp4 inline), avatar stands in as og:image.
      tag('og:type', 'video.other');
      tag('og:video', media);
      tag('og:video:secure_url', media);
      tag('og:video:type', post.mediaContentType || 'video/mp4');
      tag('og:image', avatar);
      ntag('twitter:card', 'summary');
    } else if (post.type === 'photo' && media) {
      tag('og:type', 'article');
      tag('og:image', media);
      ntag('twitter:card', 'summary_large_image');
    } else {
      tag('og:type', 'article');
      tag('og:image', avatar);
      ntag('twitter:card', 'summary');
    }
    noscript = '<h1>' + escapeHtml(post.title || name) + '</h1>'
      + (post.body ? '<p>' + escapeHtml(post.body) + '</p>' : '')
      + (post.transcript ? '<p>' + escapeHtml(post.transcript) + '</p>' : '')
      + '<p><time datetime="' + escapeHtml(post.createdAt || '') + '">' + escapeHtml((post.createdAt || '').slice(0, 10)) + '</time></p>'
      + '<p>By <a href="/">' + escapeHtml(name) + '</a> (' + escapeHtml(subdomain) + ')</p>';
  } else {
    title = name === subdomain.split('.')[0] ? subdomain : name + ' — ' + subdomain;
    desc = nodeDesc;
    canonical = origin + '/';
    tag('og:type', 'profile');
    tag('og:image', avatar);
    ntag('twitter:card', 'summary');
    const items = (feed || []).slice(0, 50).map(i =>
      '<li><a href="/p/' + escapeHtml(i.id) + '">'
      + escapeHtml(i.title || (i.body || '').slice(0, 80) || (i.createdAt || '').slice(0, 10) || 'post')
      + '</a></li>').join('');
    noscript = '<h1>' + escapeHtml(name) + '</h1>'
      + (profile.bio ? '<p>' + escapeHtml(profile.bio) + '</p>' : '')
      + (items ? '<ul>' + items + '</ul>' : '');
  }
  tag('og:title', title);
  tag('og:description', desc);
  tag('og:url', canonical);
  tag('og:site_name', 'Social Node');
  ntag('twitter:title', title);
  ntag('twitter:description', desc);
  const head = '<title>' + escapeHtml(title) + '</title>\n'
    + '<meta name="description" content="' + escapeHtml(desc) + '">\n'
    + '<link rel="canonical" href="' + escapeHtml(canonical) + '">\n'
    + tags.join('\n');
  return { head, noscript: '<noscript>' + noscript + '</noscript>', postId: post ? post.id : '' };
}

// ---- node policies page (/legal) ----
const REPORT_REASONS = ['csam', 'ncii', 'hate', 'harassment', 'copyright', 'defamation', 'other'];

function renderLegalPage(subdomain, name) {
  const h = escapeHtml(subdomain), n = escapeHtml(name);
  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Policies — ${h}</title>
<style>
body{background:#000;color:rgba(255,255,255,0.85);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;line-height:1.65;max-width:680px;margin:0 auto;padding:32px 20px 80px}
h1{font-size:22px;color:#fff}h2{font-size:16px;color:#fff;margin-top:32px}
a{color:#20D5EC}p,li{font-size:14px}code{background:rgba(255,255,255,0.1);padding:1px 5px;border-radius:4px;font-size:13px}
.muted{color:rgba(255,255,255,0.45);font-size:12px}
</style></head><body>
<h1>Policies &amp; reporting — ${n} (${h})</h1>
<p class="muted">This node runs the open-source <a href="https://github.com/aitchisonpeter/social-node">Social Node</a> software. The operator of this hostname is responsible for the content it serves and for acting on the reports below.</p>

<h2>Acceptable use</h2>
<p>Content on this node must be legal where the node is operated. The following is removed on notice, and gets the poster removed for repeat or serious cases:</p>
<ul>
<li><strong>Child sexual abuse material</strong> — removed immediately and reported to authorities. Not negotiable.</li>
<li><strong>Intimate images shared without consent</strong> — removed immediately.</li>
<li>Hate propaganda or incitement to violence.</li>
<li>Targeted harassment or threats.</li>
<li>Content that infringes someone else's copyright (see imported content, below).</li>
<li>Defamatory content, on notice.</li>
</ul>

<h2>How to report</h2>
<p>Use the flag button on any post in the app, or send <code>POST /report</code> to this node with <code>{"postId","reason","details"}</code>. Reports go to this node's operator <em>and</em> to the network root operator. You don't need an account.</p>
<p>If you are reporting child sexual abuse material you can also report directly to <a href="https://www.cybertip.ca">Cybertip.ca</a> or your local police.</p>

<h2>Takedown process</h2>
<ul>
<li>Reports land in the operator's inbox and are reviewed by a human.</li>
<li>Illegal content (CSAM, non-consensual intimate images) is removed on sight; CSAM is reported to authorities as the law requires.</li>
<li>Copyright complaints: include the work, proof you hold rights, and the post link — the operator removes the content or forwards your notice to the poster (Canadian notice-and-notice).</li>
<li>Defamation and other on-notice claims are actioned once the operator is made aware.</li>
<li><strong>Network enforcement:</strong> nodes that won't moderate are removed from the network registry by the root operator. That removes them from cross-node discovery — their hostname and hosting are their own.</li>
</ul>

<h2>Privacy</h2>
<p>This node collects almost nothing:</p>
<ul>
<li>No viewer accounts. Your display name and preferences live in your browser's local storage, on your device.</li>
<li>The node stores what you submit: comments (with the name you chose), like counts keyed to a random per-device id, and reports.</li>
<li>IP addresses are used transiently for rate-limiting and ad-impression deduping, in short rolling windows.</li>
<li>No tracking pixels, no analytics scripts, no data sales.</li>
</ul>

<h2>Imported content</h2>
<p>The import tools (e.g. TikTok export import) re-host <em>your</em> files on <em>this</em> node. Platform licences do not transfer — in particular, commercial music licensed to TikTok is <strong>not</strong> licensed here. You are responsible for holding the rights to everything you import. Rights-holder complaints follow the takedown process above.</p>

<p class="muted" style="margin-top:40px"><a href="/">← back to ${n}</a></p>
</body></html>`;
}

// ============================================================
// PWA MANIFEST
// ============================================================

function renderManifest(subdomain) {
  return {
    name: subdomain,
    short_name: subdomain.split('.')[0],
    description: 'a sovereign social node',
    start_url: '/',
    display: 'standalone',
    orientation: 'portrait',
    background_color: '#000000',
    theme_color: '#000000',
    icons: [
      { src: '/icon.svg', sizes: 'any', type: 'image/svg+xml', purpose: 'any maskable' },
    ],
  };
}

function renderIcon() {
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <rect width="512" height="512" fill="#000"/>
  <circle cx="256" cy="256" r="120" fill="none" stroke="#fff" stroke-width="24"/>
  <circle cx="256" cy="256" r="40" fill="#fff"/>
</svg>`;
}

function renderServiceWorker() {
  return `
const CACHE = 'node-v${PROTOCOL_VERSION}';
self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(['/', '/manifest.json', '/icon.svg'])));
  self.skipWaiting();
});
self.addEventListener('activate', e => {
  e.waitUntil(clients.claim());
});
self.addEventListener('fetch', e => {
  // network-first for everything, cache as fallback
  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request))
  );
});
`;
}

// ============================================================
// THE APP — single-page PWA  (v0.4.0: TikTok-style UI)
// ============================================================
//
//   UP / DOWN  →  move between nodes
//   LEFT / RIGHT  →  move between posts on the current node
//
// Two panels (A/B) are swapped on every committed gesture.
// Only the active panel ever plays video.
// Node feeds are loaded lazily on first visit.
// ============================================================

function renderUnclaimed(subdomain) {
  return `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(subdomain)} — available</title>
<style>
  :root { color-scheme: dark; }
  body { margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center;
    background:#0a0a0a; color:#eee; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; }
  .card { max-width:420px; padding:40px 32px; text-align:center; }
  .dot { width:14px; height:14px; border-radius:50%; background:#ff3b5c; display:inline-block; margin-bottom:24px; }
  h1 { font-size:20px; margin:0 0 8px; font-weight:600; word-break:break-all; color:#ff3b5c; }
  p { color:#999; font-size:14px; line-height:1.6; margin:8px 0 0; }
</style></head><body>
  <div class="card">
    <span class="dot"></span>
    <h1>${escapeHtml(subdomain)}</h1>
    <p>This handle isn't registered on this network yet.</p>
    <p>If you'd like to claim it, contact the network operator.</p>
  </div>
</body></html>`;
}

function renderApp({ identity, subdomain, seo }) {
  const s = seo || {}; // absent in tests/harness — fall back to the plain title
  const seoHead = s.head || ('<title>' + escapeHtml(subdomain) + '</title>');
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
<meta name="theme-color" content="#000000">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="mobile-web-app-capable" content="yes">
${seoHead}
<link rel="manifest" href="/manifest.json">
<link rel="icon" href="/icon.svg" type="image/svg+xml">
<link rel="apple-touch-icon" href="/icon.svg">
<style>
* { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }
html, body {
  height: 100%; width: 100%; overflow: hidden;
  background: #000; color: #fff;
  font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
  font-size: 15px; line-height: 1.4;
  user-select: none; -webkit-user-select: none;
}
button { font: inherit; cursor: pointer; border: none; background: none; color: inherit; }
input, textarea { font: inherit; }

/* STAGE */
#stage {
  position: fixed; inset: 0;
  overflow: hidden; background: #000;
  touch-action: none;
}
.panel {
  position: absolute; inset: 0;
  will-change: transform; background: #000;
  overflow: hidden;
}

/* CARDS */
.card {
  position: absolute; inset: 0;
  display: flex; flex-direction: column;
  overflow: hidden;
}

/* Media fill */
.card-video .c-media,
.card-photo .c-media { position: absolute; inset: 0; }
/* contain, not cover — full frame at its real aspect ratio; top-anchored so the letterbox space is always at the bottom */
.card-video .c-media video { width: 100%; height: 100%; object-fit: contain; object-position: top center; background: #000; display: block; }
.card-photo .c-media img  { width: 100%; height: 100%; object-fit: cover; display: block; }

/* Gradient scrim */
.card-video::after, .card-photo::after {
  content: ''; position: absolute; inset: 0; pointer-events: none; z-index: 1;
  background: linear-gradient(to top, rgba(0,0,0,0.72) 0%, rgba(0,0,0,0.08) 45%, transparent 70%),
              linear-gradient(to bottom, rgba(0,0,0,0.3) 0%, transparent 18%);
}

/* Writing card */
.card-writing { align-items: flex-start; justify-content: flex-end; }
.card-writing .writing-bg { position: absolute; inset: 0; }
.card-writing .writing-content {
  position: relative; z-index: 2;
  padding: 0 16px calc(max(env(safe-area-inset-bottom),16px) + 110px) 16px;
  width: calc(100% - 76px);
}
.writing-title {
  font-size: clamp(22px, 6.5vw, 38px); font-weight: 800;
  line-height: 1.1; letter-spacing: -0.02em;
  margin-bottom: 12px; word-break: break-word;
  text-shadow: 0 2px 8px rgba(0,0,0,0.6);
}
.writing-body {
  font-size: 17px; line-height: 1.5; opacity: 0.92;
  white-space: pre-wrap; word-break: break-word;
  text-shadow: 0 1px 4px rgba(0,0,0,0.5);
  display: -webkit-box; -webkit-line-clamp: 7; -webkit-box-orient: vertical; overflow: hidden;
}

/* Loading / empty / error */
.card-loading, .card-empty, .card-error {
  align-items: center; justify-content: center;
  text-align: center; gap: 12px; background: #0a0a0a;
}
.spinner {
  width: 32px; height: 32px;
  border: 3px solid rgba(255,255,255,0.12);
  border-top-color: #fff; border-radius: 50%;
  animation: spin 0.7s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
.c-label { font-size: 13px; color: rgba(255,255,255,0.45); }

/* Delete btn */
.card-del {
  position: absolute; top: calc(max(env(safe-area-inset-top),0px) + 56px); right: 12px;
  width: 34px; height: 34px; border-radius: 50%;
  background: rgba(0,0,0,0.5); backdrop-filter: blur(6px);
  display: none; align-items: center; justify-content: center;
  font-size: 15px; z-index: 10; color: #fff;
}
body.creator .card-del { display: flex; }

/* ── BOTTOM-LEFT INFO ─────────────────────────────────────── */
.card-info {
  position: absolute; bottom: 0; left: 0;
  right: 76px; z-index: 5;
  padding: 14px 14px calc(max(env(safe-area-inset-bottom),0px) + 64px) 14px;
}
.info-username {
  font-size: 15px; font-weight: 700; margin-bottom: 5px;
  text-shadow: 0 1px 4px rgba(0,0,0,0.5);
}
.info-desc {
  font-size: 14px; line-height: 1.4;
  color: rgba(255,255,255,0.92);
  display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden;
  margin-bottom: 9px;
  text-shadow: 0 1px 3px rgba(0,0,0,0.5);
}
.info-music {
  display: flex; align-items: center; gap: 7px;
  font-size: 13px; color: rgba(255,255,255,0.88);
}
/* ── RIGHT SIDEBAR ────────────────────────────────────────── */
.sidebar {
  position: absolute; right: 6px;
  bottom: calc(max(env(safe-area-inset-bottom),0px) + 64px);
  width: 64px; z-index: 5;
  display: flex; flex-direction: column; align-items: center; gap: 18px;
}
.sidebar-item {
  display: flex; flex-direction: column; align-items: center; gap: 3px;
}
.sidebar-btn {
  width: 48px; height: 48px;
  display: flex; align-items: center; justify-content: center;
}
.sidebar-icon { font-size: 30px; line-height: 1; filter: drop-shadow(0 1px 4px rgba(0,0,0,0.6)); }
.sidebar-label {
  font-size: 12px; font-weight: 600;
  color: rgba(255,255,255,0.92);
  text-shadow: 0 1px 3px rgba(0,0,0,0.7);
}

/* Avatar */
.avatar-wrap { position: relative; width: 48px; height: 48px; margin-bottom: 8px; }
.avatar-circle {
  width: 48px; height: 48px; border-radius: 50%;
  border: 2px solid #fff;
  display: flex; align-items: center; justify-content: center;
  font-size: 19px; font-weight: 800; color: #fff;
}
/* Live overlay edit mode (broadcaster): show the elements are grabbable */
.ov-edit { outline: 1.5px dashed rgba(255,255,255,0.55); outline-offset: 4px; cursor: grab; }

/* Like icon */
.like-icon { font-size: 30px; transition: transform 0.12s; display: block; }
.like-icon.liked { filter: none; }
.like-icon.pop { animation: heartPop 0.28s ease; }
@keyframes heartPop { 0%{transform:scale(1)} 50%{transform:scale(1.5)} 100%{transform:scale(1)} }

/* Double-tap heart burst */
.heart-burst {
  position: fixed; pointer-events: none; z-index: 60;
  font-size: 90px; opacity: 0;
  transform: translate(-50%,-50%) scale(0.2);
}
.heart-burst.burst { animation: heartBurst 0.65s ease forwards; }
@keyframes heartBurst {
  0%   { opacity:0; transform:translate(-50%,-50%) scale(0.2); }
  20%  { opacity:1; transform:translate(-50%,-50%) scale(1.3); }
  55%  { opacity:1; transform:translate(-50%,-50%) scale(1.0); }
  100% { opacity:0; transform:translate(-50%,-50%) scale(0.85); }
}

/* Music disc */
/* ── LIVE UI ──────────────────────────────────────────────── */
.info-live-badge {
  display: inline-block; background: #FE2C55; color: #fff;
  font-size: 11px; font-weight: 700; letter-spacing: 0.05em;
  padding: 2px 7px; border-radius: 4px; margin-bottom: 4px;
}
.live-overlay {
  position: fixed; inset: 0; z-index: 50; pointer-events: none;
  display: flex; flex-direction: column; justify-content: flex-end; padding-bottom: 80px;
}
.live-chat-feed {
  padding: 0 12px; max-height: 40vh; overflow: hidden;
  display: flex; flex-direction: column; justify-content: flex-end; gap: 4px;
}
.live-chat-msg {
  background: rgba(0,0,0,0.45); backdrop-filter: blur(4px);
  border-radius: 16px; padding: 5px 12px;
  font-size: 13px; color: #fff; width: fit-content; max-width: 80%;
}
.live-chat-msg strong { color: #FE2C55; }
.live-viewer-badge {
  position: absolute; top: max(env(safe-area-inset-top),12px); right: 12px;
  background: rgba(0,0,0,0.5); border-radius: 16px;
  padding: 4px 10px; font-size: 12px; color: #fff; font-weight: 600;
  display: flex; align-items: center; gap: 4px;
}
.live-viewer-badge::before { content: '●'; color: #FE2C55; font-size: 10px; }

/* ── LIVE CARD (full-screen in feed) ─────────────────────────── */
.card-live { position: relative; background: #000; overflow: hidden; }
.live-card-badges {
  position: absolute; top: max(env(safe-area-inset-top),16px); left: 12px;
  z-index: 5; display: flex; align-items: center; gap: 8px;
}
.live-card-info {
  position: absolute; bottom: calc(15% + 60px); left: 12px; right: 80px;
  z-index: 5;
}
.live-card-chat-area {
  position: absolute; bottom: 15%; left: 0; right: 80px;
  padding: 0 12px; z-index: 5;
  max-height: 25.5vh; overflow-y: auto; overscroll-behavior: contain;
  -webkit-overflow-scrolling: touch;
  display: flex; flex-direction: column; gap: 5px;
  pointer-events: auto;
}
.live-card-chat-msg {
  background: rgba(0,0,0,0.5); backdrop-filter: blur(6px);
  border-radius: 16px; padding: 5px 12px;
  font-size: 13px; color: #fff; width: fit-content; max-width: 85%;
  animation: chatIn 0.25s ease;
}
.live-card-chat-msg strong { color: #FE2C55; }
@keyframes chatIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:none; } }

/* Progress segs (post position) */
.progress-segs {
  position: fixed; top: max(env(safe-area-inset-top),0px);
  left: 0; right: 0; height: 2px; z-index: 21;
  display: flex; gap: 2px; padding: 0 2px; pointer-events: none;
}
.prog-seg { flex:1; height:2px; border-radius:1px; background:rgba(255,255,255,0.22); }
.prog-seg.active { background:rgba(255,255,255,0.9); }

/* ── BOTTOM NAV ───────────────────────────────────────────── */
.bottom-nav {
  position: fixed; bottom: 0; left: 0; right: 0; z-index: 20;
  padding-bottom: max(env(safe-area-inset-bottom),0px);
  height: calc(max(env(safe-area-inset-bottom),0px) + 54px);
  display: flex; align-items: center; justify-content: space-around;
  background: rgba(0,0,0,0.88);
  border-top: 0.5px solid rgba(255,255,255,0.1);
  backdrop-filter: blur(14px);
}
.bnav-item {
  display: flex; flex-direction: column; align-items: center; gap: 2px;
  flex: 1; padding: 6px 0; background: none; border: none; color: #fff;
}
.bnav-icon { font-size: 24px; line-height: 1; }
.bnav-label { font-size: 10px; color: rgba(255,255,255,0.6); font-weight: 500; }
.bnav-item.active .bnav-label { color: #fff; font-weight: 700; }

/* The + create button — neutral outline; fills in when this account is a creator,
   so the create affordance itself signals creator status (no separate badge). */
.create-btn {
  width: 42px; height: 28px; border-radius: 8px;
  border: 1.5px solid rgba(255,255,255,0.85);
  display: flex; align-items: center; justify-content: center;
  font-size: 20px; font-weight: 400; color: #fff; line-height: 1;
}
body.creator .create-btn { background: #fff; color: #000; border-color: #fff; }

/* ── MODALS ───────────────────────────────────────────────── */
.modal {
  position: fixed; inset: 0; z-index: 100;
  background: rgba(0,0,0,0.97);
  display: none; flex-direction: column;
  padding: max(env(safe-area-inset-top),24px) 24px max(env(safe-area-inset-bottom),24px);
  overflow-y: auto;
}
.modal.show { display: flex; }
.modal-header {
  display: flex; justify-content: space-between; align-items: center;
  margin-bottom: 24px; padding-top: 8px;
}
.modal-title { font-size: 18px; font-weight: 700; }
.modal-close {
  width: 36px; height: 36px; border-radius: 50%;
  background: rgba(255,255,255,0.1);
  display: flex; align-items: center; justify-content: center;
  font-size: 20px;
}
.field { margin-bottom: 20px; }
.field label {
  display: block; font-size: 12px; font-weight: 600;
  color: rgba(255,255,255,0.45); margin-bottom: 8px;
  text-transform: uppercase; letter-spacing: 0.08em;
}
.field input, .field textarea {
  width: 100%; background: rgba(255,255,255,0.06);
  border: 1px solid rgba(255,255,255,0.14);
  color: #fff; padding: 14px; border-radius: 10px; font-size: 16px;
}
.field textarea { resize: vertical; min-height: 120px; line-height: 1.4; }
.field input:focus, .field textarea:focus { outline: none; border-color: rgba(255,255,255,0.45); }
.field input[type="file"] { display: none; }
.file-drop {
  width: 100%; border: 1.5px dashed rgba(255,255,255,0.22);
  border-radius: 12px; padding: 32px 16px; text-align: center; cursor: pointer; display: none;
}
.file-drop.visible { display: block; }
.file-drop.has-file { border-color: rgba(255,255,255,0.7); }
.file-drop-icon { font-size: 32px; margin-bottom: 8px; }
.file-drop-label { font-size: 13px; color: rgba(255,255,255,0.45); }
.file-drop-name { font-size: 13px; color: #fff; margin-top: 8px; word-break: break-all; font-weight: 600; }
.file-preview { display: none; width: 100%; border-radius: 10px; overflow: hidden; margin-top: 12px; }
.file-preview.visible { display: block; }
.file-preview img, .file-preview video { width: 100%; max-height: 240px; object-fit: cover; display: block; }
.upload-progress { display: none; margin-top: 12px; }
.upload-progress.visible { display: block; }
.progress-bar-bg { background: rgba(255,255,255,0.1); border-radius: 999px; height: 3px; overflow: hidden; }
.progress-bar { background: #FE2C55; height: 100%; width: 0%; border-radius: 999px; transition: width 0.2s; }
.progress-label { font-size: 11px; color: rgba(255,255,255,0.4); margin-top: 6px; }
.type-picker { display: grid; grid-template-columns: repeat(3,1fr); gap: 8px; margin-bottom: 20px; }
.type-picker button {
  padding: 14px 8px; border: 1px solid rgba(255,255,255,0.14);
  border-radius: 10px; font-size: 13px; font-weight: 600; color: rgba(255,255,255,0.5);
}
.type-picker button.active { background: #FE2C55; color: #fff; border-color: #FE2C55; }
.submit-btn {
  width: 100%; background: #FE2C55; color: #fff;
  padding: 16px; border-radius: 999px; font-weight: 700;
  font-size: 16px; margin-top: 16px; letter-spacing: 0.01em;
}
.submit-btn:disabled { opacity: 0.4; }

/* ── PROFILE MODAL ──────────────────────────────────────────── */
.profile-sheet {
  position: fixed; inset: 0; z-index: 110;
  background: #111;
  display: none; flex-direction: column;
  overflow: hidden;
}
.profile-sheet.show { display: flex; }
.profile-header {
  display: flex; align-items: center; justify-content: space-between;
  padding: max(env(safe-area-inset-top),16px) 16px 0;
  flex-shrink: 0;
}
.profile-header-handle { font-size: 17px; font-weight: 700; }
.profile-edit-btn {
  background: rgba(255,255,255,0.1); border-radius: 8px;
  padding: 7px 16px; font-size: 13px; font-weight: 600; color: #fff;
  border: 1px solid rgba(255,255,255,0.18);
}
.profile-body { flex: 1; overflow-y: auto; padding-bottom: max(env(safe-area-inset-bottom),16px); }
.profile-hero {
  display: flex; flex-direction: column; align-items: center;
  padding: 20px 24px 16px;
}
.profile-avatar {
  width: 88px; height: 88px; border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 34px; font-weight: 800; color: #fff;
  border: 2.5px solid rgba(255,255,255,0.15); margin-bottom: 12px;
  overflow: hidden; flex-shrink: 0;
}
.profile-avatar img { width: 100%; height: 100%; object-fit: cover; }
.profile-name { font-size: 19px; font-weight: 800; margin-bottom: 3px; }
.profile-handle { font-size: 14px; color: rgba(255,255,255,0.5); margin-bottom: 14px; }
.profile-stats {
  display: flex; gap: 0; width: 100%; justify-content: center;
  border-top: 0.5px solid rgba(255,255,255,0.1);
  border-bottom: 0.5px solid rgba(255,255,255,0.1);
  margin-bottom: 14px;
}
.profile-stat {
  flex: 1; display: flex; flex-direction: column; align-items: center;
  padding: 12px 0;
  border-right: 0.5px solid rgba(255,255,255,0.1);
}
.profile-stat:last-child { border-right: none; }
.profile-stat-n { font-size: 18px; font-weight: 800; }
.profile-stat-l { font-size: 11px; color: rgba(255,255,255,0.45); margin-top: 2px; text-transform: uppercase; letter-spacing: 0.05em; }
.profile-bio { font-size: 14px; color: rgba(255,255,255,0.8); line-height: 1.5; text-align: center; padding: 0 24px 16px; }
.profile-grid {
  display: grid; grid-template-columns: repeat(3,1fr); gap: 1.5px;
}
.profile-grid-item {
  aspect-ratio: 0.75; background: #1a1a1a; position: relative; overflow: hidden; cursor: pointer;
}
.profile-grid-item img, .profile-grid-item video {
  width: 100%; height: 100%; object-fit: cover; display: block; pointer-events: none;
}
.profile-grid-item .grid-type-icon {
  position: absolute; bottom: 5px; left: 6px;
  font-size: 13px; text-shadow: 0 1px 4px rgba(0,0,0,0.8);
}
.profile-grid-item.sel { outline: 3px solid #FE2C55; outline-offset: -3px; opacity: 0.75; }
.profile-grid-item.sel::after {
  content: '✓'; position: absolute; top: 6px; left: 6px;
  width: 22px; height: 22px; border-radius: 50%; background: #FE2C55;
  color: #fff; font-size: 13px; font-weight: 700;
  display: flex; align-items: center; justify-content: center;
}
.grid-del {
  position: absolute; top: 4px; right: 4px;
  width: 24px; height: 24px; border-radius: 50%;
  background: rgba(0,0,0,0.6); color: #fff; border: none;
  font-size: 15px; line-height: 1; z-index: 2;
  display: flex; align-items: center; justify-content: center;
}
.profile-grid-text {
  width: 100%; height: 100%;
  display: flex; align-items: center; justify-content: center;
  padding: 8px; font-size: 12px; font-weight: 600;
  color: rgba(255,255,255,0.85); text-align: center;
  line-height: 1.3; overflow: hidden;
}
.profile-connect-btn {
  width: calc(100% - 48px); margin: 0 24px 16px;
  background: #FE2C55; color: #fff;
  padding: 13px; border-radius: 999px; font-weight: 700;
  font-size: 15px; text-align: center;
}

/* ── EDIT PROFILE SHEET ─────────────────────────────────────── */
.edit-profile-sheet {
  position: fixed; inset: 0; z-index: 120;
  background: #111; display: none; flex-direction: column;
  padding: max(env(safe-area-inset-top),24px) 24px max(env(safe-area-inset-bottom),24px);
  overflow-y: auto;
}
.edit-profile-sheet.show { display: flex; }

/* ── INBOX MODAL ────────────────────────────────────────────── */
.inbox-sheet {
  position: fixed; inset: 0; z-index: 110;
  background: #111; display: none; flex-direction: column;
}
.inbox-sheet.show { display: flex; }
.inbox-header {
  display: flex; align-items: center; justify-content: space-between;
  padding: max(env(safe-area-inset-top),16px) 16px 12px;
  border-bottom: 0.5px solid rgba(255,255,255,0.1);
  flex-shrink: 0;
}
.inbox-title { font-size: 17px; font-weight: 700; }
.inbox-list { flex: 1; overflow-y: auto; padding-bottom: max(env(safe-area-inset-bottom),16px); }
.inbox-notif {
  display: flex; align-items: center; gap: 14px;
  padding: 14px 16px; border-bottom: 0.5px solid rgba(255,255,255,0.07);
  cursor: pointer;
}
.inbox-notif.unread { background: rgba(254,44,85,0.06); }
.inbox-notif-avatar {
  width: 46px; height: 46px; border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 18px; font-weight: 800; color: #fff; flex-shrink: 0;
}
.inbox-notif-body { flex: 1; min-width: 0; }
.inbox-notif-text { font-size: 14px; line-height: 1.4; color: rgba(255,255,255,0.9); }
.inbox-notif-text strong { color: #fff; }
.inbox-notif-time { font-size: 12px; color: rgba(255,255,255,0.35); margin-top: 3px; }
.inbox-unread-dot {
  width: 8px; height: 8px; border-radius: 50%; background: #FE2C55; flex-shrink: 0;
}
.inbox-empty {
  flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center;
  gap: 12px; color: rgba(255,255,255,0.3); font-size: 14px;
}
.inbox-empty-icon { font-size: 48px; }

/* Unread badge on nav */
.bnav-badge {
  position: absolute; top: 4px; right: 8px;
  background: #FE2C55; color: #fff; font-size: 9px; font-weight: 800;
  min-width: 16px; height: 16px; border-radius: 999px;
  display: none; align-items: center; justify-content: center;
  padding: 0 4px;
}
.bnav-badge.show { display: flex; }
.bnav-item { position: relative; }

/* Toast */
.toast {
  position: fixed; bottom: 80px; left: 50%; transform: translateX(-50%);
  background: rgba(30,30,30,0.95); color: #fff;
  padding: 10px 18px; border-radius: 999px;
  font-size: 13px; font-weight: 600; z-index: 200;
  opacity: 0; transition: opacity 0.25s;
  pointer-events: none; white-space: nowrap;
}
.toast.show { opacity: 1; }
</style>
</head>
<body>
${s.noscript || ''}

<div id="stage">
  <div id="panel-a" class="panel"></div>
  <div id="panel-b" class="panel"></div>
</div>

<!-- Double-tap heart burst -->
<div class="heart-burst" id="heartBurst">❤️</div>

<!-- Progress bar (post position) -->

<!-- First-visit hint (pointer-events:none — swipes pass through) -->
<div id="swipeHint" style="display:none;position:fixed;inset:0;z-index:80;background:rgba(0,0,0,0.55);align-items:center;justify-content:center;flex-direction:column;gap:16px;pointer-events:none">
  <div style="font-size:40px">⬆️</div>
  <div style="font-size:16px;font-weight:700">Swipe up for the next creator</div>
  <div style="font-size:13px;color:rgba(255,255,255,0.75)">Swipe left / right for more from this one</div>
</div>

<!-- Bottom nav -->
<div class="bottom-nav">
  <button class="bnav-item" id="bnavProfile" onclick="onProfileTap()">
    <span class="bnav-icon">👤</span>
    <span class="bnav-label">Profile</span>
  </button>
  <button class="bnav-item" id="bnavLive" onclick="onLiveTap()">
    <span class="bnav-icon">🔴</span>
    <span class="bnav-label">Live</span>
  </button>
  <button class="bnav-item" onclick="onCreateTap()">
    <span class="create-btn">+</span>
    <span class="bnav-label">Create</span>
  </button>
  <button class="bnav-item" id="bnavInbox" onclick="onInboxTap()">
    <span class="bnav-icon">💬</span>
    <span class="bnav-label">Inbox</span>
    <span class="bnav-badge" id="inboxBadge"></span>
  </button>
  <button class="bnav-item" id="bnavSearch" onclick="onSearchTap()">
    <span class="bnav-icon">🔍</span>
    <span class="bnav-label">Search</span>
  </button>
</div>

<!-- Search modal -->
<div class="modal" id="searchModal">
  <div class="modal-header">
    <div class="modal-title">Search</div>
    <button class="modal-close" onclick="document.getElementById('searchModal').classList.remove('show')">×</button>
  </div>
  <input id="searchInput" class="field-input" placeholder="Node address (e.g. name.domain.com)" style="margin-bottom:12px" oninput="onSearchInput(this.value)">
  <div id="searchResults"></div>
</div>

<!-- Unlock modal -->
<div class="modal" id="unlockModal">
  <div class="modal-header">
    <div class="modal-title">Creator Mode</div>
    <button class="modal-close" onclick="closeUnlock()">×</button>
  </div>
  <div class="field">
    <label>Password</label>
    <input type="password" id="pwInput" placeholder="your password" autocomplete="current-password" onkeydown="if(event.key==='Enter')submitLogin()">
  </div>
  <button class="submit-btn" onclick="submitLogin()">Log in</button>
  <p style="margin-top:14px;font-size:12px;text-align:center;line-height:1.8">
    <a href="#" onclick="toggleClaim();return false" style="color:#20D5EC">First time? Claim with a code</a><br>
    <a href="#" onclick="toggleTokenUnlock();return false" style="color:rgba(255,255,255,0.5)">Use an admin / creator token</a>
  </p>
  <div id="claimForm" style="display:none;margin-top:6px;border-top:1px solid rgba(255,255,255,0.1);padding-top:14px">
    <div class="field"><label>Claim code</label><input type="text" id="claimCode" placeholder="code from your host" autocomplete="off"></div>
    <div class="field"><label>Set a password (min 8)</label><input type="password" id="claimPw" placeholder="new password" autocomplete="new-password"></div>
    <button class="submit-btn" onclick="submitClaim()">Claim &amp; set password</button>
  </div>
  <div id="tokenForm" style="display:none;margin-top:6px;border-top:1px solid rgba(255,255,255,0.1);padding-top:14px">
    <div class="field"><label>Admin / creator token</label><input type="password" id="tokenInput" placeholder="paste token" autocomplete="off"></div>
    <button class="submit-btn" onclick="submitToken()">Unlock</button>
  </div>
</div>

<!-- Manage creators (host master only) — z-index above the profile-sheet (110) it's opened from -->
<div class="modal" id="creatorsModal" style="z-index:130">
  <div class="modal-header">
    <div class="modal-title">Manage Creators</div>
    <button class="modal-close" onclick="closeCreators()">×</button>
  </div>
  <div class="field">
    <label>New creator handle</label>
    <input type="text" id="newCreatorHandle" placeholder="e.g. alice" autocomplete="off" onkeydown="if(event.key==='Enter')addCreator()">
  </div>
  <button class="submit-btn" onclick="addCreator()">Create &amp; get invite link</button>
  <div id="creatorLinkBox" style="display:none;margin-top:14px;padding:12px;background:rgba(32,213,236,0.08);border:1px solid rgba(32,213,236,0.3);border-radius:10px">
    <div style="font-size:12px;color:rgba(255,255,255,0.6);margin-bottom:6px">Send this invite link to the creator — they set their own password:</div>
    <div id="creatorLink" style="font-size:12px;word-break:break-all;color:#20D5EC"></div>
    <button class="btn-secondary" style="margin-top:10px" onclick="copyCreatorLink()">Copy link</button>
  </div>
  <div id="creatorList" style="margin-top:18px"></div>
</div>

<!-- Name modal (guest display name for comments + live chat) -->
<div class="modal" id="nameModal" style="z-index:130">
  <div class="modal-header">
    <div class="modal-title">Choose a name</div>
    <button class="modal-close" onclick="closeNameModal()">×</button>
  </div>
  <div class="field">
    <label>Display name</label>
    <input type="text" id="nameInput" placeholder="e.g. Alex" maxlength="30" autocomplete="off" onkeydown="if(event.key==='Enter')saveName()">
  </div>
  <button class="submit-btn" onclick="saveName()">Continue</button>
  <p style="margin-top:16px;font-size:12px;color:rgba(255,255,255,0.4);line-height:1.6">
    This is how you'll appear in comments and live chat. Stored on this device.
  </p>
</div>

<!-- Live fullscreen overlay -->
<div id="liveModal" style="display:none;position:fixed;inset:0;z-index:100;background:#000;flex-direction:column">
  <!-- Fullscreen video -->
  <video disableremoteplayback x-webkit-airplay="deny" id="livePreview"   autoplay muted     playsinline style="display:none;position:absolute;inset:0;width:100%;height:100%;object-fit:cover;z-index:1;transform:scaleX(-1)"></video>
  <video disableremoteplayback x-webkit-airplay="deny" id="liveViewVideo" autoplay playsinline style="display:none;position:absolute;inset:0;width:100%;height:100%;object-fit:cover;z-index:1"></video>

  <!-- Broadcaster's text/image overlay — rendered client-side over the stream (z above video,
       below ad/controls). Elements are centered on {x,y} screen fractions; the broadcaster
       drags/pinches them in place (layer z is raised while editing so they're grabbable). -->
  <div id="liveOverlayLayer" style="display:none;position:absolute;inset:0;z-index:6;pointer-events:none;overflow:hidden">
    <img id="liveOverlayImg" alt="" draggable="false" style="display:none;position:absolute;transform:translate(-50%,-50%);width:30vw;border-radius:10px;box-shadow:0 2px 12px rgba(0,0,0,0.5);touch-action:none">
    <div id="liveOverlayText" style="display:none;position:absolute;transform:translate(-50%,-50%);width:max-content;max-width:86vw;text-align:center;font-size:19px;font-weight:700;color:#fff;text-shadow:0 1px 5px rgba(0,0,0,0.85);line-height:1.35;word-wrap:break-word;touch-action:none"></div>
  </div>

  <!-- Pre-roll sponsor ad overlay (covers the stream until the ad finishes) -->
  <div id="prerollOverlay" style="display:none;position:absolute;inset:0;z-index:30;background:#000">
    <video disableremoteplayback x-webkit-airplay="deny" id="prerollVideo" playsinline style="position:absolute;inset:0;width:100%;height:100%;object-fit:contain;background:#000"></video>
    <div style="position:absolute;top:max(env(safe-area-inset-top),16px);left:16px;background:rgba(0,0,0,0.5);border-radius:6px;padding:4px 8px;font-size:11px;letter-spacing:0.08em;text-transform:uppercase;color:rgba(255,255,255,0.85)">Ad</div>
    <div id="prerollCountdown" style="position:absolute;top:max(env(safe-area-inset-top),16px);right:60px;background:rgba(0,0,0,0.6);border-radius:16px;padding:6px 12px;font-size:13px;font-weight:600;color:#fff">Stream starts soon…</div>
    <div id="prerollSponsor" onclick="onPrerollClick()" style="display:none;position:absolute;bottom:max(env(safe-area-inset-bottom),20px);left:16px;right:16px;background:rgba(0,0,0,0.55);backdrop-filter:blur(6px);border-radius:12px;padding:12px 16px;color:#fff;font-size:14px;cursor:pointer"></div>
  </div>

  <!-- Top bar -->
  <div style="position:absolute;top:max(env(safe-area-inset-top),16px);left:0;right:0;padding:0 16px;display:flex;align-items:center;justify-content:space-between;z-index:41">
    <div style="display:flex;align-items:center;gap:8px">
      <div class="info-live-badge" style="font-size:13px;padding:4px 10px" id="liveModalTitle">LIVE</div>
      <div class="live-viewer-badge" style="position:static;cursor:pointer" id="liveViewerBadge" onclick="openLiveViewers()"><span id="liveViewerCount">0</span></div>
    </div>
    <button onclick="closeLiveModal()" style="background:rgba(0,0,0,0.5);border:none;color:#fff;font-size:22px;width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center">×</button>
  </div>

  <!-- Broadcaster controls (creator only) -->
  <div id="liveBroadcasterPanel" style="display:none;position:absolute;top:max(calc(env(safe-area-inset-top) + 64px),80px);left:0;right:0;flex-direction:column;align-items:center;gap:8px;z-index:9">
    <div style="display:flex;gap:8px">
      <button class="btn-primary" id="liveBtnStart" onclick="liveStart()" style="min-width:120px">Start Live</button>
      <button class="btn-secondary" id="liveBtnEnd" onclick="liveEnd()" style="display:none;min-width:120px">End Live</button>
    </div>
    <div style="display:flex;gap:8px">
      <button id="liveBtnPreroll" onclick="openPrerollSheet()" style="background:rgba(255,255,255,0.12);border:none;color:#fff;border-radius:8px;padding:7px 14px;font-size:13px;font-weight:600">🎬 Sponsor Ad</button>
      <button onclick="openOverlaySheet()" style="background:rgba(255,255,255,0.12);border:none;color:#fff;border-radius:8px;padding:7px 14px;font-size:13px;font-weight:600">🖼 Overlay</button>
      <button onclick="openStreamHistory()" style="background:rgba(255,255,255,0.12);border:none;color:#fff;border-radius:8px;padding:7px 14px;font-size:13px;font-weight:600">📊 Past streams</button>
    </div>
    <div id="liveStatusMsg" style="font-size:13px;color:rgba(255,255,255,0.7);text-align:center"></div>
  </div>

  <!-- Viewer status -->
  <div id="liveViewStatus" style="display:none;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:rgba(255,255,255,0.7);font-size:14px;z-index:10">Connecting…</div>
  <!-- Tap to unmute (shown if autoplay blocks audio) -->
  <div id="liveUnmuteBtn" onclick="const v=document.getElementById('liveViewVideo');v.muted=false;v.play().catch(()=>{});this.style.display='none'" style="display:none;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);background:rgba(0,0,0,0.6);border-radius:24px;padding:12px 24px;color:#fff;font-size:15px;font-weight:600;z-index:11;align-items:center;gap:8px;cursor:pointer">🔊 Tap to unmute</div>

  <!-- Chat overlay — sits just above the chat input; capped height, scrollable history -->
  <div style="position:absolute;bottom:calc(max(env(safe-area-inset-bottom),12px) + 52px);left:0;width:75%;max-height:25.5vh;overflow-y:auto;overscroll-behavior:contain;-webkit-overflow-scrolling:touch;padding:0 16px;z-index:10;display:flex;flex-direction:column;gap:5px;pointer-events:auto" id="liveChatBox"></div>

  <!-- Chat input -->
  <div style="position:absolute;bottom:max(env(safe-area-inset-bottom),12px);left:0;right:0;padding:0 16px;display:flex;gap:8px;z-index:10">
    <input id="liveChatInput" placeholder="Say something…" style="flex:1;background:rgba(255,255,255,0.15);border:none;border-radius:20px;padding:8px 16px;color:#fff;font-size:14px;outline:none" onkeydown="if(event.key==='Enter')liveSendChat()">
    <button onclick="liveSendChat()" style="background:rgba(255,255,255,0.15);border:none;color:#fff;border-radius:20px;padding:8px 16px;font-size:14px">Send</button>
  </div>
</div>

<!-- Bulk-select action bar (own profile grid) -->
<div id="selBar" style="display:none;position:fixed;bottom:0;left:0;right:0;z-index:120;background:rgba(18,18,18,0.97);border-top:1px solid rgba(255,255,255,0.1);padding:12px 16px calc(max(env(safe-area-inset-bottom),12px)) 16px;gap:10px;align-items:center">
  <button class="btn-secondary" style="flex:1" onclick="toggleSelMode()">Cancel</button>
  <button id="selDeleteBtn" class="submit-btn" style="flex:2;margin:0;background:#FE2C55" onclick="bulkDelete()" disabled>Delete (0)</button>
</div>

<!-- My algorithm — viewer-tunable feed ranking (all on-device) -->
<div class="modal" id="algoModal" style="z-index:130">
  <div class="modal-header">
    <div class="modal-title">My Algorithm</div>
    <button class="modal-close" onclick="closeAlgo()">×</button>
  </div>
  <p style="font-size:12px;color:rgba(255,255,255,0.5);line-height:1.5;margin-bottom:14px">
    Ranking runs on your device, on your data. Nothing leaves your phone.
  </p>
  <div class="field"><label>My taste (creators you watch most)</label>
    <input type="range" id="algoTaste" min="0" max="100" style="width:100%"></div>
  <div class="field"><label>Recency (newest posts first)</label>
    <input type="range" id="algoRecent" min="0" max="100" style="width:100%"></div>
  <div class="field"><label>Discovery (shuffle in surprises)</label>
    <input type="range" id="algoFresh" min="0" max="100" style="width:100%"></div>
  <button class="submit-btn" onclick="applyAlgo()">Apply</button>
  <button class="btn-secondary" style="margin-top:10px;width:100%" onclick="resetAffinity()">Reset my taste data</button>
</div>

<!-- TikTok import — opened from the profile sheet (z 110), so needs a higher z -->
<div class="modal" id="importModal" style="z-index:130">
  <div class="modal-header">
    <div class="modal-title">Import from TikTok</div>
    <button class="modal-close" onclick="closeImport()">×</button>
  </div>
  <div id="importIntro">
    <p style="font-size:13px;color:rgba(255,255,255,0.65);line-height:1.6;margin-bottom:14px">
      In TikTok: <strong>Profile → ☰ → Settings and privacy → Account → Download your data</strong>.
      Choose <strong>JSON</strong> format. When it's ready (can take a day), download the ZIP, open it,
      and pick the <strong>user_data*.json</strong> file here. Your videos import with their original
      captions and dates — fresh exports work best (video links inside expire).
    </p>
    <p style="font-size:12px;color:rgba(255,255,255,0.45);line-height:1.6;margin-bottom:14px">
      ⚠️ Only import what you have the rights to re-host. TikTok's music licences do <strong>not</strong>
      transfer with your videos — commercial soundtracks become your responsibility here.
      <a href="/legal" target="_blank" style="color:#20D5EC">Details</a>
    </p>
    <input type="file" id="importFile" accept=".json,application/json" style="display:none" onchange="onImportFile(this.files[0])">
    <button class="submit-btn" onclick="document.getElementById('importFile').click()">Choose export file…</button>
  </div>
  <div id="importPlan" style="display:none">
    <div id="importSummary" style="font-size:14px;margin-bottom:12px"></div>
    <button class="submit-btn" id="importStartBtn" onclick="runImport()">Import all</button>
  </div>
  <div id="importProgress" style="display:none">
    <div id="importStatus" style="font-size:14px;margin-bottom:8px"></div>
    <div style="height:6px;background:rgba(255,255,255,0.1);border-radius:3px;overflow:hidden"><div id="importBar" style="height:100%;width:0%;background:#20D5EC"></div></div>
    <div id="importErrors" style="font-size:12px;color:#FE2C55;margin-top:10px;max-height:25vh;overflow-y:auto"></div>
    <button class="btn-secondary" id="importCancelBtn" style="margin-top:12px" onclick="_importCancel=true">Cancel</button>
  </div>
</div>

<!-- Live stream overlay editor (broadcaster) — opened from inside #liveModal (z 100) -->
<div class="modal" id="overlayModal" style="z-index:130">
  <div class="modal-header">
    <div class="modal-title">Stream overlay</div>
    <button class="modal-close" onclick="closeOverlaySheet()">×</button>
  </div>
  <div class="field"><label>Text (shows above chat)</label>
    <input type="text" id="ovText" maxlength="120" placeholder="e.g. Merch drops Friday — link in bio" autocomplete="off"></div>
  <div class="field"><label>Link (makes the text a tappable button)</label>
    <input type="url" id="ovLink" maxlength="300" placeholder="https://…" autocomplete="off"></div>
  <div class="field"><label>Image (corner badge)</label>
    <input type="file" id="ovFile" accept="image/*" style="display:none" onchange="onOverlayPick(this)">
    <div style="display:flex;gap:10px;align-items:center">
      <button class="btn-secondary" style="flex:1" onclick="document.getElementById('ovFile').click()">Choose image…</button>
      <div id="ovPreview" style="width:52px;height:52px;border-radius:8px;background:rgba(255,255,255,0.08);overflow:hidden;flex-shrink:0"></div>
    </div>
  </div>
  <button class="submit-btn" id="ovApplyBtn" onclick="applyOverlay()">Show overlay</button>
  <button class="btn-secondary" style="margin-top:10px;width:100%" onclick="clearOverlay()">Clear overlay</button>
  <p style="margin-top:12px;font-size:12px;color:rgba(255,255,255,0.4);line-height:1.6">
    Viewers see this over your stream instantly. Once it's up, <strong>drag</strong> the text or image
    to move it and <strong>pinch</strong> to resize — viewers follow along. Clears when the stream ends.
  </p>
</div>

<!-- Report content — public moderation flag, goes to the node operator + network root -->
<div class="modal" id="reportModal" style="z-index:130">
  <div class="modal-header">
    <div class="modal-title">Report this post</div>
    <button class="modal-close" onclick="closeReport()">×</button>
  </div>
  <div class="field"><label>Reason</label>
    <select id="reportReason" style="width:100%;background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.14);color:#fff;padding:14px;border-radius:10px;font-size:16px">
      <option value="other" selected>Something else</option>
      <option value="csam">Child sexual abuse material</option>
      <option value="ncii">Intimate images shared without consent</option>
      <option value="hate">Hate speech or incitement</option>
      <option value="harassment">Harassment or threats</option>
      <option value="copyright">Copyright infringement</option>
      <option value="defamation">Defamation</option>
    </select></div>
  <div class="field"><label>Details (optional)</label>
    <textarea id="reportDetails" maxlength="1000" style="min-height:80px" placeholder="Anything that helps the operator act on this"></textarea></div>
  <button class="submit-btn" style="background:#FE2C55" onclick="submitReport()">Send report</button>
  <p style="margin-top:12px;font-size:12px;color:rgba(255,255,255,0.4);line-height:1.6">
    Your report goes to this node's operator and the network root. No account needed.
    <a href="/legal" target="_blank" style="color:#20D5EC">Policies &amp; reporting</a>
  </p>
</div>

<!-- Live viewer list (+ mute moderation for the broadcaster) — opened from inside #liveModal (z 100) -->
<div class="modal" id="liveViewersModal" style="z-index:130">
  <div class="modal-header">
    <div class="modal-title">In this live</div>
    <button class="modal-close" onclick="closeLiveViewers()">×</button>
  </div>
  <div id="liveViewersBody" style="max-height:55vh;overflow-y:auto">Loading…</div>
</div>

<!-- Past streams (broadcaster analytics) — opened from inside #liveModal (z 100), so needs a higher z -->
<div class="modal" id="streamHistoryModal" style="z-index:130">
  <div class="modal-header">
    <div class="modal-title">Past streams</div>
    <button class="modal-close" onclick="closeStreamHistory()">×</button>
  </div>
  <div id="streamHistoryBody" style="max-height:60vh;overflow-y:auto">Loading…</div>
  <p style="margin-top:12px;font-size:11px;color:rgba(255,255,255,0.4);line-height:1.5">
    Data &amp; cost are estimates: ~2.5 Mbps per viewer × watch time, egress at $0.05/GB (first 1 TB/month free).
  </p>
</div>

<!-- Profile sheet -->
<div class="profile-sheet" id="profileSheet">
  <div class="profile-header">
    <button onclick="closeProfile()" style="background:none;color:#fff;font-size:22px;width:36px;height:36px;display:flex;align-items:center;justify-content:center">←</button>
    <div class="profile-header-handle" id="profileHandle"></div>
    <button class="profile-edit-btn" id="profileEditBtn" style="display:none" onclick="openEditProfile()">Edit</button>
    <div style="width:36px" id="profileEditSpacer"></div>
  </div>
  <div class="profile-body" id="profileBody">
    <div style="display:flex;align-items:center;justify-content:center;padding:60px;color:rgba(255,255,255,0.3)">Loading…</div>
  </div>
</div>

<!-- Edit profile sheet -->
<div class="edit-profile-sheet" id="editProfileSheet">
  <div class="modal-header">
    <div class="modal-title">Edit Profile</div>
    <button class="modal-close" onclick="closeEditProfile()">×</button>
  </div>
  <div style="display:flex;flex-direction:column;align-items:center;margin-bottom:24px">
    <div id="editAvatarPreview" class="profile-avatar" style="margin-bottom:10px;cursor:pointer" onclick="document.getElementById('avatarInput').click()"></div>
    <button style="font-size:13px;color:rgba(255,255,255,0.5)" onclick="document.getElementById('avatarInput').click()">Change photo</button>
    <input type="file" id="avatarInput" accept="image/*" style="display:none" onchange="onAvatarPick(this)">
  </div>
  <div class="field">
    <label>Display Name</label>
    <input type="text" id="editDisplayName" placeholder="Your name" maxlength="50">
  </div>
  <div class="field">
    <label>Bio</label>
    <textarea id="editBio" placeholder="Tell your story…" maxlength="200" style="min-height:80px"></textarea>
  </div>
  <button class="submit-btn" id="saveProfileBtn" onclick="saveProfile()">Save</button>
</div>

<!-- Pre-roll sponsor ad sheet (creator) -->
<div class="edit-profile-sheet" id="prerollSheet">
  <div class="modal-header">
    <div class="modal-title">Pre-roll Sponsor Ad</div>
    <button class="modal-close" onclick="closePrerollSheet()">×</button>
  </div>
  <p style="font-size:13px;color:rgba(255,255,255,0.5);line-height:1.5;margin-bottom:20px">
    Your node's sponsor ad — a short clip shown before live streams and occasionally between feed posts, <strong>across every creator on this node</strong>. You collect the revenue and share it per-creator (Manage Creators → Share %). Each viewer sees it at most once per 8&nbsp;minutes.
  </p>
  <label style="display:flex;align-items:center;gap:10px;margin-bottom:20px;font-size:15px">
    <input type="checkbox" id="prerollEnabled" style="width:20px;height:20px"> Show pre-roll ad
  </label>
  <div class="field">
    <label>Ad video (≤15s)</label>
    <div class="file-drop visible" id="prerollDrop" onclick="document.getElementById('prerollFile').click()">
      <div class="file-drop-icon">🎬</div>
      <div class="file-drop-label">tap to choose a video</div>
      <div class="file-drop-name" id="prerollFileName"></div>
    </div>
    <input type="file" id="prerollFile" accept="video/*" style="display:none" onchange="onPrerollFilePick(this)">
  </div>
  <div class="field">
    <label>Sponsor name</label>
    <input type="text" id="prerollSponsorName" placeholder="e.g. Joe's Coffee" maxlength="60">
  </div>
  <div class="field">
    <label>Click-through URL (optional)</label>
    <input type="text" id="prerollClickUrl" placeholder="https://…" maxlength="300">
  </div>
  <div class="field">
    <label>Ad category (viewers can mute categories)</label>
    <input type="text" id="prerollCategory" placeholder="e.g. food, auto, fashion" maxlength="30">
  </div>
  <div class="field">
    <label>Your rate — CPM ($ per 1000 views)</label>
    <input type="text" id="prerollCpm" inputmode="decimal" placeholder="e.g. 4.00">
  </div>
  <div id="prerollStats" style="background:rgba(255,255,255,0.05);border-radius:12px;padding:14px;margin-bottom:16px"></div>
  <div id="hostLedger" style="background:rgba(255,255,255,0.05);border-radius:12px;padding:14px;margin-bottom:16px;display:none"></div>
  <button class="submit-btn" id="prerollSaveBtn" onclick="savePreroll()">Save</button>
</div>

<!-- Creator earnings (hosted creators with a revenue share) -->
<div class="edit-profile-sheet" id="earningsSheet">
  <div class="modal-header">
    <div class="modal-title">Your Earnings</div>
    <button class="modal-close" onclick="document.getElementById('earningsSheet').classList.remove('show')">×</button>
  </div>
  <div id="earningsBody" style="background:rgba(255,255,255,0.05);border-radius:12px;padding:16px">Loading…</div>
  <p style="margin-top:14px;font-size:12px;color:rgba(255,255,255,0.4);line-height:1.6">
    Your host runs the sponsor ad on this node and shares revenue from ad views on YOUR content at the % shown. Payouts are settled by your host.
  </p>
</div>

<!-- Inbox sheet -->
<div class="inbox-sheet" id="inboxSheet">
  <div class="inbox-header">
    <button onclick="closeInbox()" style="background:none;color:#fff;font-size:22px;width:36px;height:36px;display:flex;align-items:center;justify-content:center">←</button>
    <div class="inbox-title">Inbox</div>
    <button onclick="markInboxRead()" style="font-size:13px;color:rgba(255,255,255,0.4)">Mark read</button>
  </div>
  <div class="inbox-list" id="inboxList">
    <div class="inbox-empty"><div class="inbox-empty-icon">💬</div>No notifications yet</div>
  </div>
</div>

<!-- Comments sheet -->
<div class="inbox-sheet" id="commentsSheet">
  <div class="inbox-header">
    <button onclick="closeComments()" style="background:none;color:#fff;font-size:22px;width:36px;height:36px;display:flex;align-items:center;justify-content:center">←</button>
    <div class="inbox-title">Comments</div>
    <div style="width:36px"></div>
  </div>
  <div class="inbox-list" id="commentsList"></div>
  <div style="display:flex;gap:8px;padding:10px 12px calc(max(env(safe-area-inset-bottom),10px) + 4px);border-top:0.5px solid rgba(255,255,255,0.1)">
    <input id="commentInput" placeholder="Add a comment…" maxlength="300" style="flex:1;background:rgba(255,255,255,0.08);border:none;border-radius:20px;padding:10px 16px;color:#fff;font-size:14px;outline:none" onkeydown="if(event.key==='Enter')submitComment()">
    <button onclick="submitComment()" style="background:#FE2C55;border:none;color:#fff;border-radius:20px;padding:10px 18px;font-size:14px;font-weight:600">Post</button>
  </div>
</div>

<!-- Publish modal -->
<div class="modal" id="publishModal">
  <div class="modal-header">
    <div class="modal-title">New Post</div>
    <button class="modal-close" onclick="closePublish()">×</button>
  </div>
  <div class="type-picker" id="typePicker">
    <button data-type="writing" class="active">Text</button>
    <button data-type="photo">Photo</button>
    <button data-type="video">Video</button>
  </div>
  <div class="field">
    <label id="fileLabel" style="display:none">File</label>
    <div class="file-drop" id="fileDrop" onclick="document.getElementById('fileInput').click()">
      <div class="file-drop-icon" id="fileDropIcon">📷</div>
      <div class="file-drop-label">tap to choose</div>
      <div class="file-drop-name" id="fileDropName"></div>
    </div>
    <input type="file" id="fileInput" accept="image/*,video/*">
    <div class="file-preview" id="filePreview"></div>
    <div class="upload-progress" id="uploadProgress">
      <div class="progress-bar-bg"><div class="progress-bar" id="progressBar"></div></div>
      <div class="progress-label" id="progressLabel">uploading…</div>
    </div>
  </div>
  <div class="field">
    <label>Title</label>
    <input type="text" id="titleInput" placeholder="">
  </div>
  <div class="field">
    <label id="bodyLabel">Caption</label>
    <textarea id="bodyInput" placeholder=""></textarea>
  </div>
  <button class="submit-btn" id="publishBtn" onclick="submitPublish()">Post</button>
</div>

<!-- In-feed sponsor ad: shows the current creator's pre-roll ad between posts -->
<div id="feedAdOverlay" style="display:none;position:fixed;inset:0;z-index:90;background:#000">
  <video disableremoteplayback x-webkit-airplay="deny" id="feedAdVideo" playsinline style="position:absolute;inset:0;width:100%;height:100%;object-fit:contain;background:#000"></video>
  <div style="position:absolute;top:max(env(safe-area-inset-top),16px);left:16px;background:rgba(0,0,0,0.5);border-radius:6px;padding:4px 8px;font-size:11px;letter-spacing:0.08em;text-transform:uppercase;color:rgba(255,255,255,0.85)">Sponsored</div>
  <div id="feedAdCountdown" style="position:absolute;top:max(env(safe-area-inset-top),16px);right:16px;background:rgba(0,0,0,0.6);border-radius:16px;padding:6px 12px;font-size:13px;font-weight:600;color:#fff"></div>
  <button id="feedAdSkip" onclick="skipFeedAd()" style="display:none;position:absolute;top:max(env(safe-area-inset-top),16px);right:16px;background:rgba(0,0,0,0.6);border:none;border-radius:16px;padding:6px 14px;font-size:13px;font-weight:600;color:#fff">Skip ›</button>
  <div id="feedAdSponsor" onclick="onFeedAdClick()" style="display:none;position:absolute;bottom:max(env(safe-area-inset-bottom),20px);left:16px;right:16px;background:rgba(0,0,0,0.55);border-radius:12px;padding:12px 16px;color:#fff;font-size:14px;cursor:pointer"></div>
  <button onclick="muteFeedAd()" style="position:absolute;bottom:calc(max(env(safe-area-inset-bottom),20px) + 64px);right:16px;background:none;border:none;color:rgba(255,255,255,0.55);font-size:12px;text-decoration:underline">Hide ads like this</button>
</div>

<div class="toast" id="toast"></div>

<script>
// ── STATE ─────────────────────────────────────────────────────
const SELF_SUBDOMAIN = '${escapeHtml(subdomain)}';
const SELF_KEY       = '${escapeHtml(identity.publicKey.slice(0, 8))}';
const SELF_PUBKEY    = '${escapeHtml(identity.publicKey)}';
const NETWORK_ROOT_HOST = '${escapeHtml(NETWORK_ROOT_HOST)}';
const PERMALINK_POST = '${escapeHtml(s.postId || '')}'; // set when served from /p/<id> — boot lands on that post
let isMember = false;

const nodeGraph = [{
  subdomain: SELF_SUBDOMAIN,
  url: '',
  feed: null, postIndex: 0,
  loaded: false, loading: false,
}];
let nodeIndex  = 0;
let currentTab = 'foryou';

let token    = localStorage.getItem('adminToken') || '';
let isCreator = false;
let isHost    = false; // true only when unlocked with the host master token (shows creator-management UI)

let selectedType             = 'writing';
let selectedFile             = null;
let uploadedMediaUrl         = null;
let uploadedMediaContentType = null;

// ── LIKES (local) ─────────────────────────────────────────────
function getLikes() { try { return JSON.parse(localStorage.getItem('likes')||'{}'); } catch(e) { return {}; } }
function saveLikes(l) { localStorage.setItem('likes', JSON.stringify(l)); }
function isLiked(id) { return !!getLikes()[id]; }
function toggleLike(id) {
  const l = getLikes(), was = !!l[id];
  was ? delete l[id] : (l[id] = 1);
  saveLikes(l); return !was;
}
function fmtCount(n) {
  return n >= 1e6 ? (n/1e6).toFixed(1).replace('.0','')+'M'
       : n >= 1e3 ? (n/1e3).toFixed(1).replace('.0','')+'K'
       : String(n);
}

// ── SEEN HISTORY (local) — returning viewers land on unseen posts, so content feels new ──
function loadSeen() { try { return new Set(JSON.parse(localStorage.getItem('seen') || '[]')); } catch(e) { return new Set(); } }
let seenSet = loadSeen();
function markSeen(id) {
  if (!id || seenSet.has(id)) return;
  seenSet.add(id);
  let arr = [...seenSet];
  if (arr.length > 2000) { arr = arr.slice(arr.length - 2000); seenSet = new Set(arr); } // cap growth
  try { localStorage.setItem('seen', JSON.stringify(arr)); } catch(e) {}
}
function firstUnseenIndex(feed) {
  if (!Array.isArray(feed) || !feed.length) return 0;
  const i = feed.findIndex(it => it && it.id && !seenSet.has(it.id));
  return i === -1 ? 0 : i;
}
// Does this creator have anything the viewer hasn't watched? Drives skip-on-swipe.
function hasUnseen(node) {
  if (!node.loaded) return true;                      // not loaded yet — can't call it drained
  if (!node.feed || !node.feed.length) return false;  // unreachable or empty
  if (node.feed.some(it => it && it.id && !seenSet.has(it.id))) return true;
  return node.feed.length < (node.feedTotal || 0);    // deeper pages may be unwatched
}
// Where to land when arriving at a creator: stay put if the resume post is still fresh,
// otherwise jump to their first unseen post; fully-drained keeps the resume position.
function landingIndex(t) {
  if (!t.feed || !t.feed.length) return t.postIndex || 0;
  const cur = t.feed[t.postIndex];
  if (cur && cur.id && !seenSet.has(cur.id)) return t.postIndex;
  const fu = t.feed.findIndex(it => it && it.id && !seenSet.has(it.id));
  return fu >= 0 ? fu : (t.postIndex || 0);
}

// ── AVATAR ────────────────────────────────────────────────────
function avatarGrad(sub) {
  let h = 0; for (const c of sub) h = Math.imul(31,h) + c.charCodeAt(0)|0;
  const hue = Math.abs(h) % 360;
  return \`linear-gradient(135deg,hsl(\${hue},65%,52%),hsl(\${(hue+55)%360},65%,42%))\`;
}

// ── WRITING BACKGROUND ────────────────────────────────────────
function writingBg(id) {
  let h = 0; for (const c of id) h = Math.imul(31,h) + c.charCodeAt(0)|0;
  const bgs = [
    'linear-gradient(160deg,#0d0d0d,#161616,#0a0a1a)',
    'linear-gradient(160deg,#0d0000,#1a0005,#2d000f)',
    'linear-gradient(160deg,#000d00,#001a05,#00290a)',
    'linear-gradient(160deg,#08000d,#120019,#1e0033)',
    'linear-gradient(160deg,#0d0800,#1a1000,#2d1c00)',
  ];
  return bgs[Math.abs(h) % bgs.length];
}

// ── SERVICE WORKER ────────────────────────────────────────────
if ('serviceWorker' in navigator) navigator.serviceWorker.register('/sw.js').catch(()=>{});

// ── HELPERS ───────────────────────────────────────────────────
function esc(s) {
  return String(s).replace(/[&<>"']/g, c =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
let toastTimer;
function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg; t.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove('show'), 2200);
}
// Some call sites use showToast(); keep them working.
const showToast = toast;

// ── NODE LOADING ──────────────────────────────────────────────
const FEED_PAGE = 30; // posts per fetch — big accounts page in instead of shipping everything

async function loadNode(node) {
  if (node.loaded || node.loading) return;
  node.loading = true;
  try {
    const res = await fetch(node.url + '/.well-known/feed.json?limit=' + FEED_PAGE);
    const fd = await res.json();
    node.feed = fd.items || [];
    node.feedTotal = fd.total ?? node.feed.length;
    node.displayName = fd.displayName || '';
    node.avatarUrl = fd.avatarUrl ? (fd.avatarUrl.startsWith('http') ? fd.avatarUrl : (node.url || '') + fd.avatarUrl) : null;
    // hunt deeper pages while everything loaded is already seen — otherwise a fully-seen
    // first page strands the viewer on post 0 forever ("same videos every session")
    while (node.feed.length < (node.feedTotal || 0)
           && node.feed.every(it => !it || !it.id || seenSet.has(it.id))) {
      const more = await fetch(node.url + '/.well-known/feed.json?limit=' + FEED_PAGE + '&offset=' + node.feed.length).then(r => r.json());
      const items = more.items || [];
      if (!items.length) break;
      items.forEach(it => node.feed.push(it));
      node.feedTotal = more.total ?? node.feedTotal;
    }
    node.postIndex = firstUnseenIndex(node.feed); // land on first unseen post (runs once — loadNode guards on loaded)
  } catch(e) { node.feed = null; }
  node.loaded = true; node.loading = false;
  // The viewer may be parked on this card's spinner (swiped faster than the prefetch) —
  // without this the loading card sat there until the next manual swipe re-rendered.
  if (nodeGraph[nodeIndex] === node) renderCurrent();
  // Fetch live status separately — never blocks feed display
  fetch(node.url + '/live/status.json')
    .then(r => r.ok ? r.json() : null)
    .then(s => { if (s?.active) { node.liveStatus = s; renderCurrent(); } })
    .catch(() => {});
}

// The network IS the root's signed member registry. Every node builds the same
// graph from it, fetched directly from the baked-in root over HTTPS (TLS-authenticated).
async function initNetwork() {
  try {
    const reg = await fetch('https://' + NETWORK_ROOT_HOST + '/.well-known/registry.json').then(r => r.json());
    const members = reg.members || [];
    isMember = members.some(m => m.pubkey === SELF_PUBKEY);
    const blocked = getBlocked();
    for (const m of members) {
      if (m.subdomain === SELF_SUBDOMAIN) continue;
      if (blocked.includes(m.subdomain)) continue;
      if (nodeGraph.find(n => n.subdomain === m.subdomain)) continue;
      nodeGraph.push({ subdomain: m.subdomain, url: 'https://' + m.subdomain,
        feed: null, postIndex: 0, loaded: false, loading: false });
    }
  } catch(e) {}
  rankFeed(); // initial order: affinity + jitter (feeds/live not loaded yet — re-rank via the knobs)
  updateIndicators();
  refreshJoinButton();
}

function refreshJoinButton() {
  const btn = document.getElementById('joinNetworkBtn');
  if (btn) btn.style.display = (isCreator && !isMember) ? 'block' : 'none';
}

async function requestJoin() {
  try {
    const r = await fetch('/admin/request-join', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token } }).then(r => r.json());
    if (r.alreadyMember) { isMember = true; refreshJoinButton(); showToast('Already in the network'); }
    else if (r.ok) showToast('Join request sent to the network');
    else showToast(r.error || 'Request failed');
  } catch(e) { showToast('Request failed'); }
}

// ── FEED ALGORITHM v0 — runs on-device, data stays in localStorage, user-tunable ──
let _dwellKey = null, _dwellStart = 0;
function algoPrefs() { try { return JSON.parse(localStorage.getItem('algoPrefs')) || {}; } catch(e) { return {}; } }
function setAlgoPref(k, v) { const p = algoPrefs(); p[k] = v; localStorage.setItem('algoPrefs', JSON.stringify(p)); }
function affinityMap() { try { return JSON.parse(localStorage.getItem('affinity')) || {}; } catch(e) { return {}; } }
// dwell = seconds you actually spent on a creator's posts (capped per view, decays as it grows)
function recordDwell() {
  if (!_dwellKey || !_dwellStart) return;
  const sec = Math.min((Date.now() - _dwellStart) / 1000, 60);
  _dwellStart = 0;
  if (sec < 0.5) return; // instant skips don't count
  const a = affinityMap();
  a[_dwellKey] = Math.min((a[_dwellKey] || 0) + sec, 600);
  localStorage.setItem('affinity', JSON.stringify(a));
}
function startDwell(sub) { recordDwell(); _dwellKey = sub; _dwellStart = Date.now(); }
function scoreNode(n) {
  const p = algoPrefs();
  const aff = Math.log1p(affinityMap()[n.subdomain] || 0) / Math.log1p(600); // 0..1
  const newest = n.feed && n.feed[0] && Date.parse(n.feed[0].createdAt) || 0;
  const rec = newest ? Math.max(0, 1 - (Date.now() - newest) / (14 * 86400000)) : 0.3; // unknown = neutral
  const live = n.liveStatus && n.liveStatus.active ? 2 : 0; // live jumps the queue
  return live + (p.taste ?? 0.5) * aff + (p.recent ?? 0.5) * rec + (p.fresh ?? 0.3) * Math.random();
}
// Re-orders everything after slot 0 (the node you arrived on). Keeps your current card stable.
function rankFeed() {
  if (nodeGraph.length < 3) return;
  const cur = nodeGraph[nodeIndex];
  const head = nodeGraph[0];
  const rest = nodeGraph.slice(1);
  rest.sort((x, y) => scoreNode(y) - scoreNode(x));
  nodeGraph.splice(0, nodeGraph.length, head, ...rest);
  nodeIndex = Math.max(0, nodeGraph.indexOf(cur));
}
function openAlgo() {
  const p = algoPrefs();
  document.getElementById('algoTaste').value = Math.round((p.taste ?? 0.5) * 100);
  document.getElementById('algoRecent').value = Math.round((p.recent ?? 0.5) * 100);
  document.getElementById('algoFresh').value = Math.round((p.fresh ?? 0.3) * 100);
  document.getElementById('algoModal').classList.add('show');
}
function closeAlgo() { document.getElementById('algoModal').classList.remove('show'); }
function applyAlgo() {
  setAlgoPref('taste', document.getElementById('algoTaste').value / 100);
  setAlgoPref('recent', document.getElementById('algoRecent').value / 100);
  setAlgoPref('fresh', document.getElementById('algoFresh').value / 100);
  rankFeed(); updateIndicators(); closeAlgo(); toast('Feed re-ranked');
}
function resetAffinity() { localStorage.removeItem('affinity'); rankFeed(); updateIndicators(); toast('Taste data cleared'); }

function preloadAdjacent() {
  [-1,1].forEach(d => { const i = nodeIndex+d; if (i>=0&&i<nodeGraph.length) loadNode(nodeGraph[i]); });
}

// Fetch the next page of a node's feed when the viewer is a few posts from the end.
async function loadMorePosts(node) {
  if (!node || node.loadingMore || !node.loaded || !node.feed) return;
  if (node.feedTotal == null || node.feed.length >= node.feedTotal) return;
  node.loadingMore = true;
  try {
    const fd = await fetch(node.url + '/.well-known/feed.json?limit=' + FEED_PAGE + '&offset=' + node.feed.length).then(r => r.json());
    (fd.items || []).forEach(it => node.feed.push(it));
    node.feedTotal = fd.total ?? node.feedTotal;
  } catch(e) {}
  node.loadingMore = false;
}

// ── MEDIA PREFETCH — load the NEXT thing while the current one plays ──
// A detached <video preload=auto>/<img> pulls the head of the file into the browser's
// media cache (and warms our edge cache), so the swipe lands on already-loading media.
const _prefetched = new Set();
function prefetchMedia(rawUrl, base, type) {
  if (!rawUrl) return;
  const u = rawUrl.startsWith('http') ? rawUrl : base + rawUrl;
  if (_prefetched.has(u)) return;
  _prefetched.add(u);
  if (_prefetched.size > 80) _prefetched.clear();
  if (type === 'photo') { const i = new Image(); i.src = u; return; }
  if (type !== 'video') return;
  const v = document.createElement('video');
  v.preload = 'auto'; v.muted = true; v.src = u;
  // stop pulling after the head is buffered — don't burn the viewer's data on full files
  setTimeout(() => { try { v.removeAttribute('src'); v.load(); } catch(e) {} }, 12000);
}
function prefetchUpcoming() {
  const node = nodeGraph[nodeIndex];
  if (node?.feed) {
    const nxt = node.feed[node.postIndex + 1];
    if (nxt) prefetchMedia(nxt.mediaUrl, postBase(node), nxt.type);
  }
  [-1, 1].forEach(d => {
    const n2 = nodeGraph[nodeIndex + d];
    const it = n2?.feed?.[n2.postIndex];
    if (it) prefetchMedia(it.mediaUrl, postBase(n2), it.type);
  });
}

// ── LIVE STREAMING ────────────────────────────────────────────
let liveBroadcaster = null;
let liveViewer = null;
let liveChatInterval = null;
let liveStatusInterval = null;
let liveSocket = null;
let prerollActive = false; // true while a pre-roll ad is playing — keeps the live stream muted
let _pendingLiveRerender = false; // live status flipped while the live overlay was open — re-render on close

// LiveSocket — realtime chat + viewer count + status via the node's LiveRoom DO.
// Replaces 2s chat polling. Auto-reconnects. Works cross-node (wss to that node).
class LiveSocket {
  constructor(subdomain, role, opts) {
    this.opts = opts || {};
    this.role = role;
    this.host = subdomain === SELF_SUBDOMAIN ? location.host : subdomain;
    this.chat = [];
    this.closed = false;
    this.connect();
  }
  connect() {
    if (this.closed) return;
    // URL built per attempt so reconnects carry the CURRENT display name
    const wsScheme = location.protocol === 'https:' ? 'wss://' : 'ws://';
    const name = displayName() || 'viewer';
    const url = wsScheme + this.host + '/live/ws?role=' + this.role + '&name=' + encodeURIComponent(name) + '&vid=' + encodeURIComponent(getViewerId());
    try { this.ws = new WebSocket(url); } catch(e) { return; }
    this.ws.onmessage = e => this.onMsg(e);
    this.ws.onclose = () => { if (!this.closed) setTimeout(() => this.connect(), 2000); };
    this.ws.onerror = () => {};
  }
  rename(name) {
    if (this.ws && this.ws.readyState === 1) { try { this.ws.send(JSON.stringify({ t: 'name', name })); } catch(e) {} }
  }
  onMsg(e) {
    let d; try { d = JSON.parse(e.data); } catch { return; }
    if (d.t === 'init') {
      this.chat = d.chat || []; this.renderChat(); this.setViewers(d.viewers);
      if (d.status?.active) this.opts.onLive?.(d.status);
      this.opts.onOverlay?.(d.overlay || null);
    } else if (d.t === 'chat') {
      this.chat.push(d.msg); if (this.chat.length > 100) this.chat.shift(); this.renderChat();
    } else if (d.t === 'viewers') {
      this.setViewers(d.viewers);
    } else if (d.t === 'live') {
      this.opts.onLive?.(d.status);
    } else if (d.t === 'overlay') {
      this.opts.onOverlay?.(d.overlay || null);
    } else if (d.t === 'ended') {
      this.opts.onOverlay?.(null);
      this.opts.onEnded?.();
    }
  }
  renderChat() {
    const box = this.opts.chatBoxId && document.getElementById(this.opts.chatBoxId);
    if (!box) return;
    // full history (DO retains last 100); stick to newest unless the user scrolled up
    const stick = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
    box.innerHTML = this.chat.map(m => {
      // broadcaster can tap a message to mute its sender
      const mod = this.opts.canMod && m.sid
        ? ' data-sid="' + esc(m.sid) + '" data-name="' + esc(m.name) + '" onclick="modMuteTap(this)" style="cursor:pointer"'
        : '';
      const who = commenterInfo(m.name);
      return '<div class="live-chat-msg"' + mod + '><strong>' + esc(who ? who.display : m.name) + '</strong> ' + esc(m.text) + '</div>';
    }).join('');
    if (stick) box.scrollTop = box.scrollHeight;
  }
  setViewers(n) {
    const el = this.opts.viewerCountId && document.getElementById(this.opts.viewerCountId);
    if (el && typeof n === 'number') el.textContent = n;
  }
  send(text, name) {
    if (this.ws && this.ws.readyState === 1) { this.ws.send(JSON.stringify({ t: 'chat', text, name })); return true; }
    return false;
  }
  close() { this.closed = true; if (this.ws) { try { this.ws.close(); } catch {} } }
}

function pauseFeedVideos() { document.querySelectorAll('.card video').forEach(v => v.pause()); }
// true while something fullscreen covers the feed — card videos must never (re)start underneath it
function feedCovered() {
  if (document.getElementById('liveModal').style.display === 'flex') return true;
  if (typeof _feedAdShowing !== 'undefined' && _feedAdShowing) return true;
  return false;
}
function resumeFeedVideos() {
  // was '.card.active video' — a class no card ever had, so closing an overlay never resumed playback
  if (feedCovered()) return;
  activePanel().querySelectorAll('.card video').forEach(v => v.play().catch(()=>{}));
}

function onLiveTap() {
  const node = nodeGraph[nodeIndex];
  const isBroadcaster = isCreator && node?.subdomain === SELF_SUBDOMAIN;
  const modal = document.getElementById('liveModal');
  pauseFeedVideos();
  modal.style.display = 'flex';

  const preview   = document.getElementById('livePreview');
  const viewVideo = document.getElementById('liveViewVideo');
  const viewStatus = document.getElementById('liveViewStatus');
  const bcPanel   = document.getElementById('liveBroadcasterPanel');

  if (isBroadcaster) {
    document.getElementById('liveModalTitle').textContent = 'LIVE';
    preview.style.display   = 'block';
    viewVideo.style.display = 'none';
    viewStatus.style.display = 'none';
    bcPanel.style.display   = 'block';
    // 720p30 ideal (not exact — older devices fall back); default was often VGA
    navigator.mediaDevices.getUserMedia({
      video: { width: { ideal: 1280 }, height: { ideal: 720 }, frameRate: { ideal: 30 } },
      audio: true,
    }).then(stream => {
      preview.srcObject = stream;
      if (!liveBroadcaster) liveBroadcaster = new LiveBroadcaster(stream);
    }).catch(e => setLiveStatus('Camera error: ' + e.message));
  } else {
    document.getElementById('liveModalTitle').textContent = 'LIVE';
    preview.style.display   = 'none';
    viewVideo.style.display = 'block';
    viewStatus.style.display = 'block';
    bcPanel.style.display   = 'none';
    runPrerollThenView(node.subdomain);
  }

  // Realtime chat + viewer count over WebSocket (replaces polling).
  if (liveSocket) { liveSocket.close(); liveSocket = null; }
  liveSocket = new LiveSocket(node.subdomain, isBroadcaster ? 'broadcaster' : 'viewer', {
    chatBoxId: 'liveChatBox',
    viewerCountId: 'liveViewerCount',
    canMod: isBroadcaster,
    onEnded: () => { if (!isBroadcaster) { setLiveStatus('Stream ended'); } },
    // overlay images are origin-relative on the STREAMING node — prefix for remote viewers
    onOverlay: ov => renderLiveOverlay(ov, node.subdomain === SELF_SUBDOMAIN ? '' : 'https://' + node.subdomain),
  });
  setOverlayEditable(isBroadcaster); // broadcaster can drag/pinch the overlay in place
}

// ── LIVE OVERLAY (broadcaster text/image over the stream) ────
// Placement = {x,y} as screen fractions (element centered there) + scale s. The
// broadcaster drags/pinches the elements on their own preview; positions ride along
// with the overlay broadcast so viewers see the same proportional placement.
const OV_DEFAULTS = { img: { x: 0.82, y: 0.20, s: 1 }, txt: { x: 0.5, y: 0.60, s: 1 } };
let _ovCurrent = null; // latest overlay state (broadcaster gestures mutate it in place)
let _ovBase = '';      // media origin of the streaming node (for relative imageUrls)
function renderLiveOverlay(ov, base) {
  _ovCurrent = ov;
  if (base !== undefined) _ovBase = base || '';
  const layer = document.getElementById('liveOverlayLayer');
  const img = document.getElementById('liveOverlayImg');
  const txt = document.getElementById('liveOverlayText');
  const has = ov && (ov.text || ov.imageUrl);
  layer.style.display = has ? 'block' : 'none';
  if (!has) { img.style.display = 'none'; img.removeAttribute('src'); txt.style.display = 'none'; return; }
  if (ov.imageUrl) {
    const want = ov.imageUrl.startsWith('http') ? ov.imageUrl : _ovBase + ov.imageUrl;
    if (img.dataset.srcSet !== want) { img.dataset.srcSet = want; img.src = want; }
    const p = ov.img || OV_DEFAULTS.img;
    img.style.left  = (p.x * 100) + '%';
    img.style.top   = (p.y * 100) + '%';
    img.style.width = (30 * p.s) + 'vw';
    img.style.display = 'block';
  } else { img.style.display = 'none'; img.removeAttribute('src'); delete img.dataset.srcSet; }
  if (ov.text) {
    const p = ov.txt || OV_DEFAULTS.txt;
    txt.textContent = ov.text;
    txt.style.left = (p.x * 100) + '%';
    txt.style.top  = (p.y * 100) + '%';
    txt.style.fontSize = (19 * p.s) + 'px';
    // a link turns the text into a button viewers can tap (broadcaster keeps drag instead)
    if (ov.linkUrl) {
      txt.style.background = '#FE2C55'; txt.style.padding = '10px 20px'; txt.style.borderRadius = '24px';
      txt.style.textShadow = 'none'; txt.style.boxShadow = '0 2px 12px rgba(0,0,0,0.45)';
      if (!txt.classList.contains('ov-edit')) txt.style.pointerEvents = 'auto';
      txt.onclick = () => { if (!txt.classList.contains('ov-edit')) window.open(ov.linkUrl, '_blank', 'noopener'); };
    } else {
      txt.style.background = 'none'; txt.style.padding = '0'; txt.style.borderRadius = '0';
      txt.style.textShadow = '0 1px 5px rgba(0,0,0,0.85)'; txt.style.boxShadow = 'none';
      if (!txt.classList.contains('ov-edit')) txt.style.pointerEvents = 'none';
      txt.onclick = null;
    }
    txt.style.display = 'block';
  } else { txt.style.display = 'none'; }
}
// Broadcaster edit mode: elements become grabbable; layer rises above chat/panel so
// a badge parked over them can still be picked back up.
function setOverlayEditable(on) {
  document.getElementById('liveOverlayLayer').style.zIndex = on ? '25' : '6';
  for (const id of ['liveOverlayImg', 'liveOverlayText']) {
    const el = document.getElementById(id);
    el.style.pointerEvents = on ? 'auto' : 'none';
    el.classList.toggle('ov-edit', on);
  }
}
function ovPlace(key) {
  if (!_ovCurrent) return null;
  if (!_ovCurrent[key]) _ovCurrent[key] = { x: OV_DEFAULTS[key].x, y: OV_DEFAULTS[key].y, s: OV_DEFAULTS[key].s };
  return _ovCurrent[key];
}
let _ovPersistT = null;
function ovPersistSoon() { // one POST per settled gesture, not per move event
  clearTimeout(_ovPersistT);
  _ovPersistT = setTimeout(() => { if (_ovCurrent) setOverlayOnServer(_ovCurrent).catch(() => {}); }, 350);
}
function ovDist(t) { return Math.hypot(t[0].clientX - t[1].clientX, t[0].clientY - t[1].clientY) || 1; }
function ovAttachGestures(id, key) {
  const el = document.getElementById(id);
  const clamp = (v, lo, hi) => Math.min(Math.max(v, lo), hi);
  let drag = null, pinch = null;
  el.addEventListener('touchstart', e => {
    if (!el.classList.contains('ov-edit')) return; // viewers tap the link, only the host drags
    const p = ovPlace(key); if (!p) return;
    e.preventDefault(); e.stopPropagation();
    if (e.touches.length >= 2) { pinch = { d0: ovDist(e.touches), s0: p.s }; drag = null; }
    else drag = { x0: e.touches[0].clientX, y0: e.touches[0].clientY, px: p.x, py: p.y };
  }, { passive: false });
  el.addEventListener('touchmove', e => {
    const p = ovPlace(key); if (!p) return;
    e.preventDefault(); e.stopPropagation();
    if (pinch && e.touches.length >= 2) {
      p.s = clamp(pinch.s0 * (ovDist(e.touches) / pinch.d0), 0.2, 4);
    } else if (drag && e.touches.length === 1) {
      p.x = clamp(drag.px + (e.touches[0].clientX - drag.x0) / window.innerWidth, 0.03, 0.97);
      p.y = clamp(drag.py + (e.touches[0].clientY - drag.y0) / window.innerHeight, 0.03, 0.97);
    }
    renderLiveOverlay(_ovCurrent);
  }, { passive: false });
  el.addEventListener('touchend', e => {
    if (e.touches.length === 0) { if (drag || pinch) ovPersistSoon(); drag = pinch = null; }
    else if (e.touches.length === 1 && pinch) {
      // pinch released down to one finger — hand off into a drag without a lift
      pinch = null;
      const p = ovPlace(key);
      drag = { x0: e.touches[0].clientX, y0: e.touches[0].clientY, px: p.x, py: p.y };
    }
  });
  // desktop conveniences: mouse drag + wheel to resize
  el.addEventListener('mousedown', e => {
    if (!el.classList.contains('ov-edit')) return;
    const p = ovPlace(key); if (!p) return;
    e.preventDefault();
    const m = { x0: e.clientX, y0: e.clientY, px: p.x, py: p.y };
    const mv = ev => {
      p.x = clamp(m.px + (ev.clientX - m.x0) / window.innerWidth, 0.03, 0.97);
      p.y = clamp(m.py + (ev.clientY - m.y0) / window.innerHeight, 0.03, 0.97);
      renderLiveOverlay(_ovCurrent);
    };
    const up = () => { document.removeEventListener('mousemove', mv); document.removeEventListener('mouseup', up); ovPersistSoon(); };
    document.addEventListener('mousemove', mv); document.addEventListener('mouseup', up);
  });
  el.addEventListener('wheel', e => {
    if (!el.classList.contains('ov-edit')) return;
    const p = ovPlace(key); if (!p) return;
    e.preventDefault();
    p.s = clamp(p.s * (e.deltaY < 0 ? 1.08 : 0.92), 0.2, 4);
    renderLiveOverlay(_ovCurrent); ovPersistSoon();
  }, { passive: false });
}
ovAttachGestures('liveOverlayImg', 'img');
ovAttachGestures('liveOverlayText', 'txt');
let _ovImageUrl = '';
function openOverlaySheet() {
  // editing a live overlay: seed the sheet from current state so re-applying keeps the image
  if (_ovCurrent) {
    document.getElementById('ovText').value = _ovCurrent.text || '';
    document.getElementById('ovLink').value = _ovCurrent.linkUrl || '';
    if (_ovCurrent.imageUrl && !_ovImageUrl) {
      _ovImageUrl = _ovCurrent.imageUrl;
      const u = _ovCurrent.imageUrl.startsWith('http') ? _ovCurrent.imageUrl : _ovBase + _ovCurrent.imageUrl;
      document.getElementById('ovPreview').innerHTML = \`<img src="\${esc(u)}" style="width:100%;height:100%;object-fit:cover">\`;
    }
  }
  document.getElementById('overlayModal').classList.add('show');
}
function closeOverlaySheet() {
  document.getElementById('overlayModal').classList.remove('show');
}
async function onOverlayPick(input) {
  const file = input.files[0];
  if (!file) return;
  const btn = document.getElementById('ovApplyBtn');
  btn.disabled = true; btn.textContent = 'Uploading…';
  try {
    const fd = new FormData(); fd.append('file', file);
    const res = await fetch('/admin/upload', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token }, body: fd });
    const data = await res.json();
    _ovImageUrl = data.url;
    document.getElementById('ovPreview').innerHTML = \`<img src="\${esc(data.url)}" style="width:100%;height:100%;object-fit:cover">\`;
  } catch(e) { toast('Upload failed'); }
  btn.disabled = false; btn.textContent = 'Show overlay';
}
async function setOverlayOnServer(ov) {
  const r = await fetch('/admin/live/overlay', {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ text: ov.text || '', imageUrl: ov.imageUrl || '', linkUrl: ov.linkUrl || '', txt: ov.txt || null, img: ov.img || null }),
  });
  if (!r.ok) throw new Error(r.status);
}
async function applyOverlay() {
  const text = document.getElementById('ovText').value.trim();
  if (!text && !_ovImageUrl) { toast('Add text or an image first'); return; }
  // keep existing placements when re-applying (editing text mid-stream shouldn't reset layout)
  const link = document.getElementById('ovLink').value.trim();
  if (link && !link.startsWith('https://')) { toast('Link must start with https://'); return; }
  const ov = { text, imageUrl: _ovImageUrl, linkUrl: link, txt: _ovCurrent?.txt || null, img: _ovCurrent?.img || null };
  try {
    await setOverlayOnServer(ov);
    toast('Overlay is live — drag to move, pinch to resize');
    closeOverlaySheet();
  } catch(e) { toast('Could not set overlay'); }
}
async function clearOverlay() {
  _ovImageUrl = '';
  document.getElementById('ovText').value = '';
  document.getElementById('ovLink').value = '';
  document.getElementById('ovPreview').innerHTML = '';
  try { await setOverlayOnServer({ text: '', imageUrl: '' }); toast('Overlay cleared'); } catch(e) {}
  closeOverlaySheet();
}

function closeLiveModal() {
  // If broadcaster is live, end the stream first
  const endBtn = document.getElementById('liveBtnEnd');
  if (liveBroadcaster && endBtn && endBtn.style.display !== 'none') {
    liveEnd();
  }
  document.getElementById('liveModal').style.display = 'none';
  document.getElementById('liveUnmuteBtn').style.display = 'none';
  // tear down pre-roll if a viewer bailed mid-ad
  prerollActive = false;
  const _ov = document.getElementById('prerollOverlay');
  if (_ov) _ov.style.display = 'none';
  const _pv = document.getElementById('prerollVideo');
  if (_pv) { try { _pv.pause(); } catch {} _pv.src = ''; }
  const preview = document.getElementById('livePreview');
  if (preview.srcObject) { preview.srcObject.getTracks().forEach(t => t.stop()); preview.srcObject = null; }
  const viewVid = document.getElementById('liveViewVideo');
  viewVid.srcObject = null;
  renderLiveOverlay(null);
  setOverlayEditable(false);
  if (liveBroadcaster) { liveBroadcaster.cleanup(); liveBroadcaster = null; }
  if (liveViewer) { liveViewer.cleanup(); liveViewer = null; }
  if (liveSocket) { liveSocket.close(); liveSocket = null; }
  stopLiveChatPoll();
  // live state flipped while the overlay was up → rebuild the card; otherwise just resume playback
  if (_pendingLiveRerender) { _pendingLiveRerender = false; renderCurrent(); }
  else resumeFeedVideos();
}

function setLiveStatus(msg) {
  const el = document.getElementById('liveStatusMsg');
  if (el) el.textContent = msg;
}

async function liveStart() {
  if (!liveBroadcaster) return;
  document.getElementById('liveBtnStart').disabled = true;
  setLiveStatus('Starting…');
  try {
    await liveBroadcaster.start();
    document.getElementById('liveBtnStart').style.display = 'none';
    document.getElementById('liveBtnEnd').style.display = '';
    setLiveStatus('🔴 Live');
  } catch(e) {
    setLiveStatus('Error: ' + e.message);
    document.getElementById('liveBtnStart').disabled = false;
  }
}

async function liveEnd() {
  if (!liveBroadcaster) return;
  await liveBroadcaster.end();
  document.getElementById('liveBtnStart').style.display = '';
  document.getElementById('liveBtnEnd').style.display = 'none';
  document.getElementById('liveBtnStart').disabled = false;
  setLiveStatus('Stream ended');
}

async function liveSendChat() {
  const input = document.getElementById('liveChatInput');
  const text = input.value.trim();
  if (!text) return;
  const name = await ensureName();
  if (!name) return; // guest cancelled the name prompt
  input.value = '';
  // Prefer the live WebSocket; fall back to HTTP if it isn't open.
  if (liveSocket && liveSocket.send(text, name)) return;
  const node = nodeGraph[nodeIndex];
  const base = (node && node.subdomain !== SELF_SUBDOMAIN) ? 'https://' + node.subdomain : '';
  await fetch(base + '/live/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text, name, vid: getViewerId() }),
  }).catch(() => {});
}

function startLiveChatPoll(subdomain) {
  stopLiveChatPoll();
  const base = subdomain === SELF_SUBDOMAIN ? '' : 'https://' + subdomain;
  async function poll() {
    try {
      const r = await fetch(base + '/live/chat.json');
      if (r.ok) {
        const data = await r.json();
        const msgs = data.messages || data;
        const box = document.getElementById('liveChatBox');
        if (box) {
          box.innerHTML = msgs.map(m =>
            \`<div class="live-chat-msg"><strong>\${esc(m.name)}</strong> \${esc(m.text)}</div>\`
          ).join('');
          box.scrollTop = box.scrollHeight;
        }
      }
    } catch {}
  }
  poll();
  liveChatInterval = setInterval(poll, 2000);
}

function stopLiveChatPoll() {
  if (liveChatInterval) { clearInterval(liveChatInterval); liveChatInterval = null; }
}

let liveCardChatInterval = null;
function startLiveCardChatPoll(subdomain) {
  if (liveCardChatInterval) clearInterval(liveCardChatInterval);
  const base = subdomain === SELF_SUBDOMAIN ? '' : 'https://' + subdomain;
  async function poll() {
    try {
      const r = await fetch(base + '/live/chat.json');
      if (!r.ok) return;
      const data = await r.json();
      const msgs = data.messages || data;
      const box = document.getElementById('liveCardChat');
      if (box) {
        const stick = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
        box.innerHTML = msgs.map(m => {
          const who = commenterInfo(m.name);
          return \`<div class="live-card-chat-msg"><strong>\${esc(who ? who.display : m.name)}</strong> \${esc(m.text)}</div>\`;
        }).join('');
        if (stick) box.scrollTop = box.scrollHeight;
      }
    } catch {}
  }
  poll();
  liveCardChatInterval = setInterval(poll, 2000);
}

// LiveBroadcaster — publishes camera/mic via Cloudflare Calls
class LiveBroadcaster {
  constructor(stream) {
    this.stream = stream;
    this.pc = null;
    this.sessionId = null;
  }
  async start() {
    // 1. Create publisher session
    const sessRes = await fetch('/admin/live/start', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + token },
    });
    if (!sessRes.ok) throw new Error((await sessRes.json()).error || sessRes.statusText);
    const sess = await sessRes.json();
    this.sessionId = sess.sessionId;

    // 2. Create RTCPeerConnection and add tracks
    this.pc = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.cloudflare.com:3478' }] });
    this.stream.getTracks().forEach(t => this.pc.addTrack(t, this.stream));

    // 3. Create offer and set local description
    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);

    // 4. Build tracks array from transceivers (Cloudflare Calls format)
    const tracks = this.pc.getTransceivers().map(t => ({
      location: 'local',
      mid: t.mid,
      trackName: t.sender.track ? t.sender.track.kind + '-' + t.sender.track.id.slice(0, 8) : t.mid,
    }));

    // 5. Exchange SDP + tracks with Calls via worker
    const tracksRes = await fetch('/admin/live/tracks', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sessionId: this.sessionId,
        sessionDescription: { type: 'offer', sdp: offer.sdp },
        tracks,
      }),
    });
    if (!tracksRes.ok) throw new Error('Tracks failed: ' + await tracksRes.text());
    const tracksResp = await tracksRes.json();

    await this.pc.setRemoteDescription({ type: 'answer', sdp: tracksResp.sessionDescription.sdp });

    // 5b. Quality tuning — cap bitrate so a shaky uplink degrades resolution, not latency.
    // The 2.5 Mbps ceiling is plenty for 720p30 and bounds egress cost on fast connections.
    const vTrack = this.stream.getVideoTracks()[0];
    if (vTrack) { try { vTrack.contentHint = 'motion'; } catch(e) {} }
    for (const sender of this.pc.getSenders()) {
      if (!sender.track || sender.track.kind !== 'video') continue;
      try {
        const p = sender.getParameters();
        p.degradationPreference = 'maintain-framerate';
        if (!p.encodings || !p.encodings.length) p.encodings = [{}];
        p.encodings[0].maxBitrate = 2500000;
        await sender.setParameters(p);
      } catch(e) {}
    }

    // 6. Publish session (store trackNames in DO, set active=true)
    await fetch('/admin/live/publish', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId: this.sessionId, trackNames: tracks.map(t => t.trackName) }),
    });

    // 7. Heartbeat so the stream is marked dead if this tab closes/crashes.
    this.hb = setInterval(() => {
      fetch('/admin/live/heartbeat', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token },
      }).catch(() => {});
    }, 15000);
  }
  async end() {
    if (this.hb) { clearInterval(this.hb); this.hb = null; }
    await fetch('/admin/live/end', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + token },
    }).catch(() => {});
    this.cleanup();
  }
  cleanup() {
    if (this.hb) { clearInterval(this.hb); this.hb = null; }
    if (this.pc) { this.pc.close(); this.pc = null; }
    if (this.stream) { this.stream.getTracks().forEach(t => t.stop()); }
  }
}

// LiveViewer — subscribes to a remote node's live stream
class LiveViewer {
  constructor(subdomain) {
    this.base = 'https://' + subdomain;
    this.pc = null;
    this.sessionId = null;
  }
  async start(videoEl, statusEl) {
    // 1. Check status + get trackNames
    const status = await fetch(this.base + '/live/status.json').then(r => r.json());
    if (!status.active) { statusEl.textContent = 'Not live right now'; return; }
    const trackNames = status.trackNames || [];

    // 2. Create subscriber session
    const sessRes = await fetch(this.base + '/live/subscribe', { method: 'POST' });
    if (!sessRes.ok) throw new Error('Subscribe failed: ' + await sessRes.text());
    const sess = await sessRes.json();
    this.sessionId = sess.sessionId;

    // 3. Create RTCPeerConnection
    this.pc = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.cloudflare.com:3478' }] });
    const rxStream = new MediaStream();
    videoEl.srcObject = rxStream;
    let playStarted = false;
    this.pc.ontrack = e => {
      console.log('ontrack:', e.track.kind, e.track.readyState);
      rxStream.addTrack(e.track);
      if (!playStarted) {
        playStarted = true;
        if (prerollActive) {
          // a pre-roll ad is on top — keep the live stream silent until it ends
          videoEl.muted = true;
          videoEl.play().catch(() => {});
        } else {
          videoEl.muted = false;
          videoEl.play().catch(() => {
            videoEl.muted = true;
            videoEl.play().catch(()=>{});
            const btn = document.getElementById('liveUnmuteBtn');
            if (btn) btn.style.display = 'flex';
          });
        }
        if (statusEl) { statusEl.textContent = ''; statusEl.style.display = 'none'; }
      }
    };

    // 4. Subscribe to publisher tracks (must include publisher's sessionId)
    const tracks = trackNames.map(trackName => ({ location: 'remote', trackName, sessionId: status.publisherSessionId }));
    const tracksRes = await fetch(this.base + '/live/tracks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ subscriberSessionId: this.sessionId, tracks }),
    });
    if (!tracksRes.ok) throw new Error('Tracks failed: ' + await tracksRes.text());
    const tracksResp = await tracksRes.json();

    // 5. Set remote description (offer from Calls) and answer
    await this.pc.setRemoteDescription({ type: 'offer', sdp: tracksResp.sessionDescription.sdp });
    const answer = await this.pc.createAnswer();
    await this.pc.setLocalDescription(answer);

    // 6. Renegotiate with answer
    await fetch(this.base + '/live/renegotiate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ subscriberSessionId: this.sessionId, sessionDescription: { type: 'answer', sdp: answer.sdp } }),
    });
  }
  cleanup() {
    if (this.pc) { this.pc.close(); this.pc = null; }
  }
}

async function startLiveViewer(subdomain) {
  const videoEl  = document.getElementById('liveViewVideo');
  const statusEl = document.getElementById('liveViewStatus');
  if (liveViewer) { liveViewer.cleanup(); liveViewer = null; }
  liveViewer = new LiveViewer(subdomain);
  try {
    await liveViewer.start(videoEl, statusEl);
    if (statusEl) statusEl.style.display = 'none';
  } catch(e) {
    if (statusEl) statusEl.textContent = 'Error: ' + e.message;
  }
}

// ── PRE-ROLL SPONSOR AD (viewer) ──────────────────────────────
let _prerollClickUrl = '', _prerollBase = '';

function getViewerId() {
  let v = localStorage.getItem('vid');
  if (!v) { v = (crypto.randomUUID ? crypto.randomUUID() : String(Date.now()) + Math.random().toString(36).slice(2)); localStorage.setItem('vid', v); }
  return v;
}

async function runPrerollThenView(subdomain) {
  const base = subdomain === SELF_SUBDOMAIN ? '' : 'https://' + subdomain;
  // Only gate an actually-live stream.
  let status = { active: false };
  try { status = await fetch(base + '/live/status.json').then(r => r.json()); } catch(e) {}
  if (!status.active) { startLiveViewer(subdomain); return; }

  const vid = getViewerId();
  let ad = null;
  try {
    const d = await fetch(base + '/live/preroll?vid=' + encodeURIComponent(vid)).then(r => r.json());
    if (d.show && !getAdMutes().includes((d.ad && d.ad.category) || 'general')) ad = d.ad;
  } catch(e) {}

  // Connect the stream in the background DURING the ad, so it's ready at countdown 0.
  prerollActive = !!ad;
  if (ad) { const lv = document.getElementById('liveViewVideo'); if (lv) lv.muted = true; }
  startLiveViewer(subdomain);
  if (ad) { reportImpression(subdomain, 'live'); await playPreroll(ad, base, vid); }
}

function playPreroll(ad, base, vid) {
  return new Promise(resolve => {
    const overlay = document.getElementById('prerollOverlay');
    const video   = document.getElementById('prerollVideo');
    const cd      = document.getElementById('prerollCountdown');
    const sp      = document.getElementById('prerollSponsor');
    let dur       = ad.durationSec || 15;
    _prerollClickUrl = ad.clickUrl || '';
    _prerollBase     = base;

    if (ad.sponsorName || ad.clickUrl) {
      sp.style.display = 'block';
      sp.innerHTML = (ad.sponsorName ? '<strong>' + esc(ad.sponsorName) + '</strong>' : '') + (ad.clickUrl ? ' &nbsp;Learn more ›' : '');
    } else { sp.style.display = 'none'; }

    overlay.style.display = 'block';
    video.src = ad.mediaUrl.startsWith('http') ? ad.mediaUrl : base + ad.mediaUrl;

    let done = false;
    const finish = () => {
      if (done) return; done = true;
      clearTimeout(guard);
      overlay.style.display = 'none';
      try { video.pause(); } catch {}
      video.src = '';
      // ad's over — hand audio back to the live stream
      prerollActive = false;
      const lv = document.getElementById('liveViewVideo');
      if (lv) {
        lv.muted = false;
        lv.play().catch(() => {
          lv.muted = true;
          lv.play().catch(() => {});
          const b = document.getElementById('liveUnmuteBtn');
          if (b) b.style.display = 'flex';
        });
      }
      fetch(base + '/live/preroll/seen', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ vid }),
      }).catch(() => {});
      resolve();
    };
    const tick = () => { cd.textContent = 'Stream starts in ' + Math.max(0, Math.ceil(dur - (video.currentTime || 0))) + '…'; };

    video.onloadedmetadata = () => {
      // count down from the clip's real length (capped at 15s) so it always hits 0
      if (isFinite(video.duration) && video.duration > 0) dur = Math.min(Math.ceil(video.duration), 15);
      tick();
    };
    video.ontimeupdate = tick;
    video.onended = finish;
    video.onerror = finish; // if the creative fails to load, don't trap the viewer
    const guard = setTimeout(finish, (dur + 4) * 1000); // safety net if 'ended' never fires
    // don't make the viewer stare at a black screen: if the creative hasn't started in 5s, skip to the stream
    const startGuard = setTimeout(() => { if ((video.currentTime || 0) === 0) finish(); }, 5000);
    video.onplaying = () => clearTimeout(startGuard);

    // We arrived here via the user's "Live" tap, so unmuted autoplay should be allowed;
    // fall back to muted if the browser still blocks it.
    video.muted = false;
    video.play().catch(() => { video.muted = true; video.play().catch(finish); });
  });
}

function onPrerollClick() {
  if (!_prerollClickUrl.startsWith('http://') && !_prerollClickUrl.startsWith('https://')) return;
  fetch(_prerollBase + '/live/preroll/click', { method: 'POST' }).catch(() => {});
  window.open(_prerollClickUrl, '_blank', 'noopener');
}

// ── IN-FEED SPONSOR ADS ───────────────────────────────────────
// Every AD_EVERY committed swipes, show the CURRENT creator's pre-roll ad as a skippable
// interstitial. Reuses the same config + DO impression counters + 8-min per-viewer grace
// window as the live pre-roll, so the creator's earnings meter covers both surfaces —
// the node being viewed serves (and earns from) its own ad.
const AD_EVERY = 10;
const AD_MIN_GAP_MS = 150000; // global cap: at most one interstitial per 2.5 min across ALL nodes
let _swipesSinceAd = 0, _feedAdShowing = false, _feedAdClickUrl = '', _feedAdBase = '', _feedAdCategory = '';

// Verified-impression beacon → the network root ("don't grade your own homework"):
// the viewed node's local meter pays creators; the root's deduped count faces advertisers.
function reportImpression(host, surface) {
  fetch('https://' + NETWORK_ROOT_HOST + '/measure/impression', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ host: host || SELF_SUBDOMAIN, vid: getViewerId(), surface }),
  }).catch(() => {});
}

// Viewer-side ad controls: muted categories live on-device, like seen-history.
function getAdMutes() { try { return JSON.parse(localStorage.getItem('adMutes') || '[]'); } catch(e) { return []; } }
function muteFeedAd() {
  const m = getAdMutes();
  if (_feedAdCategory && !m.includes(_feedAdCategory)) m.push(_feedAdCategory);
  localStorage.setItem('adMutes', JSON.stringify(m));
  toast("You'll see fewer ads like this");
  skipFeedAd();
}

function maybeShowFeedAd() {
  _swipesSinceAd++;
  if (_feedAdShowing || _swipesSinceAd < AD_EVERY) return;
  if (Date.now() - (+localStorage.getItem('lastAdAt') || 0) < AD_MIN_GAP_MS) return;
  const node = nodeGraph[nodeIndex];
  if (!node) return;
  const base = postBase(node);
  fetch(base + '/live/preroll?vid=' + encodeURIComponent(getViewerId()))
    .then(r => r.json())
    .then(d => {
      if (d.show && d.ad && d.ad.mediaUrl && !getAdMutes().includes(d.ad.category || 'general')) showFeedAd(d.ad, base, node.subdomain);
    })
    .catch(() => {});
}

function showFeedAd(ad, base, host) {
  _feedAdShowing = true; _swipesSinceAd = 0;
  _feedAdCategory = ad.category || 'general';
  localStorage.setItem('lastAdAt', String(Date.now()));
  _feedAdClickUrl = ad.clickUrl || ''; _feedAdBase = base;
  const ov = document.getElementById('feedAdOverlay');
  const video = document.getElementById('feedAdVideo');
  const sp = document.getElementById('feedAdSponsor');
  const cd = document.getElementById('feedAdCountdown');
  pauseFeedVideos();
  if (ad.sponsorName || ad.clickUrl) {
    sp.style.display = 'block';
    sp.innerHTML = (ad.sponsorName ? '<strong>' + esc(ad.sponsorName) + '</strong>' : '') + (ad.clickUrl ? ' &nbsp;Learn more ›' : '');
  } else sp.style.display = 'none';
  document.getElementById('feedAdSkip').style.display = 'none';
  cd.style.display = 'block'; cd.textContent = '5…';
  ov.style.display = 'block';
  video.src = ad.mediaUrl.startsWith('http') ? ad.mediaUrl : base + ad.mediaUrl;
  let elapsed = 0;
  video._timer = setInterval(() => {
    elapsed++;
    if (elapsed >= 5) {
      cd.style.display = 'none';
      document.getElementById('feedAdSkip').style.display = 'block';
      clearInterval(video._timer); video._timer = null;
    } else cd.textContent = (5 - elapsed) + '…';
  }, 1000);
  video.onended = skipFeedAd;
  video.onerror = skipFeedAd; // never trap the viewer behind a broken creative
  // a hung media fetch leaves play() pending forever — countdown over a black screen.
  // If nothing has rendered within 4s, bail out instead of pretending an ad ran.
  video._startGuard = setTimeout(() => { if ((video.currentTime || 0) === 0) skipFeedAd(); }, 4000);
  video._counted = false;
  video.onplaying = () => {
    if (video._startGuard) { clearTimeout(video._startGuard); video._startGuard = null; }
    if (video._counted) return;
    video._counted = true;
    // impression counts only when the creative actually renders (was: on display)
    fetch(base + '/live/preroll/seen', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ vid: getViewerId() }) }).catch(() => {});
    reportImpression(host, 'feed');
  };
  video.muted = false;
  video.play().catch(() => { video.muted = true; video.play().catch(skipFeedAd); });
}

function skipFeedAd() {
  if (!_feedAdShowing) return;
  _feedAdShowing = false;
  const video = document.getElementById('feedAdVideo');
  if (video._timer) { clearInterval(video._timer); video._timer = null; }
  if (video._startGuard) { clearTimeout(video._startGuard); video._startGuard = null; }
  try { video.pause(); } catch(e) {}
  video.src = '';
  document.getElementById('feedAdOverlay').style.display = 'none';
  resumeFeedVideos();
}

function onFeedAdClick() {
  if (!_feedAdClickUrl.startsWith('http://') && !_feedAdClickUrl.startsWith('https://')) return;
  fetch(_feedAdBase + '/live/preroll/click', { method: 'POST' }).catch(() => {});
  window.open(_feedAdClickUrl, '_blank', 'noopener');
}

// ── CARD RENDERING ────────────────────────────────────────────
function renderSidebar(node, item) {
  const letter   = ((node.displayName || node.subdomain)[0] || '?').toUpperCase();
  const grad     = avatarGrad(node.subdomain);
  const isSelf   = node.subdomain === SELF_SUBDOMAIN;
  const avInner  = node.avatarUrl
    ? \`<img src="\${esc(node.avatarUrl)}" alt="" style="width:100%;height:100%;object-fit:cover;border-radius:50%">\`
    : esc(letter);
  const liked    = item ? isLiked(item.id) : false;
  const likeClr  = liked ? 'filter:none;color:#FE2C55' : 'filter:grayscale(0.2)';
  const id       = item ? esc(item.id) : '';
  const sub      = esc(node.subdomain);

  return \`<div class="sidebar">
    <div class="sidebar-item">
      <div class="avatar-wrap" onclick="openProfile('\${sub}')" style="cursor:pointer">
        <div class="avatar-circle" style="background:\${grad};overflow:hidden">\${avInner}</div>
      </div>
    </div>
    <div class="sidebar-item">
      <button class="sidebar-btn" onclick="onLike('\${id}',this)">
        <span class="like-icon\${liked?' liked':''}" style="\${likeClr}">❤️</span>
      </button>
      <div class="sidebar-label" id="lc-\${id}">0</div>
    </div>
    <div class="sidebar-item">
      <button class="sidebar-btn" onclick="openComments('\${sub}','\${id}')"><span class="sidebar-icon">💬</span></button>
      <div class="sidebar-label" id="cc-\${id}">0</div>
    </div>
    <div class="sidebar-item">
      <button class="sidebar-btn" onclick="onShare()"><span class="sidebar-icon">↗</span></button>
      <div class="sidebar-label">Share</div>
    </div>
    \${isSelf ? '' : \`<div class="sidebar-item">
      <button class="sidebar-btn" onclick="openReport('\${sub}','\${id}')"><span class="sidebar-icon" style="font-size:20px;opacity:0.8">⚑</span></button>
      <div class="sidebar-label">Report</div>
    </div>\`}
  </div>\`;
}

function renderInfo(node, item) {
  const handle = '@' + node.subdomain; // full host — handles are globally unique across the network
  const title  = item?.title || '';
  const body   = item?.body  || '';
  const desc   = [title, body].filter(Boolean).join(' · ');
  const live   = node.liveStatus?.active ? \`<div class="info-live-badge" onclick="onLiveTap()" style="cursor:pointer;pointer-events:auto">● LIVE — tap to watch</div>\` : '';
  return \`<div class="card-info">
    \${live}
    <div class="info-username" onclick="openProfile('\${esc(node.subdomain)}')" style="cursor:pointer">\${esc(handle)}</div>
    \${desc ? \`<div class="info-desc">\${esc(desc)}</div>\` : ''}
  </div>\`;
}

function renderCard(node, postIdx) {
  if (!node.loaded) return \`<div class="card card-loading"><div class="spinner"></div></div>\`;
  if (node.feed === null) return \`<div class="card card-error"><div class="c-label">couldn't reach \${esc(node.subdomain)}</div></div>\`;
  if (node.feed.length === 0) {
    const hint = node.subdomain === SELF_SUBDOMAIN && isCreator ? 'tap + to post' : 'no posts yet';
    return \`<div class="card card-empty"><div class="c-label">\${esc(node.subdomain)}</div><div class="c-label" style="margin-top:6px;font-size:12px">\${hint}</div></div>\`;
  }

  const item   = node.feed[postIdx] || node.feed[0];
  const isSelf = node.subdomain === SELF_SUBDOMAIN;
  const sb     = renderSidebar(node, item);
  const info   = renderInfo(node, item);
  // media lives on the owning node's R2 — prefix remote nodes' relative /media URLs with their origin
  const msrc   = item.mediaUrl ? (item.mediaUrl.startsWith('http') ? item.mediaUrl : postBase(node) + item.mediaUrl) : '';

  if (item.type === 'live') {
    const handle = '@' + node.subdomain;
    const isSelf = node.subdomain === SELF_SUBDOMAIN;
    if (isSelf && isCreator) {
      return \`<div class="card card-live" data-live="self">
        <div style="position:absolute;inset:0;background:#111;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px">
          <div class="info-live-badge" style="font-size:16px;padding:6px 16px">● YOU ARE LIVE</div>
          <button class="btn-secondary" onclick="closeLiveModal();document.getElementById('liveModal').classList.add('show')">Manage Stream</button>
        </div>
        <div class="live-card-chat-area" id="liveCardChat"></div>
        \${renderSidebar(node, item)}
      </div>\`;
    }
    return \`<div class="card card-live" data-live="viewer">
      <video disableremoteplayback x-webkit-airplay="deny" id="liveCardVideo" autoplay playsinline style="position:absolute;inset:0;width:100%;height:100%;object-fit:cover;background:#000"></video>
      <div class="live-card-badges">
        <div class="info-live-badge">● LIVE</div>
      </div>
      <div class="live-card-info">
        <div class="info-username">\${esc(handle)}</div>
      </div>
      <div class="live-card-chat-area" id="liveCardChat"></div>
      \${renderSidebar(node, item)}
    </div>\`;
  }

  if (item.type === 'video' && item.mediaUrl) {
    return \`<div class="card card-video" data-id="\${esc(item.id)}">
      <div class="c-media"><video disableremoteplayback x-webkit-airplay="deny" src="\${esc(msrc)}" playsinline preload="metadata" loop></video></div>
      \${info}\${sb}
    </div>\`;
  }
  if (item.type === 'photo' && item.mediaUrl) {
    return \`<div class="card card-photo" data-id="\${esc(item.id)}">
      <div class="c-media"><img src="\${esc(msrc)}" alt="" loading="lazy"></div>
      \${info}\${sb}
    </div>\`;
  }
  // writing / text post
  return \`<div class="card card-writing" data-id="\${esc(item.id)}">
    <div class="writing-bg" style="background:\${writingBg(item.id)}"></div>
    <div class="writing-content">
      \${item.title ? \`<div class="writing-title">\${esc(item.title)}</div>\` : ''}
      \${item.body  ? \`<div class="writing-body">\${esc(item.body)}</div>\`   : ''}
    </div>
    \${info}\${sb}
  </div>\`;
}

// ── TWO-PANEL SYSTEM ──────────────────────────────────────────
const panels = [document.getElementById('panel-a'), document.getElementById('panel-b')];
let activePanelIdx = 0;
function activePanel()   { return panels[activePanelIdx]; }
function inactivePanel() { return panels[1-activePanelIdx]; }

let liveCardViewer = null;

function renderCurrent() {
  const node = nodeGraph[nodeIndex];
  const item = node.feed?.[node.postIndex];
  markSeen(item?.id); // record this post as seen for unseen-first ordering on return visits
  startDwell(node.subdomain); // feeds the on-device affinity map

  // Disconnect previous live viewer if leaving a live card
  if (liveCardViewer) { liveCardViewer.cleanup(); liveCardViewer = null; stopLiveChatPoll(); }

  activePanel().innerHTML        = renderCard(node, node.postIndex);
  activePanel().style.transition = 'none';
  activePanel().style.transform  = '';
  activePanel().style.zIndex     = '2';
  inactivePanel().style.zIndex   = '1';
  activateVideos(activePanel());
  refreshCurrentInteractions();
  prefetchUpcoming();
  if (node.feed && node.postIndex >= node.feed.length - 5) loadMorePosts(node);

  // Auto-connect viewer when landing on a live card
  if (item?.type === 'live' && !(isCreator && node.subdomain === SELF_SUBDOMAIN)) {
    const videoEl = activePanel().querySelector('#liveCardVideo');
    const statusEl = activePanel().querySelector('.live-card-info');
    liveCardViewer = new LiveViewer(node.subdomain);
    liveCardViewer.start(videoEl, statusEl || {}).catch(e => console.warn('live viewer:', e));
    startLiveCardChatPoll(node.subdomain);
  }
}

function activateVideos(p) {
  p.querySelectorAll('video').forEach(v => {
    if (!v.dataset.tapBound) {
      v.dataset.tapBound = '1';
      v.addEventListener('click', () => {
        if (v.paused) {
          v.play().catch(() => {});
          showVideoIcon(v, '&#9654;');
        } else {
          v.pause();
          showVideoIcon(v, '&#10074;&#10074;');
        }
      });
    }
    // never auto-start feed audio under the live overlay / ad interstitial
    // (the 60s live-status flip re-rendering mid-broadcast was leaking the first video's audio)
    if (feedCovered()) { v.pause(); return; }
    v.play().catch(() => {
      v.muted = true;
      v.play().catch(() => {});
    });
  });
}
function showVideoIcon(v, icon) {
  let el = v.parentElement.querySelector('.vid-icon');
  if (!el) {
    el = document.createElement('div');
    el.className = 'vid-icon';
    el.style.cssText = 'position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);font-size:48px;color:rgba(255,255,255,0.85);opacity:0;pointer-events:none;transition:opacity 0.15s;z-index:5;font-family:Arial,sans-serif;filter:drop-shadow(0 0 8px rgba(0,0,0,0.5))';
    v.parentElement.style.position = 'relative';
    v.parentElement.appendChild(el);
  }
  el.innerHTML = icon;
  el.style.opacity = '1';
  clearTimeout(el._t);
  el._t = setTimeout(() => { el.style.opacity = '0'; }, 600);
}
function deactivateVideos(p) { p.querySelectorAll('video').forEach(v => { v.pause(); v.currentTime=0; }); }

// ── INDICATORS ────────────────────────────────────────────────
// progress segments removed by user request (the white lines up top) — kept as a no-op since many callers remain
function updateIndicators() {}

// ── LIKE ──────────────────────────────────────────────────────
function postBase(node) { return (node && node.subdomain !== SELF_SUBDOMAIN) ? 'https://' + node.subdomain : ''; }

async function sendLike(id, liked) {
  const base = postBase(nodeGraph[nodeIndex]);
  try {
    const r = await fetch(base + '/post/like', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ postId: id, vid: getViewerId(), liked }),
    }).then(r => r.json());
    const el = document.getElementById('lc-' + id);
    if (el && r.count != null) el.textContent = fmtCount(r.count);
  } catch(e) {}
}

function onLike(id, btn) {
  if (!id) return;
  const nowLiked = toggleLike(id);
  const icon = btn.querySelector('.like-icon');
  icon.classList.toggle('liked', nowLiked);
  icon.style.color  = nowLiked ? '#FE2C55' : '';
  icon.style.filter = nowLiked ? 'none' : 'grayscale(0.2)';
  icon.classList.add('pop');
  setTimeout(() => icon.classList.remove('pop'), 300);
  sendLike(id, nowLiked);
}

// ── INTERACTIONS REFRESH (real like + comment counts) ─────────
function refreshCurrentInteractions() {
  const node = nodeGraph[nodeIndex];
  const item = node.feed?.[node.postIndex];
  if (item) refreshInteractions(node, item);
}
async function refreshInteractions(node, item) {
  const base = postBase(node);
  const vid = getViewerId();
  try {
    const [likeRes, cmtRes] = await Promise.all([
      fetch(base + '/post/likes?postId=' + encodeURIComponent(item.id) + '&vid=' + encodeURIComponent(vid)).then(r => r.json()),
      fetch(base + '/post/comments?postId=' + encodeURIComponent(item.id)).then(r => r.json()),
    ]);
    const lc = document.getElementById('lc-' + item.id);
    if (lc && likeRes.count != null) lc.textContent = fmtCount(likeRes.count);
    const cc = document.getElementById('cc-' + item.id);
    if (cc) cc.textContent = fmtCount((cmtRes.comments || []).length);
    // reconcile the heart with the server (so it's right across devices)
    if (likeRes.liked) {
      const l = getLikes(); if (!l[item.id]) { l[item.id] = 1; saveLikes(l); }
      const icon = activePanel().querySelector('.like-icon');
      if (icon) { icon.classList.add('liked'); icon.style.color = '#FE2C55'; icon.style.filter = 'none'; }
    }
  } catch(e) {}
}

// ── NAMES ─────────────────────────────────────────────────────
// Creators appear as their handle; guests pick a display name once (stored locally).
let _namePromiseResolve = null;

function displayName() {
  if (isCreator) return SELF_SUBDOMAIN || 'viewer'; // full host — unique across the network
  return localStorage.getItem('guestName') || '';
}

// Resolves to a usable name, opening the name modal first if a guest hasn't set one.
// Resolves to null if the guest cancels.
function ensureName() {
  return new Promise(resolve => {
    const n = displayName();
    if (n) { resolve(n); return; }
    _namePromiseResolve = resolve;
    const input = document.getElementById('nameInput');
    input.value = localStorage.getItem('guestName') || '';
    document.getElementById('nameModal').classList.add('show');
    setTimeout(() => input.focus(), 100);
  });
}

function saveName() {
  const v = document.getElementById('nameInput').value.trim().slice(0, 30);
  if (!v) return;
  localStorage.setItem('guestName', v);
  if (liveSocket) liveSocket.rename(v); // already in a live room — update the broadcaster's viewer list
  document.getElementById('nameModal').classList.remove('show');
  if (_namePromiseResolve) { _namePromiseResolve(v); _namePromiseResolve = null; }
}

function closeNameModal() {
  document.getElementById('nameModal').classList.remove('show');
  if (_namePromiseResolve) { _namePromiseResolve(null); _namePromiseResolve = null; }
}

// ── COMMENTS ──────────────────────────────────────────────────
let _commentsCtx = { subdomain: '', postId: '' };

function commentsBase() { return _commentsCtx.subdomain === SELF_SUBDOMAIN ? '' : 'https://' + _commentsCtx.subdomain; }

function openComments(subdomain, postId) {
  if (!postId) return;
  _commentsCtx = { subdomain, postId };
  document.getElementById('commentsSheet').classList.add('show');
  pauseFeedVideos();
  document.getElementById('commentsList').innerHTML =
    '<div style="display:flex;align-items:center;justify-content:center;padding:50px;color:rgba(255,255,255,0.3)">Loading…</div>';
  loadComments();
}
function closeComments() {
  document.getElementById('commentsSheet').classList.remove('show');
  resumeFeedVideos();
}
async function loadComments() {
  try {
    const data = await fetch(commentsBase() + '/post/comments?postId=' + encodeURIComponent(_commentsCtx.postId)).then(r => r.json());
    renderComments(data.comments || []);
    const cc = document.getElementById('cc-' + _commentsCtx.postId);
    if (cc) cc.textContent = fmtCount((data.comments || []).length);
  } catch(e) {
    document.getElementById('commentsList').innerHTML =
      '<div style="padding:40px;text-align:center;color:rgba(255,255,255,0.3)">Could not load comments</div>';
  }
}
// Creators comment under their full host (globally unique). At render time map a
// host back to its node so they get their profile pic + display name; guests keep
// their typed name and a letter avatar.
function commenterInfo(name) {
  const n = name && nodeGraph.find(x => x.subdomain === name);
  if (!n) return null;
  return { display: n.displayName || String(name).split('.')[0], avatarUrl: n.avatarUrl || null };
}
function renderComments(list) {
  const canDelete = isCreator && _commentsCtx.subdomain === SELF_SUBDOMAIN;
  if (!list.length) {
    document.getElementById('commentsList').innerHTML =
      '<div class="inbox-empty"><div class="inbox-empty-icon">💬</div>No comments yet</div>';
    return;
  }
  document.getElementById('commentsList').innerHTML = list.map(c => {
    const who = commenterInfo(c.name);
    const shown = who ? who.display : (c.name || 'viewer');
    const grad = avatarGrad(c.name || '?');
    const avInner = who && who.avatarUrl
      ? \`<img src="\${esc(who.avatarUrl)}" alt="" style="width:100%;height:100%;object-fit:cover;border-radius:50%">\`
      : esc((shown[0] || '?').toUpperCase());
    const del = canDelete ? \`<button onclick="deleteComment('\${esc(c.id)}')" style="background:none;color:rgba(255,255,255,0.4);font-size:18px;padding:0 4px">×</button>\` : '';
    return \`<div class="inbox-notif" style="cursor:default">
      <div class="inbox-notif-avatar" style="background:\${grad};overflow:hidden">\${avInner}</div>
      <div class="inbox-notif-body">
        <div class="inbox-notif-text"><strong>\${esc(shown)}</strong> \${esc(c.text)}</div>
        <div class="inbox-notif-time">\${timeAgo(c.at)}</div>
      </div>
      \${del}
    </div>\`;
  }).join('');
}
async function submitComment() {
  const input = document.getElementById('commentInput');
  const text = input.value.trim();
  if (!text) return;
  const name = await ensureName();
  if (!name) return; // guest cancelled the name prompt
  input.value = '';
  try {
    await fetch(commentsBase() + '/post/comment', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ postId: _commentsCtx.postId, text, name }),
    });
    loadComments();
  } catch(e) { showToast('Failed'); }
}
async function deleteComment(id) {
  try {
    await fetch('/admin/comment/delete', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
      body: JSON.stringify({ postId: _commentsCtx.postId, id }),
    });
    loadComments();
  } catch(e) {}
}

// ── REPORT CONTENT ────────────────────────────────────────────
let _reportTarget = null;
function openReport(sub, postId) {
  _reportTarget = { sub, postId };
  document.getElementById('reportDetails').value = '';
  document.getElementById('reportReason').value = 'other';
  document.getElementById('reportModal').classList.add('show');
}
function closeReport() { document.getElementById('reportModal').classList.remove('show'); }
async function submitReport() {
  if (!_reportTarget) return;
  // the report goes to the node that HOSTS the content
  const base = _reportTarget.sub === SELF_SUBDOMAIN ? '' : 'https://' + _reportTarget.sub;
  try {
    const r = await fetch(base + '/report', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        postId: _reportTarget.postId,
        reason: document.getElementById('reportReason').value,
        details: document.getElementById('reportDetails').value.trim(),
      }),
    });
    if (!r.ok) throw new Error(r.status);
    toast('Report sent to the operator');
  } catch(e) { toast('Could not send report'); }
  closeReport();
}

// ── DOUBLE-TAP LIKE ───────────────────────────────────────────
let lastTap = 0;
const stage = document.getElementById('stage');

stage.addEventListener('touchend', e => {
  const touch = e.changedTouches[0];
  const dx = Math.abs(touch.clientX - G.startX);
  const dy = Math.abs(touch.clientY - G.startY);
  if (dx > 12 || dy > 12) return; // was a swipe
  const now = Date.now();
  if (now - lastTap < 280) {
    showHeartBurst(touch.clientX, touch.clientY);
    const node = nodeGraph[nodeIndex];
    const item = node.feed?.[node.postIndex];
    if (item && !isLiked(item.id)) {
      toggleLike(item.id);
      const icon = activePanel().querySelector('.like-icon');
      if (icon) { icon.classList.add('liked','pop'); icon.style.color='#FE2C55'; icon.style.filter='none'; }
      sendLike(item.id, true);
    }
  }
  lastTap = now;
}, { passive: true });

function showHeartBurst(x, y) {
  const el = document.getElementById('heartBurst');
  el.style.left = x+'px'; el.style.top = y+'px';
  el.classList.remove('burst'); void el.offsetWidth;
  el.classList.add('burst');
}

// ── SHARE ─────────────────────────────────────────────────────
async function onShare() {
  const node = nodeGraph[nodeIndex];
  const base = node.url || window.location.origin;
  // share the POST you're on — /p/<id> permalinks carry the OG meta, so the link
  // unfurls with the actual video/photo. Falls back to the node URL on empty cards.
  const item = node.feed?.[node.postIndex];
  const url   = item?.id ? base + '/p/' + item.id : base;
  const title = item?.title || node.displayName || node.subdomain;
  try {
    if (navigator.share) await navigator.share({ title, url });
    else { await navigator.clipboard.writeText(url); toast('link copied'); }
  } catch(e) {}
}

// ── CREATOR MODE ──────────────────────────────────────────────
let createTaps = 0, createTapTimer;

function onCreateTap() {
  if (isCreator) { document.getElementById('publishModal').classList.add('show'); return; }
  createTaps++;
  clearTimeout(createTapTimer);
  createTapTimer = setTimeout(() => createTaps = 0, 3000);
  if (createTaps >= 5) { createTaps = 0; document.getElementById('unlockModal').classList.add('show'); }
  else { toast('tap ' + (5-createTaps) + ' more to unlock'); }
}

// ── PROFILE ───────────────────────────────────────────────────
let _profileCache = {};

// The Profile tab is always YOUR profile (reachable from anywhere in the app);
// other creators' profiles open by tapping their avatar or @handle on their cards.
function onProfileTap() {
  openProfile(SELF_SUBDOMAIN);
}

function openProfile(subdomain) {
  const sheet = document.getElementById('profileSheet');
  document.getElementById('profileHandle').textContent = '@' + subdomain;
  const isOwn = subdomain === SELF_SUBDOMAIN && isCreator;
  document.getElementById('profileEditBtn').style.display = isOwn ? 'block' : 'none';
  document.getElementById('profileEditSpacer').style.display = isOwn ? 'none' : 'block';
  document.getElementById('profileBody').innerHTML =
    '<div style="display:flex;align-items:center;justify-content:center;padding:60px;color:rgba(255,255,255,0.3)">Loading…</div>';
  sheet.classList.add('show');
  pauseFeedVideos();
  loadProfileData(subdomain).then(data => renderProfileBody(data, subdomain));
}

function closeProfile() {
  exitSelMode();
  document.getElementById('profileSheet').classList.remove('show');
  resumeFeedVideos();
}

async function loadProfileData(subdomain) {
  if (_profileCache[subdomain] && Date.now() - _profileCache[subdomain].ts < 30000)
    return _profileCache[subdomain].data;
  const base = subdomain === SELF_SUBDOMAIN ? '' : 'https://' + subdomain;
  const [profileRes, feedRes, peersRes] = await Promise.allSettled([
    fetch(base + '/profile.json').then(r => r.ok ? r.json() : null),
    fetch(base + '/.well-known/feed.json').then(r => r.ok ? r.json() : null),
    fetch(base + '/.well-known/peers.json').then(r => r.ok ? r.json() : null),
  ]);
  const profile = profileRes.value || {};
  const feed = feedRes.value?.items || [];
  const peers = peersRes.value?.peers || [];
  const data = { ...profile, feed, peerCount: peers.length, postCount: feed.length };
  _profileCache[subdomain] = { data, ts: Date.now() };
  return data;
}

function renderProfileBody(data, subdomain) {
  const isOwn = subdomain === SELF_SUBDOMAIN && isCreator;
  const handle = data.subdomain || subdomain; // full host — globally unique
  const name = data.displayName || handle.split('.')[0];
  const bio = data.bio || '';
  const grad = avatarGrad(subdomain);
  // media lives on the profile owner's R2 — prefix remote (non-self) relative URLs with their origin
  const pbase = subdomain === SELF_SUBDOMAIN ? '' : 'https://' + subdomain;
  const pabs = u => u ? (u.startsWith('http') ? u : pbase + u) : '';
  const avatarHtml = data.avatarUrl
    ? \`<img src="\${esc(pabs(data.avatarUrl))}" alt="">\`
    : esc(name[0].toUpperCase());

  const gridHtml = (data.feed || []).map(item => {
    const gridDel = isOwn ? \`<button class="grid-del" onclick="event.stopPropagation();deleteFromGrid('\${esc(item.id)}')">×</button>\` : '';
    if (item.type === 'video' && item.mediaUrl)
      return \`<div class="profile-grid-item" data-id="\${esc(item.id)}" onclick="openProfilePost('\${esc(subdomain)}','\${esc(item.id)}')">
        <video disableremoteplayback x-webkit-airplay="deny" data-src="\${esc(pabs(item.mediaUrl))}" preload="metadata" muted playsinline></video>
        <div class="grid-type-icon">▶</div>\${gridDel}
      </div>\`;
    if (item.type === 'photo' && item.mediaUrl)
      return \`<div class="profile-grid-item" data-id="\${esc(item.id)}" onclick="openProfilePost('\${esc(subdomain)}','\${esc(item.id)}')">
        <img src="\${esc(pabs(item.mediaUrl))}" loading="lazy" alt="">\${gridDel}
      </div>\`;
    return \`<div class="profile-grid-item" data-id="\${esc(item.id)}" onclick="openProfilePost('\${esc(subdomain)}','\${esc(item.id)}')">
      <div class="profile-grid-text" style="background:\${grad}">\${esc((item.title||item.body||'').slice(0,40))}</div>\${gridDel}
    </div>\`;
  }).join('');

  const joinBtn = isOwn ? \`<button id="joinNetworkBtn" class="profile-connect-btn" style="display:\${isCreator && !isMember ? 'block' : 'none'};background:#20D5EC" onclick="requestJoin()">Join the network</button>\` : '';
  const hostBtn = (isOwn && isHost) ? \`<button class="profile-connect-btn" style="background:#20D5EC" onclick="openCreators()">Manage creators</button>\` : '';
  // Host configures the node-wide ad + sees the ledger; a hosted creator with a share %
  // gets a read-only earnings view; everyone else gets nothing.
  const adBtn = (isOwn && isHost) ? \`<button class="profile-connect-btn" style="background:rgba(255,255,255,0.12)" onclick="openPrerollSheet()">💰 Node ad &amp; revenue</button>\`
              : (isOwn && data.adsEnabled) ? \`<button class="profile-connect-btn" style="background:rgba(255,255,255,0.12)" onclick="openEarnings()">💰 Your earnings</button>\` : '';
  const blocked = getBlocked().includes(subdomain);
  const blockBtn = !isOwn ? \`<button class="profile-connect-btn" style="background:rgba(255,255,255,0.12)" onclick="toggleBlock('\${esc(subdomain)}')">\${blocked ? 'Unblock' : 'Block'}</button>\` : '';
  const importBtn = (isOwn && isCreator) ? \`<button class="profile-connect-btn" style="background:rgba(255,255,255,0.12)" onclick="openImport()">📥 Import from TikTok</button>\` : '';
  const algoBtn = subdomain === SELF_SUBDOMAIN ? \`<button class="profile-connect-btn" style="background:rgba(255,255,255,0.12)" onclick="openAlgo()">🎛 My algorithm</button>\` : '';
  const selBtn = (isOwn && isCreator && (data.postCount || 0) > 0) ? \`<button class="profile-connect-btn" style="background:rgba(255,255,255,0.12)" onclick="toggleSelMode()">☑️ Select posts</button>\` : '';

  document.getElementById('profileBody').innerHTML = \`
    <div class="profile-hero">
      <div class="profile-avatar" style="background:\${grad}">\${avatarHtml}</div>
      <div class="profile-name">\${esc(name)}</div>
      <div class="profile-handle">@\${esc(handle)}</div>
      <div class="profile-stats">
        <div class="profile-stat">
          <div class="profile-stat-n">\${data.postCount || 0}</div>
          <div class="profile-stat-l">Posts</div>
        </div>
        <div class="profile-stat">
          <div class="profile-stat-n">\${data.networkSize ?? data.peerCount ?? 0}</div>
          <div class="profile-stat-l">Network</div>
        </div>
      </div>
      \${bio ? \`<div class="profile-bio">\${esc(bio)}</div>\` : ''}
      \${joinBtn}\${adBtn}\${hostBtn}\${importBtn}\${algoBtn}\${selBtn}\${blockBtn}
      <a href="\${pbase}/legal" target="_blank" style="display:inline-block;margin-top:12px;font-size:12px;color:rgba(255,255,255,0.4);text-decoration:underline">Policies &amp; reporting</a>
    </div>
    <div class="profile-grid">\${gridHtml || '<div style="padding:40px;text-align:center;color:rgba(255,255,255,0.3);grid-column:1/-1">No posts yet</div>'}</div>
  \`;
  lazyLoadGridVideos(document.getElementById('profileBody'));
}

// Grid videos only fetch their metadata/thumbnail when scrolled near the viewport —
// without this, opening a 100-post profile fires 100 video fetches at once.
function lazyLoadGridVideos(root) {
  const vids = root.querySelectorAll('video[data-src]');
  if (!('IntersectionObserver' in window)) { vids.forEach(v => { v.src = v.dataset.src; }); return; }
  const io = new IntersectionObserver(entries => {
    entries.forEach(en => {
      if (en.isIntersecting) {
        const v = en.target;
        if (!v.src) v.src = v.dataset.src;
        io.unobserve(v);
      }
    });
  }, { root: root, rootMargin: '300px' });
  vids.forEach(v => io.observe(v));
}

// ── BULK SELECT + DELETE (own profile grid) ──────────────────
let _selMode = false;
const _selIds = new Set();
function toggleSelMode() {
  _selMode = !_selMode;
  _selIds.clear();
  document.getElementById('selBar').style.display = _selMode ? 'flex' : 'none';
  document.querySelectorAll('.profile-grid-item.sel').forEach(el => el.classList.remove('sel'));
  updateSelBar();
}
function exitSelMode() { if (_selMode) toggleSelMode(); }
function toggleSelect(id) {
  if (_selIds.has(id)) _selIds.delete(id); else _selIds.add(id);
  const el = document.querySelector('.profile-grid-item[data-id="' + id + '"]');
  if (el) el.classList.toggle('sel', _selIds.has(id));
  updateSelBar();
}
function updateSelBar() {
  const b = document.getElementById('selDeleteBtn');
  if (b) { b.textContent = 'Delete (' + _selIds.size + ')'; b.disabled = !_selIds.size; }
}
async function bulkDelete() {
  const n = _selIds.size;
  if (!n) return;
  if (!confirm('Delete ' + n + ' post' + (n === 1 ? '' : 's') + '? Media is removed too. This cannot be undone.')) return;
  try {
    const res = await fetch('/admin/delete', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
      body: JSON.stringify({ ids: Array.from(_selIds) }),
    });
    if (!res.ok) { toast('delete failed'); return; }
    toast(n + ' deleted');
    exitSelMode();
    nodeGraph[0].loaded = false; nodeGraph[0].feed = null;
    delete _profileCache[SELF_SUBDOMAIN];
    await loadNode(nodeGraph[0]);
    loadProfileData(SELF_SUBDOMAIN).then(d => renderProfileBody(d, SELF_SUBDOMAIN));
  } catch(e) { toast('delete failed'); }
}

function openProfilePost(subdomain, postId) {
  if (_selMode) { toggleSelect(postId); return; }
  // Navigate to that node+post in feed
  closeProfile();
  const idx = nodeGraph.findIndex(n => n.subdomain === subdomain);
  if (idx >= 0) {
    const node = nodeGraph[idx];
    const postIdx = (node.feed || []).findIndex(p => p.id === postId);
    if (postIdx >= 0) node.postIndex = postIdx;
    nodeIndex = idx;
    renderCurrent(); updateIndicators();
  }
}

// ── BLOCK (viewer-side, on-device) ────────────────────────────
function getBlocked() { try { return JSON.parse(localStorage.getItem('blockedNodes') || '[]'); } catch(e) { return []; } }
function toggleBlock(subdomain) {
  let b = getBlocked();
  if (b.includes(subdomain)) {
    b = b.filter(x => x !== subdomain);
    toast('Unblocked @' + subdomain);
  } else {
    b.push(subdomain);
    toast("Blocked — you won't see this creator");
    const i = nodeGraph.findIndex(n => n.subdomain === subdomain);
    if (i > 0) { // index 0 is this origin itself — filtered on next visit instead
      nodeGraph.splice(i, 1);
      if (nodeIndex >= nodeGraph.length) nodeIndex = nodeGraph.length - 1;
      renderCurrent(); updateIndicators();
    }
    closeProfile();
  }
  localStorage.setItem('blockedNodes', JSON.stringify(b));
}

// ── EDIT PROFILE ──────────────────────────────────────────────
let _editAvatarUrl = null;

function openEditProfile() {
  const sheet = document.getElementById('editProfileSheet');
  loadProfileData(SELF_SUBDOMAIN).then(data => {
    document.getElementById('editDisplayName').value = data.displayName || '';
    document.getElementById('editBio').value = data.bio || '';
    _editAvatarUrl = data.avatarUrl || null;
    renderEditAvatar(data);
  });
  sheet.classList.add('show');
}

function renderEditAvatar(data) {
  const el = document.getElementById('editAvatarPreview');
  const grad = avatarGrad(SELF_SUBDOMAIN);
  el.style.background = grad;
  if (_editAvatarUrl) {
    el.innerHTML = \`<img src="\${esc(_editAvatarUrl)}" alt="" style="width:100%;height:100%;object-fit:cover;border-radius:50%">\`;
  } else {
    const handle = SELF_SUBDOMAIN.split('.')[0];
    el.textContent = handle[0].toUpperCase();
  }
}

async function onAvatarPick(input) {
  const file = input.files[0];
  if (!file) return;
  const btn = document.getElementById('saveProfileBtn');
  btn.disabled = true; btn.textContent = 'Uploading…';
  try {
    const fd = new FormData(); fd.append('file', file);
    const res = await fetch('/admin/upload', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token }, body: fd });
    const data = await res.json();
    _editAvatarUrl = data.url;
    document.getElementById('editAvatarPreview').innerHTML = \`<img src="\${esc(data.url)}" style="width:100%;height:100%;object-fit:cover;border-radius:50%">\`;
  } catch(e) { showToast('Upload failed'); }
  btn.disabled = false; btn.textContent = 'Save';
}

async function saveProfile() {
  const btn = document.getElementById('saveProfileBtn');
  btn.disabled = true; btn.textContent = 'Saving…';
  try {
    const body = {
      displayName: document.getElementById('editDisplayName').value.trim(),
      bio: document.getElementById('editBio').value.trim(),
      avatarUrl: _editAvatarUrl,
    };
    const res = await fetch('/admin/profile', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error('Save failed');
    delete _profileCache[SELF_SUBDOMAIN];
    showToast('Profile saved');
    closeEditProfile();
  } catch(e) { showToast('Error: ' + e.message); }
  btn.disabled = false; btn.textContent = 'Save';
}

function closeEditProfile() {
  document.getElementById('editProfileSheet').classList.remove('show');
}

// ── TIKTOK IMPORT ────────────────────────────────────────────
let _importItems = [];
let _importCancel = false;
function openImport() {
  _importItems = []; _importCancel = false;
  document.getElementById('importIntro').style.display = 'block';
  document.getElementById('importPlan').style.display = 'none';
  document.getElementById('importProgress').style.display = 'none';
  document.getElementById('importFile').value = '';
  document.getElementById('importModal').classList.add('show');
}
function closeImport() { _importCancel = true; document.getElementById('importModal').classList.remove('show'); }
// TikTok export dates are "YYYY-MM-DD HH:MM:SS" (UTC); tolerate ISO too.
function tiktokDateToIso(d) {
  if (!d) return null;
  const t = Date.parse(String(d).includes('T') ? d : String(d).replace(' ', 'T') + 'Z');
  return isNaN(t) ? null : new Date(t).toISOString();
}
// Walk the export and collect POSTED videos: objects with an http Link + Date whose
// key path looks like the user's own videos. Likes/favorites/shares/watch-history
// sections also carry Link+Date — those are OTHER people's videos, so exclude them.
function parseTikTokExport(data, basePath) {
  const out = [];
  const seen = {};
  (function walk(o, path) {
    if (Array.isArray(o)) { for (const it of o) walk(it, path); return; }
    if (!o || typeof o !== 'object') return;
    const link = o.Link || o.link || o.VideoLink || o.videoLink;
    const date = o.Date || o.date || o.CreateTime || o.createTime;
    if (typeof link === 'string' && link.startsWith('http') && date) {
      const p = path.toLowerCase();
      const owns = p.includes('video') || p.includes('post');
      // reposts/likes/favorites/etc are OTHER people's videos (and tiktok.com page links, not media)
      const theirs = p.includes('like') || p.includes('favorite') || p.includes('share') || p.includes('brows') || p.includes('watch') || p.includes('comment') || p.includes('repost') || p.includes('deleted');
      if (owns && !theirs && !seen[link]) {
        seen[link] = 1;
        // TikTok exports the literal string 'N/A' when a caption isn't included
        const clean = v => { const s = String(v || '').trim(); return (s === 'N/A' || s.toLowerCase() === 'none') ? '' : s; };
        out.push({
          url: link,
          createdAt: tiktokDateToIso(date),
          title: (clean(o.Title) || clean(o.Desc) || clean(o.Description) || clean(o.Caption) || clean(o.AddYoursText)).slice(0, 300),
          likes: o.Likes || o.likes || '',
        });
      }
      return;
    }
    for (const k in o) walk(o[k], path + '/' + k);
  })(data, basePath || '');
  return out;
}
function onImportFile(file) {
  if (!file) return;
  file.text().then(txt => {
    let data; try { data = JSON.parse(txt); } catch(e) { toast('That file is not valid JSON'); return; }
    // prefer the canonical posted-videos section when present; fall back to the full walk
    let items = [];
    try {
      const vl = data && data.Video && data.Video.Videos && data.Video.Videos.VideoList;
      if (Array.isArray(vl)) items = parseTikTokExport(vl, 'video/videos/videolist');
    } catch(e) {}
    if (!items.length) items = parseTikTokExport(data);
    items = items.filter(it => it.createdAt);
    if (!items.length) { toast('No videos found in this file'); return; }
    items.sort((a, b) => a.createdAt.localeCompare(b.createdAt)); // oldest first — feed sort handles display order
    _importItems = items;
    document.getElementById('importIntro').style.display = 'none';
    document.getElementById('importPlan').style.display = 'block';
    document.getElementById('importSummary').textContent =
      items.length + ' video' + (items.length === 1 ? '' : 's') + ' found (' +
      items[0].createdAt.slice(0, 10) + ' → ' + items[items.length - 1].createdAt.slice(0, 10) +
      '). They keep their original dates and captions.';
  }).catch(() => toast('Could not read the file'));
}
async function runImport() {
  if (!_importItems.length) return;
  _importCancel = false;
  document.getElementById('importPlan').style.display = 'none';
  document.getElementById('importProgress').style.display = 'block';
  document.getElementById('importErrors').innerHTML = '';
  const total = _importItems.length;
  let done = 0, failed = 0, dups = 0;
  const status = document.getElementById('importStatus');
  const bar = document.getElementById('importBar');
  for (const it of _importItems) {
    if (_importCancel) break;
    status.textContent = 'Importing ' + (done + failed + 1) + ' of ' + total + '…';
    try {
      const r = await fetch('/admin/import-url', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: it.url, title: it.title, createdAt: it.createdAt, source: 'tiktok' }),
      });
      const d = await r.json().catch(() => ({}));
      if (r.ok && d.ok) { done++; if (d.dup) dups++; }
      else {
        failed++;
        document.getElementById('importErrors').innerHTML +=
          '<div>' + esc(it.createdAt.slice(0, 10)) + ': ' + esc(d.error || ('error ' + r.status)) + '</div>';
      }
    } catch(e) { failed++; }
    bar.style.width = Math.round(((done + failed) / total) * 100) + '%';
  }
  status.textContent = (_importCancel ? 'Cancelled — ' : 'Done — ') + done + ' imported' +
    (dups ? ' (' + dups + ' already there)' : '') + (failed ? ', ' + failed + ' failed' : '') + '.';
  document.getElementById('importCancelBtn').textContent = 'Close';
  document.getElementById('importCancelBtn').onclick = closeImport;
  // refresh our own feed so the imports show up without a reload
  const self = nodeGraph.find(n => n.subdomain === SELF_SUBDOMAIN);
  if (self) { self.loaded = false; self.feed = undefined; loadNode(self).then(() => { if (nodeGraph[nodeIndex] === self) renderCurrent(); }); }
}

// ── LIVE VIEWER LIST + CHAT MODERATION ───────────────────────
function liveChatBase() {
  const node = nodeGraph[nodeIndex];
  return (node && node.subdomain !== SELF_SUBDOMAIN) ? 'https://' + node.subdomain : '';
}
async function openLiveViewers() {
  // who's-here is the HOST's moderation view — viewers see only the count on the badge
  const node = nodeGraph[nodeIndex];
  const canMod = isCreator && (!node || node.subdomain === SELF_SUBDOMAIN);
  if (!canMod) return;
  document.getElementById('liveViewersModal').classList.add('show');
  const body = document.getElementById('liveViewersBody');
  body.textContent = 'Loading…';
  try {
    const d = await fetch(liveChatBase() + '/live/who.json', { headers: { 'Authorization': 'Bearer ' + token } }).then(r => r.json());
    const list = d.viewers || [];
    const row = (name, tag, right) =>
      '<div style="display:flex;align-items:center;justify-content:space-between;gap:8px;padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.08)">' +
      '<span style="font-size:14px;min-width:0;overflow:hidden;text-overflow:ellipsis">' + name + tag + '</span>' + right + '</div>';
    const muteBtn = (sid, name, muted) =>
      '<button class="btn-secondary" style="padding:4px 10px;font-size:12px' + (muted ? ';color:#FE2C55' : '') + '" data-sid="' + esc(sid) + '" data-name="' + esc(name) + '" data-muted="' + (muted ? '1' : '') + '" onclick="toggleLiveMute(this)">' + (muted ? 'Unmute' : 'Mute') + '</button>';
    let html = list.map(v => {
      const tag = v.role === 'broadcaster' ? ' <span style="font-size:11px;color:#FE2C55;font-weight:600">● host</span>' : '';
      const right = canMod && v.sid && v.role !== 'broadcaster' ? muteBtn(v.sid, v.name, v.muted) : '';
      return row(esc(v.name), tag, right);
    }).join('') || '<div style="color:rgba(255,255,255,0.5);font-size:14px;padding:16px 0;text-align:center">Nobody connected right now</div>';
    if (canMod && d.mutedList && d.mutedList.length) {
      const online = {}; list.forEach(v => { if (v.sid) online[v.sid] = 1; });
      const offline = d.mutedList.filter(m => !online[m.sid]);
      if (offline.length) {
        html += '<div style="font-size:11px;color:rgba(255,255,255,0.45);text-transform:uppercase;letter-spacing:0.06em;margin:14px 0 4px">Muted (not here now)</div>' +
          offline.map(m => row(esc(m.name), '', muteBtn(m.sid, m.name, true))).join('');
      }
    }
    body.innerHTML = html;
  } catch(e) { body.textContent = 'Could not load the viewer list'; }
}
function closeLiveViewers() { document.getElementById('liveViewersModal').classList.remove('show'); }
async function toggleLiveMute(el) {
  const sid = el.dataset.sid, name = el.dataset.name || 'viewer', mute = !el.dataset.muted;
  if (mute && !confirm('Mute ' + name + '? Their chat messages will be hidden for everyone, on this and future streams.')) return;
  try {
    await fetch('/admin/live/mute', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ sid, name, muted: mute }) });
    toast(name + (mute ? ' muted' : ' unmuted'));
    openLiveViewers();
  } catch(e) { toast('failed'); }
}
function modMuteTap(el) {
  const sid = el.dataset.sid; if (!sid) return;
  const name = el.dataset.name || 'viewer';
  if (!confirm('Mute ' + name + '? Their chat messages will be hidden for everyone, on this and future streams.')) return;
  fetch('/admin/live/mute', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ sid, name, muted: true }) })
    .then(() => toast(name + ' muted')).catch(() => toast('failed'));
}

// ── STREAM HISTORY (broadcaster analytics) ───────────────────
function fmtStreamDur(sec) {
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s = sec % 60;
  if (h) return h + 'h ' + String(m).padStart(2, '0') + 'm';
  if (m) return m + 'm ' + String(s).padStart(2, '0') + 's';
  return s + 's';
}
async function openStreamHistory() {
  document.getElementById('streamHistoryModal').classList.add('show');
  const body = document.getElementById('streamHistoryBody');
  body.textContent = 'Loading…';
  try {
    const d = await fetch('/admin/live/history', { headers: { 'Authorization': 'Bearer ' + token } }).then(r => r.json());
    const streams = d.streams || [];
    if (!streams.length) { body.innerHTML = '<div style="color:rgba(255,255,255,0.5);font-size:14px;padding:20px 0;text-align:center">No streams yet — go live and your sessions will show up here.</div>'; return; }
    const BITRATE_MBPS = 2.5, EGRESS_PER_GB = 0.05; // matches the broadcaster's maxBitrate cap
    body.innerHTML = streams.map(s => {
      const when = new Date(s.startedAt).toLocaleString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
      const gb = (s.viewerSec || 0) * BITRATE_MBPS / 8 / 1000; // Mbps→MB/s, sec→GB
      const cost = gb * EGRESS_PER_GB;
      return '<div style="padding:12px 0;border-bottom:1px solid rgba(255,255,255,0.08)">' +
        '<div style="display:flex;justify-content:space-between;font-size:14px;font-weight:600"><span>' + esc(when) + '</span><span>' + fmtStreamDur(s.durationSec || 0) + '</span></div>' +
        '<div style="display:flex;justify-content:space-between;font-size:12px;color:rgba(255,255,255,0.55);margin-top:4px">' +
          '<span>peak ' + (s.peakViewers || 0) + ' viewer' + (s.peakViewers === 1 ? '' : 's') + ' · ' + fmtStreamDur(Math.round(s.viewerSec || 0)) + ' watched</span>' +
          '<span>~' + (gb < 0.01 ? '<0.01' : gb.toFixed(2)) + ' GB · ~$' + cost.toFixed(2) + '</span>' +
        '</div>' +
      '</div>';
    }).join('');
  } catch(e) { body.textContent = 'Could not load stream history'; }
}
function closeStreamHistory() { document.getElementById('streamHistoryModal').classList.remove('show'); }

// ── PRE-ROLL ADMIN ────────────────────────────────────────────
let _prerollMediaUrl = null, _prerollContentType = null;

async function openPrerollSheet() {
  document.getElementById('prerollSheet').classList.add('show');
  try {
    const data = await fetch('/admin/preroll.json', { headers: { 'Authorization': 'Bearer ' + token } }).then(r => r.json());
    const cfg = data.preroll || {};
    document.getElementById('prerollEnabled').checked = !!cfg.enabled;
    document.getElementById('prerollSponsorName').value = cfg.sponsorName || '';
    document.getElementById('prerollClickUrl').value = cfg.clickUrl || '';
    document.getElementById('prerollCategory').value = cfg.category || '';
    document.getElementById('prerollCpm').value = cfg.cpm ? String(cfg.cpm) : '';
    _prerollMediaUrl = cfg.mediaUrl || null;
    _prerollContentType = cfg.contentType || null;
    document.getElementById('prerollFileName').textContent = cfg.mediaUrl ? 'current ad uploaded ✓' : '';
    renderPrerollStats(data.stats || {}, cfg);
    loadHostLedger();
  } catch(e) {}
}

// The host's rev-share ledger: per-creator views × CPM × share %, plus the 2% network fee.
async function loadHostLedger() {
  try {
    const led = await fetch('/admin/ads-ledger', { headers: { 'Authorization': 'Bearer ' + token } }).then(r => r.json());
    if (!led || !led.rows) return;
    const box = document.getElementById('hostLedger');
    box.style.display = 'block';
    const row = (l, r, strong) => \`<div style="display:flex;justify-content:space-between;gap:8px;font-size:13px;padding:4px 0;\${strong ? 'font-weight:700;border-top:1px solid rgba(255,255,255,0.12);margin-top:4px;padding-top:8px' : ''}"><span style="min-width:0;overflow:hidden;text-overflow:ellipsis">\${l}</span><span style="white-space:nowrap">\${r}</span></div>\`;
    box.innerHTML =
      '<div style="font-size:11px;color:rgba(255,255,255,0.45);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:6px">Revenue ledger (lifetime)</div>' +
      led.rows.map(r => row(esc(r.host), r.impressions + ' views · ' + r.sharePct + '% → $' + r.owed.toFixed(2))).join('') +
      row('Gross (all creators)', '$' + led.totals.gross.toFixed(2)) +
      row('Owed to creators', '−$' + led.totals.owedToCreators.toFixed(2)) +
      row('Network fee (2%)', '−$' + led.totals.networkFee.toFixed(2)) +
      row('Your net', '$' + led.totals.hostNet.toFixed(2), true);
  } catch(e) {}
}

// Hosted creator's read-only earnings view.
async function openEarnings() {
  document.getElementById('earningsSheet').classList.add('show');
  const el = document.getElementById('earningsBody');
  el.textContent = 'Loading…';
  try {
    const d = await fetch('/admin/my-earnings', { headers: { 'Authorization': 'Bearer ' + token } }).then(r => r.json());
    el.innerHTML =
      '<div style="font-size:26px;font-weight:800;line-height:1">$' + (d.owed || 0).toFixed(2) + '</div>' +
      '<div style="font-size:11px;color:rgba(255,255,255,0.45);text-transform:uppercase;letter-spacing:0.06em;margin-top:2px">owed to you (' + (d.sharePct || 0) + '% share)</div>' +
      '<div style="font-size:13px;color:rgba(255,255,255,0.6);margin-top:10px">' + (d.impressions || 0) + ' ad views on your content · ' + (d.clicks || 0) + ' clicks</div>';
  } catch(e) { el.textContent = 'Could not load earnings'; }
}

function renderPrerollStats(s, cfg) {
  const imp = s.impressions || 0, clicks = s.clicks || 0;
  const earnings = s.earnings != null ? s.earnings : (imp / 1000) * (cfg.cpm || 0);
  const ctr = imp ? ((clicks / imp) * 100).toFixed(1) + '%' : '—';
  document.getElementById('prerollStats').innerHTML =
    '<div style="font-size:26px;font-weight:800;color:#fff;line-height:1">$' + (earnings || 0).toFixed(2) + '</div>' +
    '<div style="font-size:11px;color:rgba(255,255,255,0.45);text-transform:uppercase;letter-spacing:0.06em;margin-top:2px">estimated earnings</div>' +
    '<div style="font-size:13px;color:rgba(255,255,255,0.6);margin-top:10px">' + imp + ' views · ' + clicks + ' clicks · ' + ctr + ' CTR</div>';
}

function closePrerollSheet() {
  document.getElementById('prerollSheet').classList.remove('show');
}

async function onPrerollFilePick(input) {
  const file = input.files[0];
  if (!file) return;
  const btn = document.getElementById('prerollSaveBtn');
  btn.disabled = true; btn.textContent = 'Uploading…';
  document.getElementById('prerollFileName').textContent = file.name;
  try {
    const r = await uploadFile(file);
    _prerollMediaUrl = r.url; _prerollContentType = r.contentType;
    document.getElementById('prerollFileName').textContent = file.name + ' ✓';
  } catch(e) {
    showToast('Upload failed');
    document.getElementById('prerollFileName').textContent = '';
  }
  btn.disabled = false; btn.textContent = 'Save';
}

async function savePreroll() {
  const enabled = document.getElementById('prerollEnabled').checked;
  if (enabled && !_prerollMediaUrl) { showToast('Upload an ad video first'); return; }
  const btn = document.getElementById('prerollSaveBtn');
  btn.disabled = true; btn.textContent = 'Saving…';
  try {
    const res = await fetch('/admin/preroll', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
      body: JSON.stringify({
        enabled,
        mediaUrl: _prerollMediaUrl,
        contentType: _prerollContentType,
        sponsorName: document.getElementById('prerollSponsorName').value.trim(),
        clickUrl: document.getElementById('prerollClickUrl').value.trim(),
        category: document.getElementById('prerollCategory').value.trim(),
        cpm: document.getElementById('prerollCpm').value.trim(),
        durationSec: 15,
      }),
    });
    if (!res.ok) throw new Error('save failed');
    showToast(enabled ? 'Pre-roll ad on' : 'Pre-roll ad saved');
    closePrerollSheet();
  } catch(e) { showToast('Error: ' + e.message); }
  btn.disabled = false; btn.textContent = 'Save';
}

// ── INBOX ─────────────────────────────────────────────────────
async function onInboxTap() {
  if (!isCreator) { onCreateTap(); return; }
  document.getElementById('inboxSheet').classList.add('show');
  pauseFeedVideos();
  document.getElementById('inboxList').innerHTML =
    '<div style="display:flex;align-items:center;justify-content:center;padding:60px;color:rgba(255,255,255,0.3)">Loading…</div>';
  try {
    const res = await fetch('/admin/inbox.json', { headers: { 'Authorization': 'Bearer ' + token } });
    const data = await res.json();
    renderInbox(data.notifications || []);
    updateInboxBadge(0);
  } catch(e) {
    document.getElementById('inboxList').innerHTML =
      '<div style="padding:40px;text-align:center;color:rgba(255,255,255,0.3)">Could not load inbox</div>';
  }
}

function closeInbox() {
  document.getElementById('inboxSheet').classList.remove('show');
  resumeFeedVideos();
}

function renderInbox(notifs) {
  if (!notifs.length) {
    document.getElementById('inboxList').innerHTML =
      '<div class="inbox-empty"><div class="inbox-empty-icon">💬</div>No notifications yet</div>';
    return;
  }
  document.getElementById('inboxList').innerHTML = notifs.map(n => {
    const handle = n.subdomain || '';
    const grad = avatarGrad(n.subdomain || '');
    const dot = n.read ? '' : '<div class="inbox-unread-dot"></div>';
    let text = '';
    if (n.type === 'peer_connected') text = \`<strong>@\${esc(handle)}</strong> connected to your node\`;
    else if (n.type === 'join_request') text = \`<strong>@\${esc(handle)}</strong> wants to join your network\`;
    else if (n.type === 'report') {
      const rl = { csam: 'Child sexual abuse material', ncii: 'Non-consensual intimate images', hate: 'Hate speech / incitement', harassment: 'Harassment or threats', copyright: 'Copyright infringement', defamation: 'Defamation', other: 'Other' };
      text = \`<strong style="color:#FE2C55">⚑ Content report</strong> on <strong>@\${esc(handle)}</strong> — \${rl[n.reason] || 'Other'}\`;
    }
    else text = esc(n.type);
    let actions = '';
    if (n.type === 'report') {
      const det = n.details ? \`<div style="margin-top:4px;font-size:12px;color:rgba(255,255,255,0.55)">“\${esc(n.details)}”</div>\` : '';
      const duty = n.reason === 'csam'
        ? '<div style="margin-top:6px;font-size:12px;color:#FE2C55">If real: remove it, preserve evidence, and report to Cybertip.ca — a legal duty for the operator.</div>'
        : n.reason === 'ncii' ? '<div style="margin-top:6px;font-size:12px;color:#FE2C55">Remove fast — criminal and provincial takedown laws apply.</div>' : '';
      const view = n.postId ? \`<a href="https://\${esc(n.subdomain)}/p/\${esc(n.postId)}" target="_blank" onclick="event.stopPropagation()" style="display:inline-block;margin-top:6px;font-size:13px;color:#20D5EC">View reported post →</a>\` : '';
      actions = det + duty + view;
    }
    if (n.type === 'join_request') {
      if (n.resolved === 'approved') actions = '<div style="margin-top:6px;font-size:13px;color:#20D5EC">✓ Added to the network</div>';
      else if (n.resolved === 'denied') actions = '<div style="margin-top:6px;font-size:13px;color:rgba(255,255,255,0.4)">Request denied</div>';
      else actions = \`<div style="display:flex;gap:8px;margin-top:8px">
      <button onclick="event.stopPropagation();approveJoin('\${esc(n.publicKey)}','\${esc(n.subdomain)}')" style="background:#FE2C55;border:none;color:#fff;border-radius:8px;padding:6px 14px;font-size:13px;font-weight:600">Approve</button>
      <button onclick="event.stopPropagation();denyJoin('\${esc(n.publicKey)}')" style="background:rgba(255,255,255,0.12);border:none;color:#fff;border-radius:8px;padding:6px 14px;font-size:13px">Deny</button>
    </div>\`;
    }
    return \`<div class="inbox-notif\${n.read ? '' : ' unread'}" onclick="openProfile('\${esc(n.subdomain)}')">
      <div class="inbox-notif-avatar" style="background:\${grad}">\${esc(handle[0]?.toUpperCase() || '?')}</div>
      <div class="inbox-notif-body">
        <div class="inbox-notif-text">\${text}</div>
        <div class="inbox-notif-time">\${timeAgo(n.at)}</div>
        \${actions}
      </div>
      \${dot}
    </div>\`;
  }).join('');
}

async function approveJoin(pubkey, subdomain) {
  try {
    const r = await fetch('/admin/registry/approve', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
      body: JSON.stringify({ pubkey, subdomain }),
    });
    if (!r.ok) throw new Error('failed');
    showToast('@' + (subdomain || '') + ' added to the network');
    // add to the live graph immediately
    if (subdomain && !nodeGraph.find(n => n.subdomain === subdomain)) {
      nodeGraph.push({ subdomain, url: 'https://' + subdomain, feed: null, postIndex: 0, loaded: false, loading: false });
    }
    onInboxTap();
  } catch(e) { showToast('Approve failed'); }
}

async function denyJoin(pubkey) {
  try {
    await fetch('/admin/registry/deny', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
      body: JSON.stringify({ pubkey }),
    });
    showToast('Request denied');
    onInboxTap();
  } catch(e) {}
}

async function markInboxRead() {
  await fetch('/admin/inbox/read', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token } }).catch(() => {});
  document.querySelectorAll('.inbox-notif').forEach(el => el.classList.remove('unread'));
  document.querySelectorAll('.inbox-unread-dot').forEach(el => el.remove());
  updateInboxBadge(0);
}

function updateInboxBadge(count) {
  const badge = document.getElementById('inboxBadge');
  if (!badge) return;
  if (count > 0) { badge.textContent = count > 9 ? '9+' : count; badge.classList.add('show'); }
  else { badge.classList.remove('show'); }
}

async function pollInboxBadge() {
  if (!isCreator) return;
  try {
    const res = await fetch('/admin/inbox.json', { headers: { 'Authorization': 'Bearer ' + token } });
    const data = await res.json();
    updateInboxBadge(data.unread || 0);
  } catch(e) {}
}

function timeAgo(iso) {
  if (!iso) return '';
  const diff = Date.now() - new Date(iso).getTime();
  const m = Math.floor(diff / 60000);
  if (m < 1) return 'just now';
  if (m < 60) return m + 'm ago';
  const h = Math.floor(m / 60);
  if (h < 24) return h + 'h ago';
  return Math.floor(h / 24) + 'd ago';
}

// ── SEARCH ────────────────────────────────────────────────────
function onSearchTap() {
  document.getElementById('searchModal').classList.add('show');
  document.getElementById('searchInput').value = '';
  renderSearchResults('');
  setTimeout(() => document.getElementById('searchInput').focus(), 100);
}

function onSearchInput(q) {
  renderSearchResults(q.trim().toLowerCase());
}

function renderSearchResults(q) {
  const box = document.getElementById('searchResults');
  const matches = nodeGraph.filter(n =>
    !q || n.subdomain.toLowerCase().includes(q)
  );
  if (matches.length === 0 && q) {
    box.innerHTML = \`<div style="text-align:center;padding:20px;color:rgba(255,255,255,0.4)">
      No nodes found.<br>
      <button class="btn-secondary" style="margin-top:12px" onclick="addNodeFromSearch()">Add "\${esc(q)}"</button>
    </div>\`;
    return;
  }
  box.innerHTML = matches.map((n, i) => {
    const grad   = avatarGrad(n.subdomain);
    const letter = ((n.displayName || n.subdomain)[0] || '?').toUpperCase();
    const avInner = n.avatarUrl
      ? \`<img src="\${esc(n.avatarUrl)}" alt="" style="width:100%;height:100%;object-fit:cover;border-radius:50%">\`
      : esc(letter);
    return \`<div onclick="goToNode('\${esc(n.subdomain)}')" style="display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid rgba(255,255,255,0.08);cursor:pointer">
      <div style="width:42px;height:42px;border-radius:50%;background:\${grad};display:flex;align-items:center;justify-content:center;font-weight:700;font-size:16px;flex-shrink:0;overflow:hidden">\${avInner}</div>
      <div>
        <div style="font-weight:600">@\${esc(n.subdomain)}</div>
        \${n.displayName ? \`<div style="font-size:12px;color:rgba(255,255,255,0.5)">\${esc(n.displayName)}</div>\` : ''}
      </div>
    </div>\`;
  }).join('');
}

function goToNode(subdomain) {
  const idx = nodeGraph.findIndex(n => n.subdomain === subdomain);
  if (idx >= 0) {
    nodeIndex = idx; renderCurrent(); updateIndicators();
    document.getElementById('searchModal').classList.remove('show');
  }
}

function addNodeFromSearch() {
  const q = document.getElementById('searchInput').value.trim();
  if (!q) return;
  const subdomain = q.includes('.') ? q : q + '.workers.dev';
  if (nodeGraph.find(n => n.subdomain === subdomain)) { goToNode(subdomain); return; }
  nodeGraph.push({ subdomain, url: 'https://' + subdomain, feed: null, postIndex: 0, loaded: false, loading: false });
  goToNode(subdomain);
}

function closeUnlock() { document.getElementById('unlockModal').classList.remove('show'); }

async function submitToken() {
  const val = document.getElementById('tokenInput').value.trim();
  if (!val) return;
  try {
    const res = await fetch('/admin/verify', { method:'POST', headers:{'Authorization':'Bearer '+val} });
    if (res.ok) {
      const d = await res.json().catch(() => ({})); isHost = !!d.master;
      token = val; localStorage.setItem('adminToken', val);
      enableCreator(); closeUnlock(); toast('creator mode on');
    } else { toast('invalid token'); }
  } catch(e) { toast('failed'); }
}

function toggleClaim() {
  const f = document.getElementById('claimForm');
  f.style.display = f.style.display === 'none' ? 'block' : 'none';
  document.getElementById('tokenForm').style.display = 'none';
}
function toggleTokenUnlock() {
  const f = document.getElementById('tokenForm');
  f.style.display = f.style.display === 'none' ? 'block' : 'none';
  document.getElementById('claimForm').style.display = 'none';
}
// Password login → signed session (stored in the same slot the token used; checkAuth accepts both)
async function submitLogin() {
  const password = document.getElementById('pwInput').value;
  if (!password) return;
  try {
    const res = await fetch('/auth/login', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ password }) });
    const d = await res.json().catch(() => ({}));
    if (res.ok && d.session) {
      token = d.session; localStorage.setItem('adminToken', d.session);
      enableCreator(); closeUnlock(); toast('logged in');
    } else { toast(d.error || 'login failed'); }
  } catch(e) { toast('login failed'); }
}
// First-time claim: redeem the host's code + set a password → signed session
async function submitClaim() {
  const code = document.getElementById('claimCode').value.trim();
  const password = document.getElementById('claimPw').value;
  if (!code || !password) return;
  try {
    const res = await fetch('/auth/claim', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code, password }) });
    const d = await res.json().catch(() => ({}));
    if (res.ok && d.session) {
      token = d.session; localStorage.setItem('adminToken', d.session);
      enableCreator(); closeUnlock(); toast('account claimed');
    } else { toast(d.error || 'claim failed'); }
  } catch(e) { toast('claim failed'); }
}

async function restoreCreator() {
  if (!token) return;
  try {
    const res = await fetch('/admin/verify', { method:'POST', headers:{'Authorization':'Bearer '+token} });
    if (res.ok) { const d = await res.json().catch(() => ({})); isHost = !!d.master; enableCreator(); }
    else { localStorage.removeItem('adminToken'); token = ''; }
  } catch(e) {}
}

function enableCreator() { isCreator = true; document.body.classList.add('creator'); refreshJoinButton(); }

// ── HOST: manage creators (master only) ───────────────────────
let _lastCreatorLink = '';
function closeCreators() { document.getElementById('creatorsModal').classList.remove('show'); }
function creatorBaseDomain() { const p = SELF_SUBDOMAIN.split('.'); return p.length > 2 ? p.slice(1).join('.') : SELF_SUBDOMAIN; }
async function openCreators() {
  if (!isHost) { toast('host master token required'); return; }
  document.getElementById('creatorLinkBox').style.display = 'none';
  document.getElementById('newCreatorHandle').value = '';
  document.getElementById('creatorsModal').classList.add('show');
  loadCreatorList();
}
async function loadCreatorList() {
  try {
    const d = await fetch('/admin/provisioned', { headers: { 'Authorization': 'Bearer ' + token } }).then(r => r.json());
    const list = (d.provisioned || []).filter(h => h !== SELF_SUBDOMAIN);
    const ads = d.ads || {};
    document.getElementById('creatorList').innerHTML = list.length
      ? '<div style="font-size:12px;color:rgba(255,255,255,0.5);margin-bottom:8px">Creators — "Share %" is their cut of ad revenue earned on their content</div>' + list.map(h =>
          \`<div style="display:flex;align-items:center;justify-content:space-between;gap:8px;padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.08)">
            <span style="font-size:13px;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis">\${esc(h)}</span>
            <button class="btn-secondary" style="padding:4px 10px;font-size:12px;\${ads[h] ? 'color:#20D5EC' : 'opacity:0.6'}" onclick="setCreatorShare('\${esc(h)}', \${ads[h] || 0})">Share \${ads[h] || 0}%</button>
            <button class="btn-secondary" style="padding:4px 10px;font-size:12px" onclick="issueCreatorLink('\${esc(h)}')">New link</button>
            <button class="btn-secondary" style="padding:4px 10px;font-size:12px;color:#FE2C55" onclick="removeCreator('\${esc(h)}')" title="Remove">✕</button>
          </div>\`).join('')
      : '';
  } catch(e) {}
}
async function setCreatorShare(host, current) {
  const v = prompt('Revenue share % for ' + host + ' (0–100, 0 = no share)', String(current || 0));
  if (v === null) return;
  const pct = Math.max(0, Math.min(parseFloat(v) || 0, 100));
  try {
    await fetch('/admin/ads', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ host, sharePct: pct }) });
    toast(host + ' → ' + pct + '% share');
    loadCreatorList();
  } catch(e) { toast('failed'); }
}
async function removeCreator(host) {
  if (!confirm('Remove ' + host + ' from this node? It will stop serving. Its content stays in storage — re-adding the handle restores it.')) return;
  try {
    const d = await fetch('/admin/unprovision', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ host }) }).then(r => r.json());
    if (d.ok) { toast(host + ' removed'); loadCreatorList(); } else toast(d.error || 'failed');
  } catch(e) { toast('failed'); }
}
async function addCreator() {
  const handle = document.getElementById('newCreatorHandle').value.trim().toLowerCase();
  if (!handle) return;
  const host = handle.includes('.') ? handle : handle + '.' + creatorBaseDomain();
  await issueCreatorLink(host);
  loadCreatorList();
}
async function issueCreatorLink(host) {
  try {
    const hdr = { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' };
    await fetch('/admin/provision', { method:'POST', headers: hdr, body: JSON.stringify({ host }) });
    const d = await fetch('/admin/creator/mint-claim', { method:'POST', headers: hdr, body: JSON.stringify({ host }) }).then(r => r.json());
    if (d.claimCode) {
      _lastCreatorLink = 'https://' + host + '/?claim=' + d.claimCode;
      document.getElementById('creatorLink').textContent = _lastCreatorLink;
      document.getElementById('creatorLinkBox').style.display = 'block';
      toast('invite link ready');
    } else toast(d.error || 'failed');
  } catch(e) { toast('failed'); }
}
function copyCreatorLink() {
  (navigator.clipboard ? navigator.clipboard.writeText(_lastCreatorLink) : Promise.reject())
    .then(() => toast('link copied')).catch(() => toast('copy this link manually'));
}

// ── PUBLISH MODAL ─────────────────────────────────────────────
function closePublish() {
  document.getElementById('publishModal').classList.remove('show');
  document.getElementById('titleInput').value = '';
  document.getElementById('bodyInput').value  = '';
  document.querySelectorAll('#typePicker button').forEach(x => x.classList.remove('active'));
  document.querySelector('#typePicker button[data-type="writing"]').classList.add('active');
  selectedType = 'writing'; updateModalForType();
}

document.querySelectorAll('#typePicker button').forEach(b => {
  b.addEventListener('click', () => {
    document.querySelectorAll('#typePicker button').forEach(x => x.classList.remove('active'));
    b.classList.add('active'); selectedType = b.dataset.type; updateModalForType();
  });
});

function updateModalForType() {
  const fileDrop  = document.getElementById('fileDrop');
  const fileLabel = document.getElementById('fileLabel');
  const fileInput = document.getElementById('fileInput');
  const bodyLabel = document.getElementById('bodyLabel');
  if (selectedType === 'writing') {
    fileDrop.classList.remove('visible'); fileLabel.style.display = 'none';
    bodyLabel.textContent = 'Caption'; fileInput.accept = 'image/*,video/*';
  } else if (selectedType === 'photo') {
    fileDrop.classList.add('visible'); fileLabel.style.display = 'block';
    document.getElementById('fileDropIcon').textContent = '📷';
    fileInput.accept = 'image/*'; bodyLabel.textContent = 'Caption';
  } else {
    fileDrop.classList.add('visible'); fileLabel.style.display = 'block';
    document.getElementById('fileDropIcon').textContent = '🎬';
    fileInput.accept = 'video/*'; bodyLabel.textContent = 'Caption';
  }
  clearFile();
}

document.getElementById('fileInput').addEventListener('change', e => {
  const file = e.target.files[0]; if (!file) return;
  selectedFile = file; uploadedMediaUrl = null; uploadedMediaContentType = null;
  document.getElementById('fileDropName').textContent = file.name;
  document.getElementById('fileDrop').classList.add('has-file');
  showFilePreview(file);
});

function showFilePreview(file) {
  const preview = document.getElementById('filePreview'); preview.innerHTML = '';
  const url = URL.createObjectURL(file);
  if (file.type.startsWith('image/')) {
    const img = document.createElement('img'); img.src = url; img.style.maxHeight = '240px'; preview.appendChild(img);
  } else if (file.type.startsWith('video/')) {
    const vid = document.createElement('video'); vid.src = url; vid.muted = true; vid.controls = true; vid.style.maxHeight = '240px'; preview.appendChild(vid);
  }
  preview.classList.add('visible');
}

function clearFile() {
  selectedFile = null; uploadedMediaUrl = null; uploadedMediaContentType = null;
  document.getElementById('fileInput').value = '';
  document.getElementById('fileDropName').textContent = '';
  document.getElementById('fileDrop').classList.remove('has-file');
  document.getElementById('filePreview').classList.remove('visible');
  document.getElementById('filePreview').innerHTML = '';
  document.getElementById('uploadProgress').classList.remove('visible');
  document.getElementById('progressBar').style.width = '0%';
}

async function uploadFile(file) {
  const prog = document.getElementById('uploadProgress');
  const bar  = document.getElementById('progressBar');
  const lbl  = document.getElementById('progressLabel');
  prog.classList.add('visible'); bar.style.width = '0%'; lbl.textContent = 'uploading…';
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/admin/upload');
    xhr.setRequestHeader('Authorization', 'Bearer '+token);
    xhr.upload.addEventListener('progress', e => {
      if (e.lengthComputable) { const p = Math.round(e.loaded/e.total*100); bar.style.width=p+'%'; lbl.textContent='uploading… '+p+'%'; }
    });
    xhr.addEventListener('load', () => {
      if (xhr.status === 200) { bar.style.width='100%'; lbl.textContent='done'; resolve(JSON.parse(xhr.responseText)); }
      else reject(new Error('upload failed: '+xhr.status));
    });
    xhr.addEventListener('error', () => reject(new Error('network error')));
    const form = new FormData(); form.append('file', file); xhr.send(form);
  });
}

async function submitPublish() {
  const title = document.getElementById('titleInput').value.trim();
  const body  = document.getElementById('bodyInput').value.trim();
  const btn   = document.getElementById('publishBtn');
  if ((selectedType==='photo'||selectedType==='video') && !selectedFile && !uploadedMediaUrl) { toast('choose a file first'); return; }
  if (selectedType==='writing' && !title && !body) { toast('write something'); return; }
  btn.disabled = true;
  try {
    if (selectedFile && !uploadedMediaUrl) {
      const r = await uploadFile(selectedFile); uploadedMediaUrl = r.url; uploadedMediaContentType = r.contentType;
    }
    const res = await fetch('/admin/publish', {
      method: 'POST',
      headers: { 'Content-Type':'application/json', 'Authorization':'Bearer '+token },
      body: JSON.stringify({ type:selectedType, title, body, mediaUrl:uploadedMediaUrl||null, mediaContentType:uploadedMediaContentType||null }),
    });
    if (res.ok) {
      closePublish();
      nodeGraph[0].loaded = false; nodeGraph[0].feed = null;
      await loadNode(nodeGraph[0]);
      if (nodeIndex === 0) renderCurrent();
      updateIndicators(); toast('posted');
    } else { toast('post failed'); }
  } catch(e) { toast(e.message||'error'); }
  btn.disabled = false;
}

// ── DELETE ────────────────────────────────────────────────────
async function deleteItem(id) {
  if (!confirm('delete?')) return;
  try {
    const res = await fetch('/admin/delete', {
      method:'POST', headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},
      body: JSON.stringify({ id }),
    });
    if (res.ok) {
      nodeGraph[0].loaded = false; nodeGraph[0].feed = null;
      await loadNode(nodeGraph[0]);
      const n = nodeGraph[0];
      if (n.postIndex >= (n.feed?.length||0)) n.postIndex = Math.max(0,(n.feed?.length||1)-1);
      if (nodeIndex === 0) renderCurrent();
      updateIndicators(); toast('deleted');
    }
  } catch(e) { toast('failed'); }
}

// Delete from the profile grid (creator's own profile) + refresh grid and feed.
async function deleteFromGrid(id) {
  if (!confirm('Delete this post?')) return;
  try {
    const res = await fetch('/admin/delete', {
      method:'POST', headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},
      body: JSON.stringify({ id }),
    });
    if (!res.ok) { toast('delete failed'); return; }
    // invalidate caches so the feed and grid both reflect the deletion
    nodeGraph[0].loaded = false; nodeGraph[0].feed = null;
    delete _profileCache[SELF_SUBDOMAIN];
    await loadNode(nodeGraph[0]);
    const n = nodeGraph[0];
    if (n.postIndex >= (n.feed?.length||0)) n.postIndex = Math.max(0,(n.feed?.length||1)-1);
    if (nodeIndex === 0) renderCurrent();
    updateIndicators();
    const data = await loadProfileData(SELF_SUBDOMAIN);
    renderProfileBody(data, SELF_SUBDOMAIN);
    toast('deleted');
  } catch(e) { toast('failed'); }
}

// ── GESTURE ENGINE ────────────────────────────────────────────
const LOCK_PX     = 6;
const COMMIT_FRAC = 0.15;

let G = {
  active: false, startX: 0, startY: 0, x: 0, y: 0,
  axis: null, dir: null, prepared: false,
  nextNodeIdx: 0, nextPostIdx: 0,
};

stage.addEventListener('touchstart',  onTouchStart,  { passive: true  });
stage.addEventListener('touchmove',   onTouchMove,   { passive: false });
stage.addEventListener('touchend',    onTouchEnd,    { passive: true  });
stage.addEventListener('touchcancel', cancelGesture, { passive: true  });

function onTouchStart(e) {
  if (e.touches.length !== 1) return;
  const t = e.touches[0];
  G = {
    active: true,
    startX: t.clientX, startY: t.clientY,
    x: t.clientX, y: t.clientY,
    axis: null, dir: null, prepared: false,
    nextNodeIdx: nodeIndex, nextPostIdx: nodeGraph[nodeIndex].postIndex,
  };
}

function onTouchMove(e) {
  if (!G.active || e.touches.length !== 1) return;
  const t = e.touches[0];
  G.x = t.clientX; G.y = t.clientY;

  const dx = G.x - G.startX;
  const dy = G.y - G.startY;

  // lock axis
  if (!G.axis) {
    if (Math.abs(dx) < LOCK_PX && Math.abs(dy) < LOCK_PX) return;
    G.axis = Math.abs(dx) > Math.abs(dy) ? 'x' : 'y';
  }

  e.preventDefault(); // stop browser scroll

  if (!G.prepared) prepareIncoming(dx, dy);
  if (G.dir) applyDrag(dx, dy);
}

function onTouchEnd() {
  if (!G.active) return;
  G.active = false;

  const dx = G.x - G.startX;
  const dy = G.y - G.startY;
  const W  = window.innerWidth;
  const H  = window.innerHeight;

  if (!G.dir || !G.prepared) { cancelGesture(); return; }

  const committed = G.axis === 'y'
    ? Math.abs(dy) > H * COMMIT_FRAC
    : Math.abs(dx) > W * COMMIT_FRAC;

  committed ? commitSwipe() : cancelGesture();
}

let _caughtUpToasted = false; // "all caught up" nudge — once per session
// Viewing history (this session): every post you swipe AWAY from, in order. Down-swipe
// walks back through it like a browser back button — across creators, exact posts.
const _viewHistory = [];

function prepareIncoming(dx, dy) {
  const node     = nodeGraph[nodeIndex];
  const feedLen  = node.feed?.length || 0;
  let dir = null, nextNI = nodeIndex, nextPI = node.postIndex;

  if (G.axis === 'y') {
    if (dy < 0 && nodeGraph.length > 1) {
      // forward swipe is UNSEEN-SEEKING: the next creator (wrapping past the end) who
      // still has something you haven't watched; fully-drained creators are skipped.
      let target = -1;
      for (let k = 1; k < nodeGraph.length; k++) {
        const cand = (nodeIndex + k) % nodeGraph.length;
        if (hasUnseen(nodeGraph[cand])) { target = cand; break; }
      }
      if (target === -1) {
        // nothing new anywhere — fall back to plain next/wrap so the feed never dead-ends
        target = (nodeIndex + 1) % nodeGraph.length;
        if (!_caughtUpToasted) { _caughtUpToasted = true; toast("You're all caught up — nothing new right now"); }
      }
      dir = 'up'; nextNI = target; nextPI = landingIndex(nodeGraph[target]);
    }
    else if (dy > 0) {
      // back through VIEWING HISTORY — the exact post you came from, even if seen.
      // Stale entries (blocked/unreachable creators) are skipped and dropped on commit.
      for (let i = _viewHistory.length - 1; i >= 0; i--) {
        const h = _viewHistory[i];
        const ni = nodeGraph.findIndex(n => n.subdomain === h.sub);
        if (ni < 0 || !nodeGraph[ni].feed || !nodeGraph[ni].feed.length) continue;
        if (ni === nodeIndex && Math.min(h.pi, nodeGraph[ni].feed.length - 1) === node.postIndex) continue; // already here
        dir = 'down'; nextNI = ni;
        nextPI = Math.min(h.pi, nodeGraph[ni].feed.length - 1);
        G.histIdx = i;
        break;
      }
      // no usable history → boundary rubber-band (dir stays null)
    }
  } else {
    if      (dx < 0 && node.postIndex < feedLen - 1) { dir = 'left';  nextPI = node.postIndex + 1; }
    else if (dx > 0 && node.postIndex > 0)            { dir = 'right'; nextPI = node.postIndex - 1; }
  }

  G.prepared    = true;
  G.dir         = dir;
  G.nextNodeIdx = nextNI;
  G.nextPostIdx = nextPI;

  if (!dir) return; // at boundary — rubber band only

  // Load target node if needed
  const targetNode = nodeGraph[nextNI];
  if (!targetNode.loaded && !targetNode.loading) {
    loadNode(targetNode).then(() => {
      // refresh inactive panel if gesture still live — and re-aim at the first unseen
      // post now that the feed (and seen-state) is actually known
      if (G.active && G.dir === dir) {
        G.nextPostIdx = landingIndex(targetNode);
        inactivePanel().innerHTML = renderCard(targetNode, G.nextPostIdx);
      }
    });
  }

  // Render target into inactive panel and position off-screen
  inactivePanel().innerHTML        = renderCard(targetNode, nextPI);
  inactivePanel().style.transition = 'none';
  inactivePanel().style.transform  = offscreenTransform(dir);
  inactivePanel().style.zIndex     = '3'; // incoming sits above outgoing during animation
}

function offscreenTransform(dir) {
  const W = window.innerWidth, H = window.innerHeight;
  if (dir === 'up')    return \`translateY(\${H}px)\`;
  if (dir === 'down')  return \`translateY(\${-H}px)\`;
  if (dir === 'left')  return \`translateX(\${W}px)\`;
  if (dir === 'right') return \`translateX(\${-W}px)\`;
  return '';
}

function applyDrag(dx, dy) {
  const W = window.innerWidth, H = window.innerHeight;
  // If at boundary (no dir), rubber-band in place
  const factor = G.dir ? 1 : 0.18;

  let curX = 0, curY = 0, inX = 0, inY = 0;
  if (G.axis === 'y') {
    curY = dy * factor;
    inY  = (G.dir === 'up' ? H : -H) + dy * factor;
  } else {
    curX = dx * factor;
    inX  = (G.dir === 'left' ? W : -W) + dx * factor;
  }

  activePanel().style.transition  = 'none';
  activePanel().style.transform   = \`translate(\${curX}px,\${curY}px)\`;
  inactivePanel().style.transition = 'none';
  inactivePanel().style.transform  = \`translate(\${inX}px,\${inY}px)\`;
}

function commitSwipe() {
  const W = window.innerWidth, H = window.innerHeight;
  const dur = '0.28s cubic-bezier(0.25,0.46,0.45,0.94)';
  let exitX = 0, exitY = 0;
  if (G.dir === 'up')    exitY = -H;
  if (G.dir === 'down')  exitY =  H;
  if (G.dir === 'left')  exitX = -W;
  if (G.dir === 'right') exitX =  W;

  activePanel().style.transition  = \`transform \${dur}\`;
  inactivePanel().style.transition = \`transform \${dur}\`;
  activePanel().style.transform   = \`translate(\${exitX}px,\${exitY}px)\`;
  inactivePanel().style.transform = 'translate(0,0)';

  // commit state — only the DESTINATION node's postIndex may change. Writing it to the
  // outgoing node too (the old first line here) clobbered that node's position with the
  // target's on every vertical swipe, so swiping back always "forgot" where you were.
  const outNode = nodeGraph[nodeIndex];
  if (G.dir === 'down') {
    // consumed a history entry — truncate it (and any stale ones above it); going back
    // must not itself create history, or down-down would ping-pong between two posts
    _viewHistory.length = G.histIdx;
  } else if (outNode.feed && outNode.feed[outNode.postIndex]) {
    _viewHistory.push({ sub: outNode.subdomain, pi: outNode.postIndex });
    if (_viewHistory.length > 200) _viewHistory.shift();
  }
  nodeIndex = G.nextNodeIdx;
  nodeGraph[nodeIndex].postIndex = G.nextPostIdx;

  setTimeout(() => {
    deactivateVideos(activePanel());
    activePanelIdx = 1 - activePanelIdx;        // swap: incoming becomes active

    // New active panel (was incoming, z-index 3) → settle to 2
    activePanel().style.transition = 'none';
    activePanel().style.transform  = '';
    activePanel().style.zIndex     = '2';

    // Old active panel (was outgoing) → drop below, clear content
    inactivePanel().style.transition = 'none';
    inactivePanel().style.transform  = '';
    inactivePanel().style.zIndex     = '1';
    inactivePanel().innerHTML        = '';

    startDwell(nodeGraph[nodeIndex].subdomain);
    activateVideos(activePanel());
    refreshCurrentInteractions();
    preloadAdjacent();
    updateIndicators();
    maybeShowFeedAd();
    prefetchUpcoming();
    const cur = nodeGraph[nodeIndex];
    // every swiped-to post counts as seen — renderCurrent only covers boot/load paths,
    // so without this the unseen-tracking missed everything watched via swiping
    markSeen(cur.feed?.[cur.postIndex]?.id);
    if (cur.feed && cur.postIndex >= cur.feed.length - 5) loadMorePosts(cur);
  }, 290);
}

function cancelGesture() {
  if (!G.prepared || !G.dir) {
    // nothing was moved
    activePanel().style.transition = 'none';
    activePanel().style.transform  = '';
    return;
  }
  const dur = '0.28s cubic-bezier(0.25,0.46,0.45,0.94)';
  activePanel().style.transition   = \`transform \${dur}\`;
  inactivePanel().style.transition = \`transform \${dur}\`;
  activePanel().style.transform    = 'translate(0,0)';
  inactivePanel().style.transform  = offscreenTransform(G.dir);

  setTimeout(() => {
    inactivePanel().style.transition = 'none';
    inactivePanel().style.transform  = '';
    inactivePanel().style.zIndex     = '1'; // back below active
    inactivePanel().innerHTML        = '';
  }, 300);
}

// ── INIT ─────────────────────────────────────────────────────
// ── BACK-GESTURE NAVIGATION ──────────────────────────────────
// One history sentinel, re-armed after every pop: back-swipe closes the topmost
// overlay instead of the app. On the bare feed, back twice within 2s exits.
function closeTopOverlay() {
  const vis = id => { const el = document.getElementById(id); return el && el.classList.contains('show'); };
  const shut = id => document.getElementById(id).classList.remove('show');
  // modals that stack above everything (z 130)
  if (vis('liveViewersModal')) { closeLiveViewers(); return true; }
  if (vis('streamHistoryModal')) { closeStreamHistory(); return true; }
  if (vis('creatorsModal')) { closeCreators(); return true; }
  if (vis('importModal')) { closeImport(); return true; }
  if (vis('reportModal')) { closeReport(); return true; }
  if (vis('overlayModal')) { closeOverlaySheet(); return true; }
  if (vis('algoModal')) { closeAlgo(); return true; }
  if (vis('nameModal')) { closeNameModal(); return true; }
  if (document.getElementById('liveModal').style.display === 'flex') {
    if (vis('prerollSheet')) { closePrerollSheet(); return true; }
    // a stray edge-swipe must never end a running broadcast
    const endBtn = document.getElementById('liveBtnEnd');
    if (liveBroadcaster && endBtn && endBtn.style.display !== 'none') { toast('Use End Live to stop streaming'); return true; }
    closeLiveModal(); return true;
  }
  const order = [
    ['commentsSheet', closeComments], ['searchModal', () => shut('searchModal')],
    ['unlockModal', closeUnlock], ['publishModal', closePublish],
    ['prerollSheet', closePrerollSheet], ['earningsSheet', () => shut('earningsSheet')],
    ['inboxSheet', closeInbox], ['editProfileSheet', closeEditProfile],
  ];
  if (_selMode) { exitSelMode(); return true; } // back leaves select mode before closing the profile
  order.push(['profileSheet', closeProfile]);
  for (const pair of order) { if (vis(pair[0])) { pair[1](); return true; } }
  return false;
}
let _exitArmedAt = 0;
history.pushState({ sn: 1 }, '');
window.addEventListener('popstate', () => {
  if (closeTopOverlay()) { history.pushState({ sn: 1 }, ''); return; } // re-arm the sentinel
  // bare feed: first back warns, a second within 2s leaves naturally (sentinel consumed)
  _exitArmedAt = Date.now();
  toast('Swipe back again to exit');
  setTimeout(() => { if (Date.now() - _exitArmedAt >= 2000) history.pushState({ sn: 1 }, ''); }, 2100);
});

(async () => {
  await restoreCreator();
  await loadNode(nodeGraph[0]);
  // Permalink deep-link: served from /p/<id> — land on that post (overrides the
  // unseen-first index), then clean the URL back to / (keeps the back-nav sentinel).
  if (PERMALINK_POST && nodeGraph[0].feed) {
    const pi = nodeGraph[0].feed.findIndex(p => p && p.id === PERMALINK_POST);
    if (pi >= 0) nodeGraph[0].postIndex = pi;
    history.replaceState({ sn: 1 }, '', '/');
  }
  renderCurrent();
  updateIndicators();
  initNetwork();
  preloadAdjacent();
  // First visit: show how to navigate, once (pointer-events:none, auto-dismisses)
  if (!localStorage.getItem('hintSeen')) {
    localStorage.setItem('hintSeen', '1');
    const hint = document.getElementById('swipeHint');
    hint.style.display = 'flex';
    const hide = () => { hint.style.display = 'none'; };
    setTimeout(hide, 3500);
    stage.addEventListener('touchstart', hide, { once: true });
  }
  // Keep the current creator's live state fresh so the "tap to watch" badge appears/disappears
  setInterval(() => {
    const n = nodeGraph[nodeIndex];
    if (!n || !n.loaded) return;
    fetch((n.url || '') + '/live/status.json').then(r => r.json()).then(s => {
      const was = !!(n.liveStatus && n.liveStatus.active);
      n.liveStatus = s;
      if (was !== !!(s && s.active)) {
        // never re-render (and start card videos) under the live overlay — defer to close
        if (feedCovered()) _pendingLiveRerender = true;
        else renderCurrent();
      }
    }).catch(() => {});
  }, 60000);
  if (isCreator) {
    pollInboxBadge();
    setInterval(pollInboxBadge, 60000);
  }
  // Invite deep-link: ?claim=CODE → open the claim form with the code prefilled (one-step onboarding)
  if (!isCreator) {
    const claim = new URLSearchParams(location.search).get('claim');
    if (claim) {
      document.getElementById('unlockModal').classList.add('show');
      document.getElementById('claimForm').style.display = 'block';
      document.getElementById('claimCode').value = claim;
      setTimeout(() => document.getElementById('claimPw').focus(), 150);
      history.replaceState(null, '', location.pathname); // strip the code from the URL bar
    }
  }
})();
</script>
</body>
</html>`;
}
