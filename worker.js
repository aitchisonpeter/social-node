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

const PROTOCOL_VERSION = '0.5.0';

// ============================================================
// NETWORK ROOT OF TRUST
// ------------------------------------------------------------
// The network is defined by a signed member registry served by the
// root node. Every node builds its graph from that registry, so the
// code can be forked but the *network* (the member set) cannot — a
// fork can't mint members that verify against this root.
// ============================================================
const NETWORK_ROOT_HOST   = 'social-node.aitchisonpeter.workers.dev';
const NETWORK_ROOT_PUBKEY = '4HAg90xqd7T8TfhXC3sYjEYIQlCGa7RPQod0x9L8oXk=';

export default {
  async fetch(request, env) {
    // Multi-tenant: each hostname is its own creator. State is keyed by hostname,
    // so one deploy hosts many creators (alex.host.com, bob.host.com, …).
    const tenant = new URL(request.url).hostname;
    if (tenant === NETWORK_ROOT_HOST) await migrateLegacyToRoot(env);
    const storage = makeStorage(env, tenant);
    return await handleRequest(request, storage, env);
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
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.serializeAttachment({ role, name });
      this.ctx.acceptWebSocket(server);
      try {
        server.send(JSON.stringify({
          t: 'init',
          status: await this.statusObj(),
          chat: await this.recentChat(),
          viewers: this.viewerCount(),
        }));
      } catch {}
      this.broadcastViewers();
      return new Response(null, { status: 101, webSocket: client });
    }

    if (seg === 'status') {
      return Response.json(await this.statusObj());
    }

    if (seg === 'publish' && request.method === 'POST') {
      const { publisherSessionId, trackNames, via } = await request.json().catch(() => ({}));
      await this.ctx.storage.put('session', {
        active: true,
        publisherSessionId: publisherSessionId || null,
        trackNames: trackNames || [],
        startedAt: new Date().toISOString(),
        heartbeatAt: Date.now(),
        via: via || 'browser',
      });
      await this.ctx.storage.put('chat', []); // fresh chat per stream
      this.broadcast({ t: 'live', status: await this.statusObj() });
      return Response.json({ ok: true });
    }

    if (seg === 'heartbeat' && request.method === 'POST') {
      const s = await this.ctx.storage.get('session');
      if (s && s.active) { s.heartbeatAt = Date.now(); await this.ctx.storage.put('session', s); }
      return Response.json({ ok: true });
    }

    if (seg === 'end' && request.method === 'POST') {
      const s = await this.ctx.storage.get('session');
      if (s) { s.active = false; s.endedAt = new Date().toISOString(); await this.ctx.storage.put('session', s); }
      this.broadcast({ t: 'ended' });
      return Response.json({ ok: true });
    }

    if (seg === 'chat' && request.method === 'POST') {
      const { text, name } = await request.json().catch(() => ({}));
      const msg = await this.addChat(text, name);
      return Response.json(msg ? { ok: true } : { error: 'empty' }, { status: msg ? 200 : 400 });
    }

    if (seg === 'chat') {
      return Response.json({ messages: await this.recentChat() });
    }

    // ---- pre-roll ad frequency cap + counters ----
    if (seg === 'ad-allowed') {
      return Response.json({ allowed: await this.adAllowed(url.searchParams.get('vid') || '') });
    }
    if (seg === 'ad-seen' && request.method === 'POST') {
      const { vid } = await request.json().catch(() => ({}));
      await this.adSeen(vid);
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

  async adSeen(vid) {
    if (!vid) return;
    const now = Date.now();
    const seen = (await this.ctx.storage.get('adseen')) || {};
    seen[vid] = now;
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
    return {
      active: !stale,
      publisherSessionId: s.publisherSessionId,
      trackNames: s.trackNames,
      startedAt: s.startedAt,
      via: s.via,
      viewers: this.viewerCount(),
    };
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

  async addChat(text, name) {
    if (!text || !String(text).trim()) return null;
    const msg = {
      id: crypto.randomUUID(),
      text: String(text).slice(0, 200),
      name: String(name || 'viewer').slice(0, 30),
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
    if (data.t === 'chat') {
      // light per-connection throttle to blunt spam (anonymous WS)
      if (!this._rate) this._rate = new WeakMap();
      const now = Date.now();
      const last = this._rate.get(ws) || 0;
      if (now - last < CHAT_MIN_GAP_MS) return;
      this._rate.set(ws, now);
      const att = ws.deserializeAttachment() || {};
      await this.addChat(data.text, data.name || att.name);
    }
  }

  async webSocketClose(ws, code, reason) {
    try { ws.close(code, reason); } catch {}
    this.broadcastViewers();
  }

  async webSocketError() {
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
async function getFeed(storage) { return (await storage.get('feed')) || []; }

async function addPeer(storage, peer) {
  const peers = await getPeers(storage);
  if (peers.find(p => p.publicKey === peer.publicKey)) return peers;
  peers.push({ ...peer, addedAt: new Date().toISOString() });
  await storage.set('peers', peers);
  return peers;
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

async function handleRequest(request, storage, env) {
  const url = new URL(request.url);
  const path = url.pathname;
  const subdomain = url.hostname;
  const gstore = makeStorage(env); // global namespace — for the network-wide registry only

  // CORS preflight
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  await ensureIdentity(storage);
  const identity = await storage.get('identity');

  // ---- PWA files ----
  if (path === '/' || path === '/index.html') {
    return html(renderApp({ identity, subdomain }));
  }
  if (path === '/manifest.json') {
    return json(renderManifest(subdomain));
  }
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

  // ---- media serving (public) ----
  if (path.startsWith('/media/') && request.method === 'GET') {
    if (!env.NODE_MEDIA) return new Response('R2 not configured', { status: 503 });
    const key = path.slice(7);
    if (!key) return new Response('not found', { status: 404 });
    const object = await env.NODE_MEDIA.get(key);
    if (!object) return new Response('not found', { status: 404 });
    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');
    return new Response(object.body, { headers });
  }

  // ---- protocol surface ----
  if (path === '/.well-known/node.json') {
    return json({
      protocol: PROTOCOL_VERSION,
      subdomain,
      publicKey: identity.publicKey,
      createdAt: identity.createdAt,
      ancestors: await getAncestors(storage),
    });
  }
  if (path === '/.well-known/feed.json') {
    const feed = await getFeed(storage);
    return json({ subdomain, publicKey: identity.publicKey, items: feed });
  }
  if (path === '/.well-known/peers.json') {
    return json({ subdomain, peers: await getPeers(storage) });
  }
  // signed member registry — the canonical network is whatever the root signs here
  if (path === '/.well-known/registry.json') {
    return jsonCors(await registrySigned(env, identity, subdomain));
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

  // ---- peer protocol ----
  if (path === '/protocol/announce' && request.method === 'POST') {
    const body = await request.json();
    const msg = JSON.parse(body.message);
    const valid = await verify(msg.publicKey, body.signature, body.message);
    if (!valid) return json({ error: 'invalid signature' }, 400);
    await addPeer(storage, { publicKey: msg.publicKey, subdomain: msg.subdomain });
    // inbox notification
    const notifs = (await storage.get('inbox:notifications')) || [];
    notifs.unshift({ id: crypto.randomUUID(), type: 'peer_connected', subdomain: msg.subdomain, publicKey: msg.publicKey, at: new Date().toISOString(), read: false });
    if (notifs.length > 100) notifs.splice(100);
    await storage.set('inbox:notifications', notifs);
    return json({
      welcome: true,
      ancestor: { publicKey: identity.publicKey, subdomain },
      peerCount: (await getPeers(storage)).length,
    });
  }

  if (path === '/protocol/peers' && request.method === 'GET') {
    const peers = await getPeers(storage);
    return json({ peers: peers.map(p => ({ publicKey: p.publicKey, subdomain: p.subdomain })) });
  }

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

  // ---- public profile ----
  if (path === '/profile.json') {
    const profile = (await storage.get('profile')) || {};
    const feed = await getFeed(storage);
    const peers = await getPeers(storage);
    return jsonCors({
      subdomain,
      displayName: profile.displayName || subdomain.split('.')[0],
      bio: profile.bio || '',
      avatarUrl: profile.avatarUrl || null,
      postCount: feed.length,
      peerCount: peers.length,
    });
  }

  // ---- admin: auth check ----
  function checkAuth(req) {
    const expected = env.ADMIN_TOKEN;
    if (!expected) return false; // fail closed: no token configured = no admin access
    const auth = req.headers.get('Authorization') || '';
    if (auth.length !== ('Bearer ' + expected).length) return false;
    // constant-time compare
    const a = new TextEncoder().encode(auth);
    const b = new TextEncoder().encode('Bearer ' + expected);
    let diff = 0;
    for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
    return diff === 0;
  }

  // ---- admin: verify token ----
  if (path === '/admin/verify' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ ok: false }, 401);
    return json({ ok: true });
  }

  // ---- admin: upload media to R2 ----
  if (path === '/admin/upload' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    if (!env.NODE_MEDIA) return json({ error: 'R2 bucket NODE_MEDIA not configured' }, 503);

    const formData = await request.formData();
    const file = formData.get('file');
    if (!file) return json({ error: 'no file provided' }, 400);

    const ext = (file.name || '').split('.').pop().toLowerCase() || 'bin';
    const key = crypto.randomUUID() + '.' + ext;

    await env.NODE_MEDIA.put(key, file.stream(), {
      httpMetadata: { contentType: file.type || 'application/octet-stream' },
    });

    return json({ url: '/media/' + key, contentType: file.type, key });
  }

  // ---- admin: publish ----
  if (path === '/admin/publish' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    const content = await request.json();
    const feed = await getFeed(storage);
    const item = {
      id: crypto.randomUUID(),
      type: content.type || 'writing',
      title: content.title || '',
      body: content.body || '',
      mediaUrl: content.mediaUrl || null,
      mediaContentType: content.mediaContentType || null,
      importedFrom: content.importedFrom || 'native',
      createdAt: new Date().toISOString(),
      authorPublicKey: identity.publicKey,
    };
    feed.unshift(item);
    await storage.set('feed', feed);
    return json({ published: item });
  }

  // ---- admin: delete an item ----
  if (path === '/admin/delete' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    const { id } = await request.json();
    const feed = await getFeed(storage);
    const item = feed.find(i => i.id === id);
    // optionally delete from R2 too
    if (item && item.mediaUrl && env.NODE_MEDIA) {
      const key = item.mediaUrl.replace('/media/', '');
      await env.NODE_MEDIA.delete(key).catch(() => {});
    }
    const filtered = feed.filter(i => i.id !== id);
    await storage.set('feed', filtered);
    return json({ deleted: id });
  }

  // ---- admin: inherit from ancestor ----
  if (path === '/admin/inherit') {
    if (!checkAuth(request)) return new Response('unauthorized — send Authorization: Bearer <ADMIN_TOKEN>', { status: 401 });
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

  // ---- live: public chat (DO-backed; WS is preferred, these are fallbacks) ----
  if (path === '/live/chat.json') {
    const r = await liveDO(env, subdomain, '/chat');
    return jsonCors(await r.json());
  }
  if (path === '/live/chat' && request.method === 'POST') {
    const body = await request.text();
    const r = await liveDO(env, subdomain, '/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
    return jsonCors(await r.json().catch(() => ({})), r.status);
  }

  // ---- live: pre-roll ad gate (public) ----
  if (path === '/live/preroll' && request.method === 'GET') {
    const cfg = await storage.get('preroll');
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
    }});
  }
  if (path === '/live/preroll/seen' && request.method === 'POST') {
    const { vid } = await request.json().catch(() => ({}));
    await liveDO(env, subdomain, '/ad-seen', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ vid }) });
    return jsonCors({ ok: true });
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
  if (path === '/live/subscribe' && request.method === 'POST') {
    if (!env.CALLS_APP_ID) return jsonCors({ error: 'not configured' }, 503);
    try {
      const data = await callsApi(env, 'POST', '/sessions/new', undefined);
      return jsonCors({ sessionId: data.sessionId });
    } catch(e) {
      return jsonCors({ error: e.message }, 502);
    }
  }

  // ---- live: viewer subscribe to tracks ----
  if (path === '/live/tracks' && request.method === 'POST') {
    if (!env.CALLS_APP_ID) return jsonCors({ error: 'not configured' }, 503);
    try {
      const { subscriberSessionId, tracks } = await request.json();
      const data = await callsApi(env, 'POST', `/sessions/${subscriberSessionId}/tracks/new`, { tracks });
      return jsonCors(data);
    } catch(e) {
      return jsonCors({ error: e.message }, 502);
    }
  }

  // ---- live: viewer renegotiate ----
  if (path === '/live/renegotiate' && request.method === 'POST') {
    if (!env.CALLS_APP_ID) return jsonCors({ error: 'not configured' }, 503);
    const { subscriberSessionId, sessionDescription } = await request.json();
    const data = await callsApi(env, 'PUT', `/sessions/${subscriberSessionId}/renegotiate`, { sessionDescription });
    return jsonCors(data);
  }

  // ---- live: WHIP ingest (Larix / OBS → Cloudflare Realtime) ----
  if (path === '/live/whip' || path.startsWith('/live/whip/')) {
    if (!env.CALLS_APP_ID) return new Response('not configured', { status: 503 });

    // WHIP POST — Larix sends SDP offer, we proxy to Cloudflare Realtime WHIP endpoint
    if (request.method === 'POST') {
      const sdpOffer = await request.text();
      const whipRes = await fetch(
        `https://rtc.live.cloudflare.com/v1/apps/${env.CALLS_APP_ID}/sessions/whip`,
        {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${env.CALLS_APP_SECRET}`,
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

    // WHIP DELETE — Larix ended the stream
    if (request.method === 'DELETE') {
      await liveDO(env, subdomain, '/end', { method: 'POST' });
      // Forward DELETE to Cloudflare Realtime
      const sessionId = path.split('/live/whip/')[1];
      if (sessionId) {
        await fetch(
          `https://rtc.live.cloudflare.com/v1/apps/${env.CALLS_APP_ID}/sessions/${sessionId}`,
          { method: 'DELETE', headers: { 'Authorization': `Bearer ${env.CALLS_APP_SECRET}` } }
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
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    if (!env.CALLS_APP_ID) return json({ error: 'CALLS_APP_ID not set' }, 503);
    try {
      const data = await callsApi(env, 'POST', '/sessions/new', undefined);
      return json({ sessionId: data.sessionId });
    } catch(e) {
      return json({ error: e.message }, 502);
    }
  }

  // ---- live: admin — add publisher tracks ----
  if (path === '/admin/live/tracks' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    const { sessionId, tracks, sessionDescription } = await request.json();
    const data = await callsApi(env, 'POST', `/sessions/${sessionId}/tracks/new`, { tracks, sessionDescription });
    return json(data);
  }

  // ---- live: admin — mark as live (store session + tracks in DO) ----
  if (path === '/admin/live/publish' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
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
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    await liveDO(env, subdomain, '/heartbeat', { method: 'POST' });
    return json({ ok: true });
  }

  // ---- live: admin — end ----
  if (path === '/admin/live/end' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    await liveDO(env, subdomain, '/end', { method: 'POST' });
    return json({ ok: true });
  }

  // ---- admin: update profile ----
  if (path === '/admin/profile' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
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
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    const body = await request.text();
    const r = await liveDO(env, subdomain, '/comment-del', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
    return json(await r.json());
  }

  // ---- admin: pre-roll sponsor ad config ----
  if (path === '/admin/preroll' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    const b = await request.json().catch(() => ({}));
    const clickUrl = (b.clickUrl || '').trim();
    const cfg = {
      enabled: !!b.enabled,
      mediaUrl: b.mediaUrl || null,
      contentType: b.contentType || null,
      sponsorName: (b.sponsorName || '').slice(0, 60),
      clickUrl: /^https?:\/\//.test(clickUrl) ? clickUrl.slice(0, 300) : '',
      durationSec: Math.min(Math.max(parseInt(b.durationSec) || 15, 3), 60),
      cpm: Math.max(0, Math.min(parseFloat(b.cpm) || 0, 1000)), // creator's rate, $ per 1000 views
      source: 'self', // 'self' = creator's own sponsor. 'network' reserved for approved nodes (house ad pool) later.
      updatedAt: new Date().toISOString(),
    };
    await storage.set('preroll', cfg);
    return json({ ok: true, preroll: cfg });
  }
  if (path === '/admin/preroll.json') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    const cfg = (await storage.get('preroll')) || { enabled: false };
    const r = await liveDO(env, subdomain, '/ad-stats');
    const stats = await r.json();
    const earnings = +(((stats.impressions || 0) / 1000) * (cfg.cpm || 0)).toFixed(2);
    return json({ preroll: cfg, stats: { ...stats, earnings } });
  }

  // ---- admin: inbox ----
  if (path === '/admin/inbox.json') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    const notifs = (await storage.get('inbox:notifications')) || [];
    return json({ notifications: notifs, unread: notifs.filter(n => !n.read).length });
  }

  if (path === '/admin/inbox/read' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    const notifs = (await storage.get('inbox:notifications')) || [];
    notifs.forEach(n => { n.read = true; });
    await storage.set('inbox:notifications', notifs);
    return json({ ok: true });
  }

  // ---- network: this node asks the root to join ----
  if (path === '/admin/request-join' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    if (isRoot(identity)) return json({ ok: true, alreadyMember: true });
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
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    if (!isRoot(identity)) return json({ error: 'not the network root' }, 403);
    return json({ members: await getRegistryMembers(env, identity), pending: (await gstore.get('registry:pending')) || [] });
  }
  if ((path === '/admin/registry/add' || path === '/admin/registry/approve') && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
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
    return json({ ok: true, members });
  }
  if (path === '/admin/registry/deny' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    if (!isRoot(identity)) return json({ error: 'not the network root' }, 403);
    const { pubkey } = await request.json().catch(() => ({}));
    const pending = ((await gstore.get('registry:pending')) || []).filter(p => p.pubkey !== pubkey);
    await gstore.set('registry:pending', pending);
    return json({ ok: true });
  }
  if (path === '/admin/registry/remove' && request.method === 'POST') {
    if (!checkAuth(request)) return json({ error: 'unauthorized' }, 401);
    if (!isRoot(identity)) return json({ error: 'not the network root' }, 403);
    const { pubkey } = await request.json().catch(() => ({}));
    if (pubkey === NETWORK_ROOT_PUBKEY) return json({ error: 'cannot remove the root' }, 400);
    const members = (await getRegistryMembers(env, identity)).filter(m => m.pubkey !== pubkey);
    await gstore.set('registry:members', members);
    await gstore.set('registry:updatedAt', new Date().toISOString());
    return json({ ok: true, members });
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
function jsonCors(data, status = 200) { return json(data, status, CORS_HEADERS); }

async function callsApi(env, method, path, body) {
  const res = await fetch(
    `https://rtc.live.cloudflare.com/v1/apps/${env.CALLS_APP_ID}${path}`,
    {
      method,
      headers: {
        'Authorization': `Bearer ${env.CALLS_APP_SECRET}`,
        'Content-Type': 'application/json',
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    }
  );
  const text = await res.text();
  if (!res.ok) throw new Error(`Calls API ${method} ${path} → ${res.status}: ${text}`);
  return JSON.parse(text);
}
function html(content) {
  return new Response(content, {
    headers: { 'Content-Type': 'text/html; charset=utf-8' },
  });
}
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
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

function renderApp({ identity, subdomain }) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
<meta name="theme-color" content="#000000">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="mobile-web-app-capable" content="yes">
<title>${escapeHtml(subdomain)}</title>
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
.card-video .c-media video { width: 100%; height: 100%; object-fit: cover; display: block; }
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
.avatar-follow {
  position: absolute; bottom: -9px; left: 50%; transform: translateX(-50%);
  width: 22px; height: 22px; border-radius: 50%;
  background: #FE2C55; border: 2px solid #000;
  display: flex; align-items: center; justify-content: center;
  font-size: 14px; font-weight: 800; color: #fff; line-height: 1;
}

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
  display: flex; flex-direction: column; gap: 5px;
  pointer-events: none;
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

/* The + create button */
.bnav-create { flex: 1; display: flex; align-items: center; justify-content: center; }
.create-pill {
  display: flex; align-items: center; height: 28px;
  border-radius: 6px; overflow: hidden; gap: 0;
}
.create-pill-l { width: 12px; height: 28px; background: #20D5EC; border-radius: 6px 0 0 6px; }
.create-pill-m {
  width: 36px; height: 28px; background: #fff;
  display: flex; align-items: center; justify-content: center;
  font-size: 22px; font-weight: 300; color: #000; line-height: 1;
}
.create-pill-r { width: 12px; height: 28px; background: #FE2C55; border-radius: 0 6px 6px 0; }

/* Creator badge */
.creator-badge {
  position: fixed; top: calc(max(env(safe-area-inset-top),0px) + 52px); left: 12px;
  z-index: 25;
  background: #FE2C55; color: #fff;
  padding: 3px 8px; border-radius: 4px;
  font-size: 10px; letter-spacing: 0.1em; text-transform: uppercase; font-weight: 700;
  display: none;
}
body.creator .creator-badge { display: block; }

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

<div id="stage">
  <div id="panel-a" class="panel"></div>
  <div id="panel-b" class="panel"></div>
</div>

<!-- Double-tap heart burst -->
<div class="heart-burst" id="heartBurst">❤️</div>

<!-- Progress bar (post position) -->
<div class="progress-segs" id="progressSegs"></div>

<!-- Creator badge -->
<div class="creator-badge" id="creatorBadge">creator</div>

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
  <div class="bnav-create">
    <button style="background:none;border:none;padding:0" onclick="onCreateTap()">
      <div class="create-pill">
        <div class="create-pill-l"></div>
        <div class="create-pill-m">+</div>
        <div class="create-pill-r"></div>
      </div>
    </button>
  </div>
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
    <label>Admin Token</label>
    <input type="password" id="tokenInput" placeholder="paste your token" autocomplete="off">
  </div>
  <button class="submit-btn" onclick="submitToken()">Unlock</button>
  <p style="margin-top:16px;font-size:12px;color:rgba(255,255,255,0.4);line-height:1.6">
    Set ADMIN_TOKEN as a secret in Cloudflare Workers. The token stays on this device only.
  </p>
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
  <video id="livePreview"   autoplay muted     playsinline style="display:none;position:absolute;inset:0;width:100%;height:100%;object-fit:cover;z-index:1"></video>
  <video id="liveViewVideo" autoplay playsinline style="display:none;position:absolute;inset:0;width:100%;height:100%;object-fit:cover;z-index:1"></video>

  <!-- Pre-roll sponsor ad overlay (covers the stream until the ad finishes) -->
  <div id="prerollOverlay" style="display:none;position:absolute;inset:0;z-index:30;background:#000">
    <video id="prerollVideo" playsinline style="position:absolute;inset:0;width:100%;height:100%;object-fit:contain;background:#000"></video>
    <div style="position:absolute;top:max(env(safe-area-inset-top),16px);left:16px;background:rgba(0,0,0,0.5);border-radius:6px;padding:4px 8px;font-size:11px;letter-spacing:0.08em;text-transform:uppercase;color:rgba(255,255,255,0.85)">Ad</div>
    <div id="prerollCountdown" style="position:absolute;top:max(env(safe-area-inset-top),16px);right:60px;background:rgba(0,0,0,0.6);border-radius:16px;padding:6px 12px;font-size:13px;font-weight:600;color:#fff">Stream starts soon…</div>
    <div id="prerollSponsor" onclick="onPrerollClick()" style="display:none;position:absolute;bottom:max(env(safe-area-inset-bottom),20px);left:16px;right:16px;background:rgba(0,0,0,0.55);backdrop-filter:blur(6px);border-radius:12px;padding:12px 16px;color:#fff;font-size:14px;cursor:pointer"></div>
  </div>

  <!-- Top bar -->
  <div style="position:absolute;top:max(env(safe-area-inset-top),16px);left:0;right:0;padding:0 16px;display:flex;align-items:center;justify-content:space-between;z-index:41">
    <div style="display:flex;align-items:center;gap:8px">
      <div class="info-live-badge" style="font-size:13px;padding:4px 10px" id="liveModalTitle">LIVE</div>
      <div class="live-viewer-badge" style="position:static" id="liveViewerBadge"><span id="liveViewerCount">0</span></div>
    </div>
    <button onclick="closeLiveModal()" style="background:rgba(0,0,0,0.5);border:none;color:#fff;font-size:22px;width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center">×</button>
  </div>

  <!-- Broadcaster controls (creator only) -->
  <div id="liveBroadcasterPanel" style="display:none;position:absolute;top:max(calc(env(safe-area-inset-top) + 64px),80px);left:0;right:0;flex-direction:column;align-items:center;gap:8px;z-index:9">
    <div style="display:flex;gap:8px">
      <button class="btn-primary" id="liveBtnStart" onclick="liveStart()" style="min-width:120px">Start Live</button>
      <button class="btn-secondary" id="liveBtnEnd" onclick="liveEnd()" style="display:none;min-width:120px">End Live</button>
    </div>
    <button id="liveBtnPreroll" onclick="openPrerollSheet()" style="background:rgba(255,255,255,0.12);border:none;color:#fff;border-radius:8px;padding:7px 14px;font-size:13px;font-weight:600">🎬 Sponsor Ad</button>
    <div id="liveStatusMsg" style="font-size:13px;color:rgba(255,255,255,0.7);text-align:center"></div>
  </div>

  <!-- Viewer status -->
  <div id="liveViewStatus" style="display:none;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:rgba(255,255,255,0.7);font-size:14px;z-index:10">Connecting…</div>
  <!-- Tap to unmute (shown if autoplay blocks audio) -->
  <div id="liveUnmuteBtn" onclick="const v=document.getElementById('liveViewVideo');v.muted=false;v.play().catch(()=>{});this.style.display='none'" style="display:none;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);background:rgba(0,0,0,0.6);border-radius:24px;padding:12px 24px;color:#fff;font-size:15px;font-weight:600;z-index:11;align-items:center;gap:8px;cursor:pointer">🔊 Tap to unmute</div>

  <!-- Chat overlay — bottom 15% -->
  <div style="position:absolute;bottom:calc(15% + max(env(safe-area-inset-bottom),0px));left:0;width:75%;padding:0 16px;z-index:10;display:flex;flex-direction:column;gap:5px;pointer-events:none" id="liveChatBox"></div>

  <!-- Chat input -->
  <div style="position:absolute;bottom:max(env(safe-area-inset-bottom),12px);left:0;right:0;padding:0 16px;display:flex;gap:8px;z-index:10">
    <input id="liveChatInput" placeholder="Say something…" style="flex:1;background:rgba(255,255,255,0.15);border:none;border-radius:20px;padding:8px 16px;color:#fff;font-size:14px;outline:none" onkeydown="if(event.key==='Enter')liveSendChat()">
    <button onclick="liveSendChat()" style="background:rgba(255,255,255,0.15);border:none;color:#fff;border-radius:20px;padding:8px 16px;font-size:14px">Send</button>
  </div>
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
    A 15s clip shown before viewers see your stream. Keep it under ~5&nbsp;MB so it loads instantly. Each viewer sees it at most once per 8&nbsp;minutes.
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
    <label>Your rate — CPM ($ per 1000 views)</label>
    <input type="text" id="prerollCpm" inputmode="decimal" placeholder="e.g. 4.00">
  </div>
  <div id="prerollStats" style="background:rgba(255,255,255,0.05);border-radius:12px;padding:14px;margin-bottom:16px"></div>
  <button class="submit-btn" id="prerollSaveBtn" onclick="savePreroll()">Save</button>
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

<div class="toast" id="toast"></div>

<script>
// ── STATE ─────────────────────────────────────────────────────
const SELF_SUBDOMAIN = '${escapeHtml(subdomain)}';
const SELF_KEY       = '${escapeHtml(identity.publicKey.slice(0, 8))}';
const SELF_PUBKEY    = '${escapeHtml(identity.publicKey)}';
const NETWORK_ROOT_HOST = '${escapeHtml(NETWORK_ROOT_HOST)}';
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
async function loadNode(node) {
  if (node.loaded || node.loading) return;
  node.loading = true;
  try {
    const res = await fetch(node.url + '/.well-known/feed.json');
    node.feed = (await res.json()).items || [];
  } catch(e) { node.feed = null; }
  node.loaded = true; node.loading = false;
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
    for (const m of members) {
      if (m.subdomain === SELF_SUBDOMAIN) continue;
      if (nodeGraph.find(n => n.subdomain === m.subdomain)) continue;
      nodeGraph.push({ subdomain: m.subdomain, url: 'https://' + m.subdomain,
        feed: null, postIndex: 0, loaded: false, loading: false });
    }
  } catch(e) {}
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

function preloadAdjacent() {
  [-1,1].forEach(d => { const i = nodeIndex+d; if (i>=0&&i<nodeGraph.length) loadNode(nodeGraph[i]); });
}

// ── LIVE STREAMING ────────────────────────────────────────────
let liveBroadcaster = null;
let liveViewer = null;
let liveChatInterval = null;
let liveStatusInterval = null;
let liveSocket = null;
let prerollActive = false; // true while a pre-roll ad is playing — keeps the live stream muted

// LiveSocket — realtime chat + viewer count + status via the node's LiveRoom DO.
// Replaces 2s chat polling. Auto-reconnects. Works cross-node (wss to that node).
class LiveSocket {
  constructor(subdomain, role, opts) {
    this.opts = opts || {};
    this.role = role;
    this.chat = [];
    this.closed = false;
    const wsScheme = location.protocol === 'https:' ? 'wss://' : 'ws://';
    const host = subdomain === SELF_SUBDOMAIN ? location.host : subdomain;
    const name = displayName() || 'viewer';
    this.url = wsScheme + host + '/live/ws?role=' + role + '&name=' + encodeURIComponent(name);
    this.connect();
  }
  connect() {
    if (this.closed) return;
    try { this.ws = new WebSocket(this.url); } catch(e) { return; }
    this.ws.onmessage = e => this.onMsg(e);
    this.ws.onclose = () => { if (!this.closed) setTimeout(() => this.connect(), 2000); };
    this.ws.onerror = () => {};
  }
  onMsg(e) {
    let d; try { d = JSON.parse(e.data); } catch { return; }
    if (d.t === 'init') {
      this.chat = d.chat || []; this.renderChat(); this.setViewers(d.viewers);
      if (d.status?.active) this.opts.onLive?.(d.status);
    } else if (d.t === 'chat') {
      this.chat.push(d.msg); if (this.chat.length > 100) this.chat.shift(); this.renderChat();
    } else if (d.t === 'viewers') {
      this.setViewers(d.viewers);
    } else if (d.t === 'live') {
      this.opts.onLive?.(d.status);
    } else if (d.t === 'ended') {
      this.opts.onEnded?.();
    }
  }
  renderChat() {
    const box = this.opts.chatBoxId && document.getElementById(this.opts.chatBoxId);
    if (!box) return;
    box.innerHTML = this.chat.slice(-6).map(m =>
      \`<div class="live-chat-msg"><strong>\${esc(m.name)}</strong> \${esc(m.text)}</div>\`
    ).join('');
    box.scrollTop = box.scrollHeight;
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
function resumeFeedVideos() { document.querySelectorAll('.card.active video').forEach(v => v.play().catch(()=>{})); }

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
    navigator.mediaDevices.getUserMedia({ video: true, audio: true }).then(stream => {
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
    onEnded: () => { if (!isBroadcaster) { setLiveStatus('Stream ended'); } },
  });
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
  if (liveBroadcaster) { liveBroadcaster.cleanup(); liveBroadcaster = null; }
  if (liveViewer) { liveViewer.cleanup(); liveViewer = null; }
  if (liveSocket) { liveSocket.close(); liveSocket = null; }
  stopLiveChatPoll();
  resumeFeedVideos();
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
    body: JSON.stringify({ text, name }),
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
        box.innerHTML = msgs.slice(-6).map(m =>
          \`<div class="live-card-chat-msg"><strong>\${esc(m.name)}</strong> \${esc(m.text)}</div>\`
        ).join('');
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
    if (d.show) ad = d.ad;
  } catch(e) {}

  // Connect the stream in the background DURING the ad, so it's ready at countdown 0.
  prerollActive = !!ad;
  if (ad) { const lv = document.getElementById('liveViewVideo'); if (lv) lv.muted = true; }
  startLiveViewer(subdomain);
  if (ad) await playPreroll(ad, base, vid);
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

// ── CARD RENDERING ────────────────────────────────────────────
function renderSidebar(node, item) {
  const handle   = node.subdomain.split('.')[0];
  const letter   = handle[0].toUpperCase();
  const grad     = avatarGrad(node.subdomain);
  const isSelf   = node.subdomain === SELF_SUBDOMAIN;
  const followEl = isSelf ? '' : \`<div class="avatar-follow">+</div>\`;
  const liked    = item ? isLiked(item.id) : false;
  const likeClr  = liked ? 'filter:none;color:#FE2C55' : 'filter:grayscale(0.2)';
  const id       = item ? esc(item.id) : '';
  const sub      = esc(node.subdomain);

  return \`<div class="sidebar">
    <div class="sidebar-item">
      <div class="avatar-wrap">
        <div class="avatar-circle" style="background:\${grad}">\${esc(letter)}</div>
        \${followEl}
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
  </div>\`;
}

function renderInfo(node, item) {
  const handle = '@' + node.subdomain.split('.')[0];
  const title  = item?.title || '';
  const body   = item?.body  || '';
  const desc   = [title, body].filter(Boolean).join(' · ');
  const live   = node.liveStatus?.active ? \`<div class="info-live-badge">● LIVE</div>\` : '';
  return \`<div class="card-info">
    \${live}
    <div class="info-username">\${esc(handle)}</div>
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

  if (item.type === 'live') {
    const handle = '@' + node.subdomain.split('.')[0];
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
      <video id="liveCardVideo" autoplay playsinline style="position:absolute;inset:0;width:100%;height:100%;object-fit:cover;background:#000"></video>
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
      <div class="c-media"><video src="\${esc(item.mediaUrl)}" playsinline preload="metadata" loop></video></div>
      \${info}\${sb}
    </div>\`;
  }
  if (item.type === 'photo' && item.mediaUrl) {
    return \`<div class="card card-photo" data-id="\${esc(item.id)}">
      <div class="c-media"><img src="\${esc(item.mediaUrl)}" alt="" loading="lazy"></div>
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

  // Disconnect previous live viewer if leaving a live card
  if (liveCardViewer) { liveCardViewer.cleanup(); liveCardViewer = null; stopLiveChatPoll(); }

  activePanel().innerHTML        = renderCard(node, node.postIndex);
  activePanel().style.transition = 'none';
  activePanel().style.transform  = '';
  activePanel().style.zIndex     = '2';
  inactivePanel().style.zIndex   = '1';
  activateVideos(activePanel());
  refreshCurrentInteractions();

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
function updateIndicators() {
  const node    = nodeGraph[nodeIndex];
  const feedLen = node.feed?.length || 0;
  const segsEl  = document.getElementById('progressSegs');
  if (feedLen <= 1) { segsEl.innerHTML = ''; }
  else {
    segsEl.innerHTML = Array.from({length: Math.min(feedLen, 14)}, (_,i) =>
      \`<div class="prog-seg\${i === node.postIndex ? ' active' : ''}"></div>\`
    ).join('');
  }
}

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
  if (isCreator) return SELF_SUBDOMAIN.split('.')[0] || 'viewer';
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
function renderComments(list) {
  const canDelete = isCreator && _commentsCtx.subdomain === SELF_SUBDOMAIN;
  if (!list.length) {
    document.getElementById('commentsList').innerHTML =
      '<div class="inbox-empty"><div class="inbox-empty-icon">💬</div>No comments yet</div>';
    return;
  }
  document.getElementById('commentsList').innerHTML = list.map(c => {
    const grad = avatarGrad(c.name || '?');
    const del = canDelete ? \`<button onclick="deleteComment('\${esc(c.id)}')" style="background:none;color:rgba(255,255,255,0.4);font-size:18px;padding:0 4px">×</button>\` : '';
    return \`<div class="inbox-notif" style="cursor:default">
      <div class="inbox-notif-avatar" style="background:\${grad}">\${esc((c.name[0] || '?').toUpperCase())}</div>
      <div class="inbox-notif-body">
        <div class="inbox-notif-text"><strong>\${esc(c.name)}</strong> \${esc(c.text)}</div>
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
  const url  = node.url || window.location.origin;
  try {
    if (navigator.share) await navigator.share({ title: node.subdomain, url });
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

function onProfileTap() {
  const node = nodeGraph[nodeIndex];
  openProfile(node.subdomain);
}

function openProfile(subdomain) {
  const sheet = document.getElementById('profileSheet');
  const handle = subdomain.split('.')[0];
  document.getElementById('profileHandle').textContent = '@' + handle;
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
  const handle = (data.subdomain || subdomain).split('.')[0];
  const name = data.displayName || handle;
  const bio = data.bio || '';
  const grad = avatarGrad(subdomain);
  const avatarHtml = data.avatarUrl
    ? \`<img src="\${esc(data.avatarUrl)}" alt="">\`
    : esc(handle[0].toUpperCase());

  const gridHtml = (data.feed || []).map(item => {
    const gridDel = isOwn ? \`<button class="grid-del" onclick="event.stopPropagation();deleteFromGrid('\${esc(item.id)}')">×</button>\` : '';
    if (item.type === 'video' && item.mediaUrl)
      return \`<div class="profile-grid-item" onclick="openProfilePost('\${esc(subdomain)}','\${esc(item.id)}')">
        <video src="\${esc(item.mediaUrl)}" preload="metadata" muted playsinline></video>
        <div class="grid-type-icon">▶</div>\${gridDel}
      </div>\`;
    if (item.type === 'photo' && item.mediaUrl)
      return \`<div class="profile-grid-item" onclick="openProfilePost('\${esc(subdomain)}','\${esc(item.id)}')">
        <img src="\${esc(item.mediaUrl)}" loading="lazy" alt="">\${gridDel}
      </div>\`;
    return \`<div class="profile-grid-item" onclick="openProfilePost('\${esc(subdomain)}','\${esc(item.id)}')">
      <div class="profile-grid-text" style="background:\${grad}">\${esc((item.title||item.body||'').slice(0,40))}</div>\${gridDel}
    </div>\`;
  }).join('');

  const connectBtn = !isOwn ? \`<button class="profile-connect-btn" onclick="connectToNode('\${esc(subdomain)}')">Connect</button>\` : '';
  const joinBtn = isOwn ? \`<button id="joinNetworkBtn" class="profile-connect-btn" style="display:\${isCreator && !isMember ? 'block' : 'none'};background:#20D5EC" onclick="requestJoin()">Join the network</button>\` : '';

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
          <div class="profile-stat-n">\${data.peerCount || 0}</div>
          <div class="profile-stat-l">Peers</div>
        </div>
      </div>
      \${bio ? \`<div class="profile-bio">\${esc(bio)}</div>\` : ''}
      \${connectBtn}\${joinBtn}
    </div>
    <div class="profile-grid">\${gridHtml || '<div style="padding:40px;text-align:center;color:rgba(255,255,255,0.3);grid-column:1/-1">No posts yet</div>'}</div>
  \`;
}

function openProfilePost(subdomain, postId) {
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

function connectToNode(subdomain) {
  if (nodeGraph.find(n => n.subdomain === subdomain)) {
    showToast('Already connected');
    return;
  }
  nodeGraph.push({ subdomain, url: 'https://' + subdomain, feed: null, postIndex: 0, loaded: false, loading: false });
  showToast('Connected to @' + subdomain.split('.')[0]);
  delete _profileCache[subdomain];
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
    document.getElementById('prerollCpm').value = cfg.cpm ? String(cfg.cpm) : '';
    _prerollMediaUrl = cfg.mediaUrl || null;
    _prerollContentType = cfg.contentType || null;
    document.getElementById('prerollFileName').textContent = cfg.mediaUrl ? 'current ad uploaded ✓' : '';
    renderPrerollStats(data.stats || {}, cfg);
  } catch(e) {}
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
    const handle = (n.subdomain || '').split('.')[0];
    const grad = avatarGrad(n.subdomain || '');
    const dot = n.read ? '' : '<div class="inbox-unread-dot"></div>';
    let text = '';
    if (n.type === 'peer_connected') text = \`<strong>@\${esc(handle)}</strong> connected to your node\`;
    else if (n.type === 'join_request') text = \`<strong>@\${esc(handle)}</strong> wants to join your network\`;
    else text = esc(n.type);
    const actions = n.type === 'join_request' ? \`<div style="display:flex;gap:8px;margin-top:8px">
      <button onclick="event.stopPropagation();approveJoin('\${esc(n.publicKey)}','\${esc(n.subdomain)}')" style="background:#FE2C55;border:none;color:#fff;border-radius:8px;padding:6px 14px;font-size:13px;font-weight:600">Approve</button>
      <button onclick="event.stopPropagation();denyJoin('\${esc(n.publicKey)}')" style="background:rgba(255,255,255,0.12);border:none;color:#fff;border-radius:8px;padding:6px 14px;font-size:13px">Deny</button>
    </div>\` : '';
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
    showToast('@' + (subdomain || '').split('.')[0] + ' added to the network');
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
    const handle = n.subdomain.split('.')[0];
    const grad   = avatarGrad(n.subdomain);
    return \`<div onclick="goToNode('\${esc(n.subdomain)}')" style="display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid rgba(255,255,255,0.08);cursor:pointer">
      <div style="width:42px;height:42px;border-radius:50%;background:\${grad};display:flex;align-items:center;justify-content:center;font-weight:700;font-size:16px;flex-shrink:0">\${esc(handle[0].toUpperCase())}</div>
      <div>
        <div style="font-weight:600">@\${esc(handle)}</div>
        <div style="font-size:12px;color:rgba(255,255,255,0.5)">\${esc(n.subdomain)}</div>
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
      token = val; localStorage.setItem('adminToken', val);
      enableCreator(); closeUnlock(); toast('creator mode on');
    } else { toast('invalid token'); }
  } catch(e) { toast('failed'); }
}

async function restoreCreator() {
  if (!token) return;
  try {
    const res = await fetch('/admin/verify', { method:'POST', headers:{'Authorization':'Bearer '+token} });
    if (res.ok) enableCreator();
    else { localStorage.removeItem('adminToken'); token = ''; }
  } catch(e) {}
}

function enableCreator() { isCreator = true; document.body.classList.add('creator'); refreshJoinButton(); }

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

function prepareIncoming(dx, dy) {
  const node     = nodeGraph[nodeIndex];
  const feedLen  = node.feed?.length || 0;
  let dir = null, nextNI = nodeIndex, nextPI = node.postIndex;

  if (G.axis === 'y') {
    if (dy < 0 && nodeIndex < nodeGraph.length - 1) { dir = 'up';   nextNI = nodeIndex + 1; nextPI = nodeGraph[nextNI].postIndex; }
    else if (dy > 0 && nodeIndex > 0)               { dir = 'down'; nextNI = nodeIndex - 1; nextPI = nodeGraph[nextNI].postIndex; }
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
      // refresh inactive panel if gesture still live
      if (G.active && G.dir === dir) {
        inactivePanel().innerHTML = renderCard(targetNode, nextPI);
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

  // commit state
  nodeGraph[nodeIndex].postIndex = G.nextPostIdx; // may be same node
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

    activateVideos(activePanel());
    refreshCurrentInteractions();
    preloadAdjacent();
    updateIndicators();
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
(async () => {
  await restoreCreator();
  await loadNode(nodeGraph[0]);
  renderCurrent();
  updateIndicators();
  initNetwork();
  preloadAdjacent();
  if (isCreator) {
    pollInboxBadge();
    setInterval(pollInboxBadge, 60000);
  }
})();
</script>
</body>
</html>`;
}
