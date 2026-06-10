# Social Node Protocol

**Protocol version: `0.8.0`** (reported in `/.well-known/node.json` as `protocol`)

This document is the contract for building on Social Node without touching the node code: third-party clients (mobile, TV, desktop), host dashboards, creator portals, analytics tools, sponsors auditing impressions.

**The invariant that keeps this honest:** the app embedded in every node is just another client of these endpoints. It has no private seams — anything it can do, your client can do. If a future node version breaks something documented here, that's a bug or a version bump, not a whim.

A **node** is one hostname (tenant). One Worker deployment can serve many nodes (wildcard hosting); each hostname has its own feed, profile, identity, and live room. Unless stated otherwise, every endpoint below is relative to the node's origin, e.g. `https://alice.example.com`.

---

## Concepts

- **Node identity** — each node has an Ed25519 keypair. The public key appears in `node.json` and the registry; it signs join requests and the registry. This is *node-to-node* trust, not user login.
- **The network** — membership is a **signed registry** served by the root node (`social.tuliptown.ca`). Clients build their cross-node feed graph from it. Anyone can fork the code; the network is whatever the root signs.
- **Viewer id (`vid`)** — clients generate a random opaque id per device (the reference client keeps it in localStorage). It dedupes likes, caps ad frequency, and identifies chat senders. Treat it as a secret of the device: endpoints never echo it back, and chat exposes only a short one-way hash of it (`sid`).
- **Auth** — three credentials, all sent as `Authorization: Bearer <value>` (see [Auth](#auth--credentials)).
- **CORS** — all public endpoints return `Access-Control-Allow-Origin: *`. Browser clients work cross-origin.

---

## Public read endpoints

### `GET /.well-known/node.json`
Node identity card.
```json
{ "protocol": "0.5.0", "subdomain": "<host>", "publicKey": "<ed25519 b64>", "createdAt": "...", "ancestors": [...] }
```

### `GET /.well-known/feed.json[?limit=&offset=]`
The node's posts, newest first. No params returns the full feed; `limit` (max 100) + `offset` paginate. Response:
```json
{ "subdomain": "...", "publicKey": "...", "displayName": "...", "avatarUrl": "/media/<key>|null",
  "total": 42, "offset": 0, "items": [ <post>... ] }
```
Post shape:
```json
{ "id": "<uuid>", "type": "writing|photo|video|live", "title": "", "body": "",
  "mediaUrl": "/media/<key>|null", "mediaContentType": "...", "transcript": "",
  "importedFrom": "native", "createdAt": "...", "authorPublicKey": "..." }
```
`mediaUrl` is **origin-relative** — prefix it with the owning node's origin. `transcript` (added 0.6.0) is optional searchable text for media posts — captions/OCR, ≤5000 chars; it feeds the post's SEO description and crawler body.

### `GET /profile.json`
```json
{ "subdomain": "...", "displayName": "...", "bio": "...", "avatarUrl": "...|null",
  "postCount": 3, "networkSize": 3, "peerCount": 3, "adsEnabled": false }
```
(`peerCount` is a legacy alias of `networkSize`.)

### `GET /.well-known/registry.json`
The signed network registry (any member serves it; the root is canonical; edge-cached 60s):
```json
{ "root": { "subdomain": "...", "pubkey": "..." },
  "members": [ { "subdomain": "...", "pubkey": "..." }... ],
  "updatedAt": "...", "signature": "<b64>" }
```
`signature` is Ed25519 by the root key over the string `updatedAt + "." + JSON.stringify(members)`. The pinned root host/pubkey are constants in `worker.js`. (Current clients trust TLS-to-root; verifying the signature client-side is encouraged and supported.)

### `GET /media/<key>`
The node's media (R2-backed). Supports `Range`/206, `ETag`, and edge caching. Safe for direct `<img>`/`<video>` use.

### Web surface (added 0.6.0)
For link sharing and crawlers — these serve HTML/text, not JSON:

- `GET /p/<post id>` — **canonical permalink** for a post. Serves the app with post-specific OpenGraph/Twitter meta (videos unfurl as `og:video`) and a `<noscript>` body for crawlers; deleted/unknown ids 302 to `/`. Link to posts with this.
- `GET /` — node-level meta (profile bio, avatar) + a crawler-readable list of recent post links.
- `GET /robots.txt` — allows all, disallows `/admin/` and `/auth/`, points at the sitemap.
- `GET /sitemap.xml` — homepage + every `/p/<id>` URL with `lastmod`.
- `GET /legal` (added 0.7.0) — the node's policies page: acceptable use, how to report, takedown process, privacy, imported-content rights.

---

## Moderation (added 0.7.0)

### `POST /report`
Flag content on the node you send it to. Public, no auth, rate-limited (5/10min/IP → `429`).
```json
{ "postId": "<uuid, optional>", "reason": "csam|ncii|hate|harassment|copyright|defamation|other", "details": "<≤1000 chars, optional>" }
```
→ `{ "ok": true }`. The report lands in that node's operator inbox **and** is escalated to the network root's inbox — moderation failures are visible at the network level, and registry removal is the enforcement tool (see `/legal`).

### `POST /protocol/report` (root only)
The escalation ingest: `{ "host", "postId", "reason", "details" }`. Non-root nodes answer `403`. Clients should use `/report` on the content's node, not this.

---

## Post interactions (public)

- `GET /post/likes?postId=&vid=` → `{ "count": 5, "liked": true }` (`liked` reflects the given `vid`)
- `POST /post/like` `{ "postId", "vid", "liked": true|false }` → `{ "count", "liked" }` — idempotent per `vid`
- `GET /post/comments?postId=` → `{ "comments": [ { "id", "name", "text", "at" }... ] }`
- `POST /post/comment` `{ "postId", "text", "name" }` → `{ "ok": true, "comment": {...} }` (creator deletes via admin)

---

## Live

Live streaming is WebRTC via Cloudflare Realtime (Calls) — sub-second latency, no HLS. A node without Calls credentials answers live endpoints with `503 { "error": "not configured" }`.

### State & presence
- `GET /live/status.json` → `{ "active": false, "viewers": 0 }` or
  `{ "active": true, "publisherSessionId": "...", "trackNames": [...], "startedAt": "...", "via": "browser|whip", "viewers": 2 }`
- `GET /live/who.json` *(creator auth required as of 0.8.0 — who's-in-the-room names are the broadcaster's moderation data, not public)* → `{ "viewers": [ { "name", "sid", "role", "muted" }... ], "mutedList": [...] }`. Unauthenticated → `401`; the viewer COUNT stays public via `status.json`/WS.

### Chat
Chat lives and dies with the stream: history (last 100 messages) is only served while live, and is cleared when the stream ends.

- **WebSocket** `GET /live/ws?role=viewer|broadcaster&name=<display>&vid=<vid>` — upgrade required. Server messages:
  - `{ "t": "init", "status": {...}, "chat": [<msg>...], "viewers": n }`
  - `{ "t": "chat", "msg": { "id", "name", "text", "sid", "at" } }`
  - `{ "t": "viewers", "viewers": n }`
  - `{ "t": "live", "status": {...} }` / `{ "t": "ended" }`

  Client→server: `{ "t": "chat", "text": "...", "name": "..." }` (per-connection throttle applies).
- **HTTP fallback**: `GET /live/chat.json` → `{ "messages": [...] }`; `POST /live/chat` `{ "text", "name", "vid" }` (per-IP throttled, 429 on spam).

`sid` is a short hash of the sender's `vid` — stable enough to moderate, useless to impersonate. Muted senders are shadow-muted: their sends are silently dropped server-side.

### Watching (subscriber SDP flow)
1. `GET /live/status.json` → need `active`, `publisherSessionId`, `trackNames`.
2. `POST /live/subscribe` → `{ "sessionId": "<subscriberSessionId>" }`
3. `POST /live/tracks` `{ "subscriberSessionId", "tracks": [ { "location": "remote", "sessionId": "<publisherSessionId>", "trackName": "<from trackNames>" }... ] }` → Calls answer (SDP offer inside if renegotiation required)
4. `POST /live/renegotiate` `{ "subscriberSessionId", "sessionDescription" }` to complete.

(These mint Calls sessions on the node operator's account — they're public by design so anyone can watch. Since 0.6.0 the node rate-limits `/live/subscribe`, `/live/tracks`, and `/live/renegotiate` per IP — expect `429` if you hammer them.)

### Broadcasting
Creator-auth only — see [Admin](#admin-endpoints). OBS/Larix can ingest via **WHIP** at `POST /live/whip?token=<creator token>` (or `Authorization: Bearer`); end the stream with the WHIP `DELETE`.

### Sponsor ads
- `GET /live/preroll?vid=` → `{ "show": false }` or `{ "show": true, "ad": { "mediaUrl", "sponsorName", "clickUrl", "durationSec", "category" } }` (per-`vid` frequency cap, ~8 min grace)
- `POST /live/preroll/seen` `{ "vid" }` — count an impression (per-IP floor applies)
- `POST /live/preroll/click` — count a click

---

## Verified measurement (root only)

Advertiser-facing numbers come from the root, not the node being paid ("don't grade your own homework"). Served only by the root host:

- `POST https://<root>/measure/impression` `{ "host": "<viewed node>", "vid" }` — clients beacon this in parallel with the local `seen` call. Root checks the host is a registry member, dedupes per `vid` AND per IP per window, day-buckets the count.
- `GET https://<root>/measure/stats.json?host=` → `{ "host", "total": n, "days": { "YYYY-MM-DD": n } }` — **public and auditable** by sponsors.

---

## Joining the network

1. Deploy your node (see README). It serves all of the above immediately — membership only affects cross-node discovery.
2. In-app: creator mode → Profile → **Join the network** (this signs and sends the request below), or raw:
   `POST https://<root>/protocol/join-request` with `{ "message": "<JSON string incl. pubkey + subdomain>", "signature": "<ed25519 b64 by that pubkey>" }`.
3. The root operator approves; your node appears in `registry.json` and every client's feed graph.

---

## Auth & credentials

Three bearer credentials, checked in this order; all admin endpoints fail closed:

1. **Host master token** — the Worker's `ADMIN_TOKEN` secret. Works on every tenant of that Worker. Required for anything marked *master*.
2. **Per-creator token** — minted by the host (`/admin/creator/mint-token`), scoped to one hostname.
3. **Session token** — from password login. 30-day, stateless (Ed25519-signed by the node's key), scoped to one hostname.

Public auth endpoints (rate-limited 10/10min/IP):
- `POST /auth/claim` `{ "code", "password" }` → `{ "ok": true, "session": "<token>" }` — one-time claim-code redemption (host onboarding), sets the password.
- `POST /auth/login` `{ "password" }` → `{ "ok": true, "session": "<token>" }`
- `POST /admin/verify` (any credential) → `{ "ok": true, "master": true|false }` — probe what a token can do.

---

## Admin endpoints

For host dashboards and creator portals. All take `Authorization: Bearer`. *(master)* = host master token only.

**Content** — `POST /admin/publish` `{ type, title, body, mediaUrl, mediaContentType, transcript? }` → `{ published: <post> }`; `POST /admin/upload` (multipart, max 256MB, returns the `/media/` URL); `POST /admin/import-url` `{ url, title?, createdAt?, source? }` — the node fetches the video server-side into its own storage and publishes it with the original date (feed stays sorted by `createdAt`; idempotent per source URL; used by the TikTok data-export importer); `POST /admin/delete` `{ id }` or `{ ids: [...] }` (bulk, max 500); `POST /admin/profile` `{ displayName, bio, avatarUrl }`; `POST /admin/comment/delete` `{ postId, id }`.

**Live** — `POST /admin/live/start` → Calls session; `POST /admin/live/tracks` (publisher SDP); `POST /admin/live/publish` `{ sessionId, trackNames }` (marks live); `POST /admin/live/heartbeat` (every ~15s — browser streams are presumed dead after 45s without one); `POST /admin/live/end`; `GET /admin/live/history` → `{ streams: [ { startedAt, endedAt, durationSec, peakViewers, viewerSec, via }... ] }` (newest first, last 50 — `viewerSec` is accumulated viewer-seconds, the honest basis for data/cost estimates); `POST /admin/live/mute` `{ sid, name, muted: true|false }` (persistent shadow-mute); `POST /admin/live/overlay` `{ text ≤120, imageUrl, txt, img }` (added 0.8.0) — text/image overlay rendered client-side over the stream; `txt`/`img` are `{x,y,s}` placements (screen fractions + scale, clamped server-side); broadcast to viewers over WS as `{ "t": "overlay", "overlay": {...} }` and included in WS `init`; stream-scoped (clears on end).

**Revenue** — `GET/POST /admin/preroll` *(master — the ad config is worker-wide)*; `GET /admin/preroll.json`; `POST /admin/ads` `{ host, sharePct }` *(master)*; `GET /admin/ads-ledger` *(master)* — per-creator views × CPM × share %, network fee, host net; `GET /admin/my-earnings` (any creator) — read-only owed view; `POST /admin/calls-creds` `{ host, appId, appSecret }` *(master)* — point a tenant's live traffic at its own Cloudflare account.

**Tenants & onboarding** *(all master)* — `GET /admin/provisioned`; `POST /admin/provision` / `/admin/unprovision` `{ host }`; `POST /admin/creator/mint-claim` `{ host }` → one-time claim code + URL; `POST /admin/creator/mint-token` / `/clear-token` `{ host }`.

**Network (root operator)** — `GET /admin/registry.json`; `POST /admin/registry/approve|deny|remove`; `GET /admin/inbox.json`; `POST /admin/request-join` (any member-to-be).

---

## Legacy / do not build on

`/.well-known/peers.json`, `/.well-known/source.json`, `/admin/inherit` — pre-registry bootstrap mechanics, kept for compatibility. They will be removed.

**Removed in 0.6.0:** `/protocol/announce` and `/protocol/peers` (unauthenticated pre-registry bootstrap) now return 404. Membership is the signed registry, full stop.

## Versioning

`protocol` in `node.json` is semver-ish: additive changes bump the minor; anything that breaks a documented shape above bumps the major. The embedded client consumes only what's documented here, so it serves as the live conformance test.
