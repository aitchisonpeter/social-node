# Social Node Protocol

**Protocol version: `0.13.0`** (reported in `/.well-known/node.json` as `protocol`)

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

When the deployment opts into traffic analytics (added 0.13.0 — see **Traffic analytics** under Admin endpoints), app pages carry Cloudflare's cookieless Web Analytics beacon (`static.cloudflareinsights.com`): aggregate page-load counts only, no cookies, no cross-site tracking. Nodes that don't configure it serve no analytics script at all.

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
- `POST /post/like` `{ "postId", "vid", "liked": true|false, "self"? }` → `{ "count", "liked" }` — idempotent per `vid`
- `GET /post/comments?postId=` → `{ "comments": [ { "id", "name", "text", "at", "sub"? }... ] }` (`sub: true` = verified subscriber, added 0.9.0)
- `POST /post/comment` `{ "postId", "text", "name", "subToken"?, "self"? }` → `{ "ok": true, "comment": {...} }` (creator deletes via admin; a valid active `subToken` stamps the comment `sub: true` — the badge is verified server-side, never client-claimed)

Engagement notifications (added 0.14.0): a new like or comment also lands in the creator's inbox — unread like-notifications aggregate per post ("3 people liked…"). `self: true` is a client hint that the creator is engaging with their own post (suppresses the notification only; no effect on counts or badges). Comments whose `name` equals the tenant hostname are treated as the creator's own.

---

## Subscribers (added 0.9.0)

Creator-approved membership for viewers — no account, same device-bound trust model as creator tokens: the plaintext subscriber token lives only on the viewer's device, the node stores its SHA-256. Active subscribers get a server-verified badge in live chat + comments, access to the creator's **lounge** (a persistent chat room), and may opt into creator-triggered Web Push.

### Requesting
- `POST /sub/request` `{ "name", "vid", "token"? }` → `{ "ok": true, "status": "pending", "token": "<shown once>" }` — rate-limited 5/10min/IP. The request lands in the creator's inbox; approval flips the token live. Passing an existing `token` is idempotent: it echoes the current standing instead of duplicating (`{ "ok": true, "status": "pending|active" }`).
- `POST /sub/status` `{ "token" }` → `{ "status": "none|pending|active", "name", "push": bool }` — `none` means the token is dead (declined/removed); clients should discard it.

### Lounge
Persistent per-creator chat room — unlike live chat it never resets. Access = active subscriber token OR creator auth.

- **WebSocket** `GET /lounge/ws?sub=<token>|auth=<creator credential>&name=&vid=` — upgrade required; non-members get `403`. Credentials ride query params (browsers can't set WS headers); the worker strips them before the room sees the connection. Server messages:
  - `{ "t": "init", "lounge": [<msg>...], "members": n }` (last 300 messages)
  - `{ "t": "chat", "msg": { "id", "name", "text", "sid", "at", "sub": true | "creator": true } }`
  - `{ "t": "members", "members": n }`

  Client→server: `{ "t": "chat", "text" }` (per-connection throttle; live-chat shadow-mutes apply here too).
- `POST /lounge/history.json` `{ "token" | "auth" }` → `{ "messages": [...] }` (POST so tokens stay out of URLs/logs).

### Web Push (opt-in)
Standards-track Web Push: VAPID + RFC 8291 `aes128gcm` payload encryption. The VAPID key is **per worker** (all tenants of a worker share it — a browser origin allows only one push subscription with one `applicationServerKey`).

- `GET /sub/vapid.json` → `{ "key": "<base64url P-256 public key>" }` — feed it to `pushManager.subscribe`.
- `POST /sub/push` `{ "token", "subscription": <PushSubscription.toJSON()> }` — active subscribers only.
- `POST /sub/push/clear` `{ "token" }` — stop pushes for this subscriber.

Pushes are **deliberate**: nothing fires automatically. The creator pushes a specific lounge message (see Admin); the encrypted payload is `{ "host", "name", "text", "url" }`. Dead subscriptions (push service 404/410) are pruned on send.

---

## Live

Live streaming is WebRTC via Cloudflare Realtime (Calls) — sub-second latency, no HLS. A node without Calls credentials answers live endpoints with `503 { "error": "not configured" }`.

### State & presence
- `GET /live/status.json` → `{ "active": false, "viewers": 0 }` or
  `{ "active": true, "publisherSessionId": "...", "trackNames": [...], "startedAt": "...", "via": "browser|whip", "viewers": 2 }`
- `GET /live/who.json` *(creator auth required as of 0.8.0 — who's-in-the-room names are the broadcaster's moderation data, not public)* → `{ "viewers": [ { "name", "sid", "role", "muted" }... ], "mutedList": [...] }`. Unauthenticated → `401`; the viewer COUNT stays public via `status.json`/WS.

### Chat
Chat lives and dies with the stream: history (last 100 messages) is only served while live, and is cleared when the stream ends.

- **WebSocket** `GET /live/ws?role=viewer|broadcaster&name=<display>&vid=<vid>[&sub=<subscriber token>]` — upgrade required. A valid active `sub` token (verified and stripped at the worker, 0.9.0) stamps that connection's chat messages `sub: true`. Server messages:
  - `{ "t": "init", "status": {...}, "chat": [<msg>...], "viewers": n }`
  - `{ "t": "chat", "msg": { "id", "name", "text", "sid", "at", "sub"? } }`
  - `{ "t": "viewers", "viewers": n }`
  - `{ "t": "live", "status": {...} }` / `{ "t": "ended" }`

  Client→server: `{ "t": "chat", "text": "...", "name": "..." }` (per-connection throttle applies).
- **HTTP fallback**: `GET /live/chat.json` → `{ "messages": [...] }`; `POST /live/chat` `{ "text", "name", "vid", "subToken"? }` (per-IP throttled, 429 on spam).

`sid` is a short hash of the sender's `vid` — stable enough to moderate, useless to impersonate. Muted senders are shadow-muted: their sends are silently dropped server-side.

### Watching (subscriber SDP flow)
1. `GET /live/status.json` → need `active`, `publisherSessionId`, `trackNames`.
2. `POST /live/subscribe` → `{ "sessionId": "<subscriberSessionId>" }`
3. `POST /live/tracks` `{ "subscriberSessionId", "tracks": [ { "location": "remote", "sessionId": "<publisherSessionId>", "trackName": "<from trackNames>" }... ] }` → Calls answer (SDP offer inside if renegotiation required)
4. `POST /live/renegotiate` `{ "subscriberSessionId", "sessionDescription" }` to complete.

(These mint Calls sessions on the node operator's account — they're public by design so anyone can watch. Since 0.6.0 the node rate-limits `/live/subscribe`, `/live/tracks`, and `/live/renegotiate` per IP — expect `429` if you hammer them.)

### Broadcasting
Creator-auth only — see [Admin](#admin-endpoints). OBS/Larix can ingest via **WHIP** at `POST /live/whip?token=<creator token>` (or `Authorization: Bearer`); end the stream with the WHIP `DELETE`.

### Ads — two separate slots (split in 0.11.0)

**Live pre-roll — owned by the CREATOR being watched** (per-tenant config, `/admin/livead`). A sponsor the creator sourced themselves, or `kind: "intro"` — a theme song / intro clip with no sponsor and no billing.
- `GET /live/preroll?vid=` → `{ "show": false }` or `{ "show": true, "ad": { "kind": "sponsor"|"intro", "mediaUrl", "sponsorName", "clickUrl", "durationSec", "category" } }` (per-`vid` frequency cap, ~8 min grace, separate from the feed ad's)
- `POST /live/preroll/seen` `{ "vid", "bill"? }` — record the grace window; increments the impression counter unless `bill: false` (clients send `bill: false` for `intro` creatives — a theme song isn't a sponsor view)
- `POST /live/preroll/click` — count a click

**In-feed interstitial — owned by the NODE host** (worker-wide config, `/admin/preroll`, master). Revenue is shared per-creator via share %.
- `GET /feed/ad?vid=` → same shape as above, no `kind` (per-`vid` cap, own grace window)
- `POST /feed/ad/seen` `{ "vid" }` — count an impression (per-IP floor applies)
- `POST /feed/ad/click` — count a click

Counters are independent per surface: watching a live pre-roll never suppresses (or inflates) the feed ad, and vice versa.

---

## Verified measurement (root only)

Advertiser-facing numbers come from the root, not the node being paid ("don't grade your own homework"). Served only by the root host:

- `POST https://<root>/measure/impression` `{ "host": "<viewed node>", "vid", "surface": "live"|"feed" }` — clients beacon this in parallel with the local `seen` call. Root checks the host is a registry member, dedupes per `vid` AND per IP per window **per surface**, day-buckets the count. Intro creatives are never beaconed.
- `POST https://<root>/measure/click` `{ "host", "vid", "surface" }` (added 0.11.0) — verified click, deduped per `vid` per window.
- `GET https://<root>/measure/stats.json?host=` → `{ "host", "total", "days": { "YYYY-MM-DD": n }, "surfaces": { "live": { "total", "days", "clicks" }, "feed": { ... } } }` — **public and auditable** by sponsors. (`total`/`days` at the top level are the combined pre-0.11 buckets.)
- `GET https://<root>/sponsor/<host>` (added 0.11.0) — a public, human-readable sponsor dashboard for that creator's live pre-roll: verified views (today / 7d / lifetime), verified clicks, daily breakdown, auto-refreshing every 5s. The URL a creator hands their sponsor.

---

## Client error beacon (added 0.14.0)

`POST /debug/client-error` `{ "msg", "src"?, "ctx"? }` → `{ "ok": true }` — public, rate-limited (20/min/IP, floods silently dropped). The embedded client reports uncaught JS errors and unhandled promise rejections here; the node logs them (visible in `wrangler tail` and Workers Logs when `observability` is enabled in wrangler.jsonc). Nothing is stored; it is purely an operator observability hook.

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
- `POST /auth/claim` `{ "code", "password" }` → `{ "ok": true, "session": "<token>", "joinRequested": true|false }` — one-time claim-code redemption (host onboarding), sets the password. Since 0.14.0 a successful claim also auto-files the network join request (the host provisioning the handle expressed the intent; the root's Approve remains the human gate) — `joinRequested` reports whether that succeeded.
- `POST /auth/login` `{ "password" }` → `{ "ok": true, "session": "<token>" }`
- `POST /admin/verify` (any credential) → `{ "ok": true, "master": true|false }` — probe what a token can do.

---

## Admin endpoints

For host dashboards and creator portals. All take `Authorization: Bearer`. *(master)* = host master token only.

**Content** — `POST /admin/publish` `{ type, title, body, mediaUrl, mediaContentType, transcript? }` → `{ published: <post> }`; `POST /admin/upload` (multipart, max 256MB, returns the `/media/` URL); `POST /admin/import-url` `{ url, title?, createdAt?, source? }` — the node fetches the video server-side into its own storage and publishes it with the original date (feed stays sorted by `createdAt`; idempotent per source URL; used by the TikTok data-export importer); **background import queue (added 0.14.0)** — `POST /admin/import/queue` `{ items: [ { url, title?, createdAt?, source? }... ] }` (max 500) queues the list server-side and imports ONE video every ~10s via a Durable Object alarm (spaces out feed writes; survives the client closing; an `import_done` inbox notification with counts/errors lands at the end), `GET /admin/import/status` → `{ remaining, stats }`, `POST /admin/import/cancel` (already-imported posts stay); `POST /admin/delete` `{ id }` or `{ ids: [...] }` (bulk, max 500); `POST /admin/profile` `{ displayName, bio, avatarUrl }`; `POST /admin/comment/delete` `{ postId, id }`.

**Live** — `POST /admin/live/start` → Calls session; `POST /admin/live/tracks` (publisher SDP); `POST /admin/live/publish` `{ sessionId, trackNames }` (marks live); `POST /admin/live/heartbeat` (every ~15s — browser streams are presumed dead after 45s without one); `POST /admin/live/end`; `GET /admin/live/history` → `{ streams: [ { startedAt, endedAt, durationSec, peakViewers, viewerSec, via }... ] }` (newest first, last 50 — `viewerSec` is accumulated viewer-seconds, the honest basis for data/cost estimates); `POST /admin/live/mute` `{ sid, name, muted: true|false }` (persistent shadow-mute); `POST /admin/live/chat-delete` `{ id }` (added 0.12.0) — moderation-deletes one chat message from storage and broadcasts `{ "t": "chat-del", "id" }` to every connected client (creator-only by design — viewers cannot delete, even their own); `POST /admin/live/overlay` `{ text ≤120, imageUrl, txt, img }` (added 0.8.0) — text/image overlay rendered client-side over the stream; `txt`/`img` are `{x,y,s}` placements (screen fractions + scale, clamped server-side); broadcast to viewers over WS as `{ "t": "overlay", "overlay": {...} }` and included in WS `init`; stream-scoped (clears on end).

**Revenue** — `POST /admin/preroll` *(master — the FEED ad config is worker-wide)*; `GET /admin/preroll.json` *(master)*; `POST /admin/livead` `{ enabled, kind: "sponsor"|"intro", mediaUrl, sponsorName, clickUrl, category, cpm, durationSec }` (any creator — their own live pre-roll, added 0.11.0); `GET /admin/livead.json` → `{ livead, stats: { impressions, clicks, earnings }, sponsorUrl }`; `POST /admin/ads` `{ host, sharePct }` *(master)*; `GET /admin/ads-ledger` *(master)* — per-creator FEED views × CPM × share %, network fee, host net; `GET /admin/my-earnings` (any creator) — read-only owed view; `POST /admin/calls-creds` `{ host, appId, appSecret }` *(master)* — point a tenant's live traffic at its own Cloudflare account.

**Traffic analytics (added 0.13.0)** — `GET /admin/analytics?days=1..90` *(master, default 7)* → `{ totals: { pageviews, visits }, byDay, byHost, byPath, byCountry, byReferer, byDevice }` — the node's web traffic, pulled live from Cloudflare's GraphQL Analytics API (Web Analytics RUM data). Opt-in per deployment: set the `ANALYTICS_TOKEN` secret (an API token with Account Analytics:Read) plus the `ANALYTICS_BEACON_TOKEN`, `ANALYTICS_SITE_TAG`, and `ANALYTICS_ACCOUNT_ID` vars; unconfigured nodes return `503`. Setting the beacon token also turns on the beacon in served pages (see Web surface). Gotcha: Cloudflare gives each Web Analytics site TWO ids — the beacon **token** (in the JS snippet) and the **site tag** (the hex id in the dashboard's manage-site URL, what GraphQL filters on). They are not interchangeable.

**Tenants & onboarding** *(all master)* — `GET /admin/provisioned`; `POST /admin/provision` / `/admin/unprovision` `{ host }`; `POST /admin/creator/mint-claim` `{ host }` → one-time claim code + URL; `POST /admin/creator/mint-token` / `/clear-token` `{ host }`.

**Subscribers (added 0.9.0)** — `GET /admin/subs.json` → `{ subscribers: [ { id, name, status, requestedAt, approvedAt, push }... ] }`; `POST /admin/sub/approve` `{ subId }`; `POST /admin/sub/decline` `{ subId }` (declines a pending request OR removes an existing subscriber — the entry, its token, and any push registration die together); `POST /admin/lounge/push` `{ text ≤180 }` → `{ ok, sent, failed }` — Web-Push the text to every opted-in subscriber (rate-limited 2/min; prunes dead push subscriptions).

**Network (root operator)** — `GET /admin/registry.json`; `POST /admin/registry/approve|deny|remove`; `GET /admin/inbox.json`; `POST /admin/inbox/delete` `{ id }` (added 0.12.0 — dismiss one notification; does not resolve pending join/sub requests); `POST /admin/request-join` (any member-to-be). Note (0.12.0): `POST /admin/unprovision` on the ROOT also ejects the host from the registry if it is a member (response gains `"ejected": true|false`) — registry members bypass the provisioning gate, so unprovisioning alone never stopped a member from serving.

**Cross-posting (added 0.10.0)** — "post once, publish everywhere." The host registers one developer app per platform (`POST /admin/xpost/apps` `{platform, clientId, clientSecret}`, *master*; stored AES-GCM-encrypted under the `XPOST_KEY` worker secret — required). Creators then connect their own accounts: `GET /admin/xpost/status`; `POST /admin/xpost/connect` `{platform}` → `{url}` (OAuth authorize URL; X uses PKCE); the platform redirects to the public `GET /xpost/callback/<platform>` which exchanges the code and stores the tokens encrypted per-tenant; `POST /admin/xpost/disconnect` `{platform}`. Fan-out: `POST /admin/xpost/publish` `{postId, platforms:[...], commercial?}` → `{results: {<platform>: {ok, id?|error}}}` — per-platform adapters enforce caps/media rules (TikTok 2200 video-only via PULL_FROM_URL + commercial disclosure toggles; Instagram 2200 image/Reels via Graph containers; YouTube 5000 resumable upload, vertical <60s auto-Shorts; X 280 + image; LinkedIn 3000 UGC; Facebook Page posts). Platforms: `tiktok instagram youtube x linkedin facebook`. Captions are truncated to the platform cap with an ellipsis; one platform's failure never blocks another.

---

## Legacy / do not build on

`/.well-known/peers.json`, `/.well-known/source.json`, `/admin/inherit` — pre-registry bootstrap mechanics, kept for compatibility. They will be removed.

**Removed in 0.6.0:** `/protocol/announce` and `/protocol/peers` (unauthenticated pre-registry bootstrap) now return 404. Membership is the signed registry, full stop.

## Versioning

`protocol` in `node.json` is semver-ish: additive changes bump the minor; anything that breaks a documented shape above bumps the major. The embedded client consumes only what's documented here, so it serves as the live conformance test.
