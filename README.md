# Social Node

A sovereign, TikTok-style social app that runs as a **single Cloudflare Worker**. Deploy your own node in a few clicks — it auto-provisions its storage and joins the network once the root approves you.

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/aitchisonpeter/social-node)

## What you get
- Your own node on **your** Cloudflare account (the free tier is enough to start)
- Storage (KV + R2) and the live-streaming engine (Durable Object) **provisioned automatically**
- Likes, comments, profiles, a vertical video/photo/text feed, and live streaming
- Auto-joins the network — host your own creators and earn the ad revenue on your domain

## Deploy (~3 clicks)
1. Click **Deploy to Cloudflare** above and sign in (a free account works).
2. When prompted, set **`ADMIN_TOKEN`** to any password you choose — that's how you unlock creator mode in the app.
   - `CALLS_APP_ID` / `CALLS_APP_SECRET` are **optional** (live streaming). Leave them blank to skip.
3. Cloudflare creates your Worker + storage and gives you a URL.

## First run
1. Open your node's URL.
2. Tap the **+** button **five times**, then enter your `ADMIN_TOKEN` to unlock **creator mode**.
3. Go to **Profile → Join the network**.
4. The network root approves your request, and your node appears across the network.

## Going live (optional)
To stream, create a **Cloudflare Realtime** app and add `CALLS_APP_ID` + `CALLS_APP_SECRET` as secrets to your Worker. Everything except live works without them.

## Hosting others (and earning)
Point a wildcard domain (`*.yourdomain.com`) at your Worker to host other creators as subdomains. Hosts earn the ad revenue on their domain — see the network docs.

## Building on the protocol
Every node exposes a clean, CORS-enabled JSON API — the embedded app is just another client of it, with no private seams. Custom dashboards, creator portals, mobile/TV clients, and sponsor-auditable impression stats are all built against the same surface. See **[PROTOCOL.md](PROTOCOL.md)**.
