# The Social Node Network

Anyone can fork the code. The **network** is something else: a signed list of members, served by the root node. This document explains how membership, hosting, and money work — including the parts that don't exist yet.

## How the network is defined

- The root node (`social.tuliptown.ca`) serves `/.well-known/registry.json`: the member list, Ed25519-signed by the root key that is pinned as a constant in `worker.js`.
- Every client builds its cross-node feed graph from that registry. If you're in it, viewers on every node can swipe to you; if you're not, your node still works — it's just your own island.
- A fork can change the pinned root and start its own network. That's a feature, not a loophole: the member set is the network, and it can't be spoofed against this root's key.

## Joining

1. Deploy your node (README) and set up your profile.
2. In creator mode: **Profile → Join the network**. Your node signs a join request with its own keypair and sends it to the root.
3. The root operator reviews and approves (or doesn't — membership is moderated, by a human). On approval you appear in the registry and in feeds across the network.

Leaving is the same in reverse: the root can remove a node from the registry (spam, abuse, legal takedowns). Removal doesn't touch your node or your data — you just stop appearing in the shared graph.

## Hosting creators

A node = one hostname. Point a wildcard domain (`*.yourdomain.com`) at your Worker and you can host many creators, each on their own subdomain with isolated content, identity, and live room:

- **Provisioning** — new hostnames are gated; you (master token) provision them via the in-app **Manage creators** panel, which also mints one-click invite links (claim codes). The creator opens the link, sets a password, and owns their subdomain from then on.
- **Their credentials, your worker** — creators get per-subdomain tokens/passwords. Your `ADMIN_TOKEN` stays yours and works everywhere on your worker.
- **Live streaming** — by default a hosted creator's streams run on your Cloudflare Realtime credentials (your bill). You can point any tenant at its own Realtime app instead (`/admin/calls-creds`), so ambitious creators carry their own costs.

## Ads and revenue — what exists today

The model: **the ad slot belongs to the node being viewed, and the host owns it.**

- You (master) configure one sponsor ad per worker — creative, click-through, duration, CPM. It plays as a skippable pre-roll on live streams and as an occasional in-feed interstitial, on every tenant you host.
- Impressions are counted twice, on purpose:
  - **Locally**, per tenant — this is what your revenue ledger pays creators from.
  - **At the root** (`/measure/*`) — deduped, day-bucketed, and **publicly auditable**, so a sponsor never has to trust the node they're paying ("don't grade your own homework").
- You set a **revenue share %** per hosted creator. The in-app ledger shows each creator's views × CPM × share, a 2% network fee, and your net. Creators see their own owed balance in their profile.

**What does not exist: payments.** Nothing moves money. The CPM is a number you typed; the sponsor pays you however you and they agree (invoice, e-transfer, a handshake), and you pay your creators the same way. The ledger is the honest bookkeeping for those conversations — not a wallet. If your plan requires automated payouts, this isn't that yet.

## Responsibilities

Running a node means hosting content on infrastructure you control, under your name:

- You are responsible for what your node (and your hosted creators) serve, under the laws that apply to you.
- Every node ships a **report button** (`POST /report`) and a policies page (`/legal`). Reports land in your inbox AND the root's — read yours, act on notice, and treat CSAM/NCII reports as drop-everything (in many jurisdictions, including Canada, reporting CSAM to authorities is a legal duty for service operators).
- The root operator moderates *network membership*, not your node. Registry removal is the network's only enforcement tool.
- Imported content (e.g. the TikTok importer) is content you chose to host — licensing for music and other third-party material in it is on you.
