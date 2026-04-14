# Nexus CX — Customer Experience Control Centre

A lightweight, zero-dependency CX operations platform built in pure Node.js. Designed for support teams that need a fast, self-hosted alternative to enterprise helpdesks.

![Status](https://img.shields.io/badge/status-active-brightgreen) ![Node](https://img.shields.io/badge/node-%3E%3D18-blue) ![License](https://img.shields.io/badge/license-MIT-green)

---

## Why I Built This

Most CX tooling is either expensive SaaS or a heavy framework install. I wanted something a team could spin up in minutes — no database, no Docker, no vendor lock-in — that still covered the full support workflow from ticket intake to resolution.

---

## Features

- **Unified Inbox** — Email (Gmail OAuth), Instagram, Facebook, Twitter, Trustpilot, App Store & Play Store in one view
- **Live Chat Widget** — Embeddable JS snippet with configurable branding and allowed origins
- **AI Reply Suggestions** — GPT-4o-mini powered suggestions with knowledge base context
- **Canned Responses** — Shortcut-triggered templates for fast replies
- **Agent Management** — Roles, channels, visibility controls, email invites
- **Bot Rules Engine** — Keyword-based auto-assignment and routing
- **Analytics Dashboard** — CSAT, resolution time, volume trends, agent performance
- **Weekly Email Reports** — Automated summaries sent to admin
- **PWA Support** — Installable on desktop and mobile
- **Cloudflare Tunnel** — One-command public URL for team sharing

---

## Tech Stack

| Layer | Choice | Reason |
|-------|--------|--------|
| Runtime | Node.js (zero npm deps) | Minimal footprint, fast cold starts |
| Storage | JSON flat-file + backup | No DB setup required |
| Auth | SHA-256 session tokens | Simple, sufficient for internal tools |
| AI | OpenAI GPT-4o-mini | Cost-effective for suggestion workloads |
| Email | Gmail OAuth 2.0 REST | No SMTP config, uses existing Google account |
| Deploy | Railway + persistent volume | One-click deploy, survives restarts |
| Tunnel | Cloudflare Tunnel | Zero-config HTTPS for team access |

---

## Quick Start

### Local

```bash
# 1. Clone
git clone https://github.com/yourusername/nexus-cx.git
cd nexus-cx

# 2. Set up environment
cp .env.example .env
# Edit .env with your values

# 3. Run
node server.js

# 4. Open
open http://localhost:3000
# Login: admin@nexus.com / admin123  ← change this immediately
```

### With PM2 (recommended for always-on)

```bash
npm install -g pm2
pm2 start server.js --name nexus-cx
pm2 save
pm2 startup
```

### Deploy to Railway

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app)

1. Fork this repo
2. New project → Deploy from GitHub repo
3. Add environment variables from `.env.example`
4. Add a Volume (mount at `/data`) for persistent storage

### Share with your team (Cloudflare Tunnel)

```bash
# Temporary URL (changes on restart)
cloudflared tunnel --url http://localhost:3000

# Permanent URL (after tunnel setup)
cloudflared tunnel run your-tunnel-name
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PORT` | No | Server port (default: 3000) |
| `BASE_URL` | Yes (production) | Your public URL for OAuth callbacks |
| `GMAIL_CLIENT_ID` | For Gmail | Google OAuth 2.0 client ID |
| `GMAIL_CLIENT_SECRET` | For Gmail | Google OAuth 2.0 client secret |

The AI key (OpenAI) and social tokens are configured per-workspace through the app's Settings UI — they're stored in `config.json` (excluded from git).

---

## Project Structure

```
nexus-cx/
├── server.js          # Backend — HTTP server, API routes, data layer (~2200 lines)
├── public/
│   ├── app.html       # Frontend — entire SPA in one file (~7900 lines)
│   ├── login.html     # Login page
│   └── manifest.json  # PWA manifest
├── .env.example       # Environment variable template
├── .gitignore
└── README.md
```

---

## Architecture Notes

- **Single-file backend**: All routes, middleware, and business logic live in `server.js`. Intentional — makes it easy to audit and deploy anywhere Node runs.
- **Flat-file DB**: `data.json` with automatic backup. Suitable for teams up to ~20 agents and moderate ticket volume. Swap in SQLite or Postgres by replacing `readDB()`/`writeDB()` with minimal changes.
- **No build step**: The frontend is vanilla JS + CSS in a single HTML file. No bundler, no framework. Opens directly in any browser.

---

## Screenshots

> *Add screenshots here — inbox view, dashboard, chat widget, settings*

---

## Author

**Sanil** — GTM & Intelligence Operator  
[LinkedIn](https://linkedin.com/in/sanilsharma8) · [Portfolio](https://yourdomain.com)

---

## License

MIT
