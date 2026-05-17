# Keep services awake on Render

## Built-in warmup (in your codebase — no UptimeRobot required)

When anyone opens the **login page**, your app automatically wakes:

| Service | How |
|---------|-----|
| `login_server` | `GET /health` |
| `channel_manager` | `GET /health` |
| `websocket_server` | `GET /health` |

**Flow:**

1. Browser requests `/` → server starts warmup in a **background thread** immediately.
2. Login page shows a spinner and calls **`/api/warmup`** until all services respond (up to ~90s).
3. Login / Google buttons unlock when ready.

### Required env vars on **web_client** (Render)

```env
LOGIN_SERVER_URL=https://YOUR-login-server.onrender.com
CHANNEL_MANAGER_URL=https://realtimechat-channel-manager.onrender.com
WARMUP_WEBSOCKET_URL=https://YOUR-websocket-server.onrender.com
```

Or set `WEBSOCKET_CLIENT_URL` to your public `wss://...onrender.com/ws` URL (warmup derives the host).

Redeploy **web_client** after setting these.

### Verify

Open: `https://YOUR-web-client.onrender.com/api/warmup`

Example when ready:

```json
{
  "ready": true,
  "services": {
    "login_server": true,
    "channel_manager": true,
    "websocket_server": true
  }
}
```

---

## Optional: UptimeRobot (extra reliability for resume link)

Built-in warmup helps **when someone visits**. UptimeRobot keeps services warm **between visitors** (no 60s wait).

1. [https://uptimerobot.com](https://uptimerobot.com) → Add monitor every **5 minutes**:
   - `https://YOUR-web-client.onrender.com/health`
   - `https://YOUR-login-server.onrender.com/health`
   - `https://realtimechat-channel-manager.onrender.com/health`
   - `https://YOUR-websocket-server.onrender.com/health`

---

## Optional env tuning (web_client)

```env
WARMUP_MAX_SECONDS=90
WARMUP_PING_TIMEOUT=15
```
