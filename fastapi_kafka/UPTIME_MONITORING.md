# Keep Render Services Awake (Option A — UptimeRobot)

On Render **free tier**, services sleep after ~15 minutes without traffic.  
Use **UptimeRobot** to ping `/health` every **10 minutes** so your **resume link** stays reliable.

## URLs to monitor

Replace with your real Render hostnames:

| Monitor name      | URL |
|-------------------|-----|
| web_client        | `https://realtimechat-web-client.onrender.com/health` |
| login_server      | `https://realtimechat-login-server.onrender.com/health` |
| channel_manager   | `https://realtimechat-channel-manager.onrender.com/health` |
| websocket_server  | `https://YOUR-websocket-service.onrender.com/health` |

> Use the **public HTTPS URL** of each Web Service (not internal hostnames).

## UptimeRobot setup (5 minutes)

1. Create a free account at [https://uptimerobot.com](https://uptimerobot.com).
2. Click **Add New Monitor**.
3. For each URL above:
   - **Monitor Type:** HTTP(s)
   - **URL:** paste the `/health` URL
   - **Monitoring Interval:** 5 minutes (free plan allows 5 min; use the shortest available)
4. Save all monitors.

Optional: enable email alerts so you know if a service is down.

## web_client environment (Option C — automatic warmup)

On **web_client** in Render → **Environment**, set:

```env
LOGIN_SERVER_URL=https://realtimechat-login-server.onrender.com
CHANNEL_MANAGER_URL=https://realtimechat-channel-manager.onrender.com
WARMUP_WEBSOCKET_URL=https://YOUR-websocket-service.onrender.com
```

When a visitor opens the login page, the app calls `/api/warmup` and waits for backends to wake **before** enabling Login / Google.

## Recommended combo

| Layer | What it does |
|-------|----------------|
| **UptimeRobot** | Keeps services warm 24/7 (best for resume link) |
| **Login warmup** | Handles cold start if a service still slept |
| **Retries** | Google login retries 502/503 automatically |

## Verify

Open each URL in a browser — you should see JSON like `{"status":"ok",...}`:

- `.../health` on every service

Then open your app: `https://realtimechat-web-client.onrender.com/`
