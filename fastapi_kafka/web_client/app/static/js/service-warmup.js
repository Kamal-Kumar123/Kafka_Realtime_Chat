/**
 * Built-in Render cold-start warmup (no UptimeRobot required).
 * Login page starts wake in background; this waits until backends respond.
 */
async function runServiceWarmup(options = {}) {
    const {
        bannerId = null,
        warningId = null,
        maxWaitMs = 95000,
        onReady = null,
    } = options;

    const banner = bannerId ? document.getElementById(bannerId) : null;
    const warning = warningId ? document.getElementById(warningId) : null;

    const setBannerText = (text) => {
        if (banner) {
            const label = banner.querySelector(".warmup-text") || banner;
            if (label.classList && label.classList.contains("warmup-text")) {
                label.textContent = text;
            } else if (banner.childNodes.length > 1) {
                banner.lastChild.textContent = " " + text;
            }
        }
    };

    const hideBanner = () => {
        if (banner) {
            banner.classList.add("d-none");
        }
    };

    setBannerText("Waking login, channel, and chat services…");

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), maxWaitMs);

    try {
        const response = await fetch("/api/warmup", { signal: controller.signal });
        const data = await response.json();
        hideBanner();

        if (data.missing_env_on_web_client && data.missing_env_on_web_client.length && warning) {
            warning.textContent =
                "Missing on web_client Render env: " +
                data.missing_env_on_web_client.join(", ") +
                ". Set them and redeploy.";
            warning.classList.remove("d-none");
        } else if (!data.ready && warning) {
            const failed = Object.entries(data.services || {})
                .filter(([, ok]) => !ok)
                .map(([name]) => name)
                .join(", ");
            warning.textContent = failed
                ? `Still waking: ${failed}. Wait 30s and refresh.`
                : "Some services are still waking. Wait 30s and refresh.";
            warning.classList.remove("d-none");
        }
    } catch (error) {
        hideBanner();
        if (warning) {
            warning.textContent =
                "Warmup timed out. Wait 30s, refresh the page, then try again.";
            warning.classList.remove("d-none");
        }
    } finally {
        clearTimeout(timeoutId);
        if (typeof onReady === "function") {
            onReady();
        }
    }
}
