/**
 * Wake sleeping Render services before login / channels (Option C).
 * Used on login.html and channels.html.
 */
async function runServiceWarmup(options = {}) {
    const {
        bannerId = null,
        warningId = null,
        maxWaitMs = 65000,
        onReady = null,
    } = options;

    const banner = bannerId ? document.getElementById(bannerId) : null;
    const warning = warningId ? document.getElementById(warningId) : null;

    const hideBanner = () => {
        if (banner) {
            banner.classList.add("d-none");
        }
    };

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), maxWaitMs);

    try {
        const response = await fetch("/api/warmup", { signal: controller.signal });
        const data = await response.json();
        hideBanner();

        if (!data.ready && warning) {
            const failed = Object.entries(data.services || {})
                .filter(([, ok]) => !ok)
                .map(([name]) => name)
                .join(", ");
            warning.textContent = failed
                ? `Some services are still waking (${failed}). Wait 30s and refresh.`
                : "Some services are still waking. Wait 30s and refresh.";
            warning.classList.remove("d-none");
        }
    } catch (error) {
        hideBanner();
        if (warning) {
            warning.textContent =
                "Warmup timed out. You can still try — wait 30s and refresh if login or chat fails.";
            warning.classList.remove("d-none");
        }
    } finally {
        clearTimeout(timeoutId);
        if (typeof onReady === "function") {
            onReady();
        }
    }
}
