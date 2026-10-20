/**
 * VORTEX // WEBSOCKET TELEMETRY STREAM CLIENT
 * Live bidirectional telemetry channel with auto-reconnection
 */

class VortexWebSocketClient {
    constructor() {
        this.socket = null;
        this.reconnectTimer = null;
        this.isConnected = false;
    }

    connect() {
        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl = `${protocol}//${window.location.host}/ws/telemetry`;

        try {
            this.socket = new WebSocket(wsUrl);

            this.socket.onopen = () => {
                this.isConnected = true;
                const statusDot = document.getElementById("wsStatusDot");
                if (statusDot) {
                    statusDot.className = "w-2.5 h-2.5 rounded-full bg-emerald-400 beacon-pulse";
                    statusDot.title = "WebSocket Connected: Live Stream Active";
                }
                console.log("[VORTEX WS] Connected to live telemetry stream");
            };

            this.socket.onmessage = (event) => {
                try {
                    const msg = JSON.parse(event.data);
                    if (msg.type === "TELEMETRY_PULSE") {
                        this.handlePulse(msg);
                    }
                } catch (e) {
                    console.error("[VORTEX WS] Message parse error:", e);
                }
            };

            this.socket.onclose = () => {
                this.isConnected = false;
                const statusDot = document.getElementById("wsStatusDot");
                if (statusDot) {
                    statusDot.className = "w-2.5 h-2.5 rounded-full bg-rose-500";
                    statusDot.title = "WebSocket Disconnected. Reconnecting...";
                }
                this.scheduleReconnect();
            };

            this.socket.onerror = (err) => {
                console.warn("[VORTEX WS] Socket error:", err);
            };

        } catch (e) {
            console.error("[VORTEX WS] Connection error:", e);
            this.scheduleReconnect();
        }
    }

    scheduleReconnect() {
        if (this.reconnectTimer) return;
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            this.connect();
        }, 3000);
    }

    handlePulse(data) {
        const ing = data.ingestion;
        const stor = data.storage;

        // 1. Update Ingestion HUD
        const opsEl = document.getElementById("hudOpsSec");
        if (opsEl) {
            opsEl.textContent = Math.round(ing.current_ops_sec).toLocaleString();
        }

        const countEl = document.getElementById("kpiTotalDocs");
        if (countEl && stor.raw_documents_count !== undefined) {
            countEl.textContent = stor.raw_documents_count.toLocaleString();
        }

        const sizeEl = document.getElementById("hudStorageSize");
        if (sizeEl && stor.estimated_size_mb !== undefined) {
            sizeEl.textContent = `${stor.estimated_size_mb} MB`;
        }

        // 2. Feed Live Terminal Waterfall
        if (ing.recent_events && ing.recent_events.length > 0) {
            const logBox = document.getElementById("liveLogWaterfall");
            if (logBox) {
                const latest = ing.recent_events[0];
                const itemEl = document.createElement("div");
                itemEl.className = "log-item flex items-center justify-between text-xs py-0.5 border-b border-slate-800/40";
                
                const statusColor = latest.status === "CRITICAL" ? "text-rose-400 font-bold" : (latest.status === "WARNING" ? "text-amber-400" : "text-emerald-400");
                
                itemEl.innerHTML = `
                    <div class="flex items-center space-x-2 truncate">
                        <span class="text-slate-500">[${latest.timestamp.substring(11, 19)}]</span>
                        <span class="text-cyan-400">${latest.source_node}</span>
                        <span class="text-slate-400">${latest.region}</span>
                    </div>
                    <div class="flex items-center space-x-3 mono">
                        <span class="text-slate-300">${latest.throughput_mbps} Mbps</span>
                        <span class="text-amber-300">${latest.latency_ms}ms</span>
                        <span class="${statusColor}">${latest.status}</span>
                    </div>
                `;
                logBox.insertBefore(itemEl, logBox.firstChild);
                
                // Keep max 25 items in log
                while (logBox.children.length > 25) {
                    logBox.removeChild(logBox.lastChild);
                }
            }
        }
    }
}

window.wsClient = new VortexWebSocketClient();
