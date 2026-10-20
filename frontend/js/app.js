/**
 * VORTEX // MASTER APPLICATION CONTROLLER
 * Coordinates UI states, API fetching, real-time analytics updates & navigation.
 */

class VortexApp {
    constructor() {
        this.activeTab = "overview";
        this.isStreaming = false;
        this.pollTimer = null;
        this.docPage = 0;
        this.docPageSize = 25;
    }

    async init() {
        console.log("⚡ VORTEX Big Data Engine Initializing...");
        
        // 1. Initialize Visualizer & Particles
        if (window.visualizer) {
            window.visualizer.initParticleCanvas();
            window.visualizer.initStreamChart();
            window.visualizer.initRegionChart();
        }

        // 2. Initialize Pipeline Studio
        if (window.pipelineStudio) {
            window.pipelineStudio.init();
        }

        // 3. Connect WebSocket
        if (window.wsClient) {
            window.wsClient.connect();
        }

        // 4. Attach Navigation & Action Listeners
        this.setupNavigation();
        this.setupActionButtons();

        // 5. Initial Data Hydration
        await this.refreshAllData();

        // 6. Start Periodic Refresh Loop (4s)
        this.pollTimer = setInterval(() => {
            if (this.activeTab === "overview") {
                this.refreshOverview();
            } else if (this.activeTab === "anomalies") {
                this.loadAnomalies();
            }
        }, 4000);

        if (window.lucide) {
            window.lucide.createIcons();
        }
    }

    setupNavigation() {
        const tabs = document.querySelectorAll(".nav-tab");
        tabs.forEach(tab => {
            tab.addEventListener("click", () => {
                const target = tab.dataset.tab;
                if (!target) return;
                
                tabs.forEach(t => t.classList.remove("active"));
                tab.classList.add("active");

                document.querySelectorAll(".tab-content").forEach(el => el.classList.add("hidden"));
                const targetEl = document.getElementById(`tab-${target}`);
                if (targetEl) targetEl.classList.remove("hidden");

                this.activeTab = target;
                if (window.soundEngine) window.soundEngine.playClick(650);

                // Re-render relevant charts on tab switch
                if (target === "overview") {
                    this.refreshOverview();
                } else if (target === "anomalies") {
                    this.loadAnomalies();
                } else if (target === "explorer") {
                    this.loadDocuments();
                }

                if (window.lucide) window.lucide.createIcons();
            });
        });
    }

    setupActionButtons() {
        // Toggle Audio
        const audioBtn = document.getElementById("toggleAudioBtn");
        if (audioBtn) {
            audioBtn.addEventListener("click", () => {
                if (window.soundEngine) {
                    const enabled = window.soundEngine.toggle();
                    audioBtn.innerHTML = enabled 
                        ? '<i data-lucide="volume-2" class="w-4 h-4 text-cyan-400"></i>' 
                        : '<i data-lucide="volume-x" class="w-4 h-4 text-slate-500"></i>';
                    if (window.lucide) window.lucide.createIcons();
                }
            });
        }

        // Toggle Streaming
        const streamBtn = document.getElementById("toggleStreamBtn");
        if (streamBtn) {
            streamBtn.addEventListener("click", () => this.toggleStreaming());
        }

        // Quick Seed 5k
        const quickSeedBtn = document.getElementById("quickSeedBtn");
        if (quickSeedBtn) {
            quickSeedBtn.addEventListener("click", () => this.seedData(5000));
        }

        // Ingestion Panel Controls
        const startStreamCtrl = document.getElementById("startStreamCtrl");
        if (startStreamCtrl) {
            startStreamCtrl.addEventListener("click", () => this.startStreamingFromPanel());
        }

        const stopStreamCtrl = document.getElementById("stopStreamCtrl");
        if (stopStreamCtrl) {
            stopStreamCtrl.addEventListener("click", () => this.stopStreaming());
        }

        const seedBatchCtrl = document.getElementById("seedBatchCtrl");
        if (seedBatchCtrl) {
            seedBatchCtrl.addEventListener("click", () => {
                const count = parseInt(document.getElementById("batchSizeSelect").value) || 5000;
                this.seedData(count);
            });
        }

        // Reset Data
        const purgeBtn = document.getElementById("purgeDatabaseBtn");
        if (purgeBtn) {
            purgeBtn.addEventListener("click", () => this.purgeData());
        }

        // Export Buttons
        const exportCsv = document.getElementById("exportCsvBtn");
        if (exportCsv) exportCsv.addEventListener("click", () => window.open("/api/export/csv", "_blank"));

        const exportJson = document.getElementById("exportJsonBtn");
        if (exportJson) exportJson.addEventListener("click", () => window.open("/api/export/json", "_blank"));

        const exportReport = document.getElementById("exportReportBtn");
        if (exportReport) exportReport.addEventListener("click", () => window.open("/api/export/report", "_blank"));

        // Modal for MongoDB Connection
        const openConfigBtn = document.getElementById("openMongoConfigBtn");
        const modal = document.getElementById("mongoConfigModal");
        const closeConfigBtn = document.getElementById("closeMongoConfigBtn");
        const applyUriBtn = document.getElementById("applyMongoUriBtn");

        if (openConfigBtn && modal) {
            openConfigBtn.addEventListener("click", () => modal.classList.remove("hidden"));
        }
        if (closeConfigBtn && modal) {
            closeConfigBtn.addEventListener("click", () => modal.classList.add("hidden"));
        }
        if (applyUriBtn) {
            applyUriBtn.addEventListener("click", () => this.updateMongoConnection());
        }

        // Explorer Pagination
        const prevPageBtn = document.getElementById("docPrevPageBtn");
        const nextPageBtn = document.getElementById("docNextPageBtn");
        if (prevPageBtn) prevPageBtn.addEventListener("click", () => {
            if (this.docPage > 0) {
                this.docPage--;
                this.loadDocuments();
            }
        });
        if (nextPageBtn) nextPageBtn.addEventListener("click", () => {
            this.docPage++;
            this.loadDocuments();
        });

        // Filter triggers
        ["docFilterRegion", "docFilterCategory", "docFilterStatus"].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.addEventListener("change", () => {
                this.docPage = 0;
                this.loadDocuments();
            });
        });
    }

    async refreshAllData() {
        await Promise.all([
            this.refreshOverview(),
            this.checkSystemStatus(),
            this.loadAnomalies(),
            this.loadInsights()
        ]);
    }

    async checkSystemStatus() {
        try {
            const res = await fetch("/api/status");
            const data = await res.json();
            
            // Mode Indicator
            const modeEl = document.getElementById("mongoModeBadge");
            if (modeEl) {
                if (data.database.mode === "live") {
                    modeEl.className = "px-2 py-0.5 rounded text-xs mono font-bold bg-emerald-500/20 text-emerald-300 border border-emerald-500/40";
                    modeEl.textContent = "LIVE MONGODB CLUSTER";
                } else {
                    modeEl.className = "px-2 py-0.5 rounded text-xs mono font-bold bg-cyan-500/20 text-cyan-300 border border-cyan-500/40";
                    modeEl.textContent = "EMBEDDED ZERO-CONFIG ENGINE";
                }
            }

            // Ping
            const pingEl = document.getElementById("mongoPingBadge");
            if (pingEl) pingEl.textContent = `${data.database.ping_latency_ms}ms PING`;

            // Stream state
            this.isStreaming = data.ingestion.is_streaming;
            this.updateStreamButtonUI();
        } catch (e) {
            console.error("Status check failed:", e);
        }
    }

    async refreshOverview() {
        try {
            // 1. Fetch Executive Overview KPIs
            const kpiRes = await fetch("/api/analytics/overview");
            const kpi = await kpiRes.json();

            const setTxt = (id, val) => {
                const el = document.getElementById(id);
                if (el) el.textContent = val;
            };

            setTxt("kpiTotalDocs", (kpi.total_documents || 0).toLocaleString());
            setTxt("kpiAvgLatency", `${kpi.avg_latency_ms || 0} ms`);
            setTxt("kpiP95Latency", `${kpi.p95_latency_ms || 0} ms`);
            setTxt("kpiP99Latency", `${kpi.p99_latency_ms || 0} ms`);
            setTxt("kpiAvgThroughput", `${kpi.avg_throughput_mbps || 0} Mbps`);
            setTxt("kpiHealthScore", `${kpi.health_score_pct || 100}%`);
            setTxt("kpiErrors", (kpi.total_errors || 0).toLocaleString());
            setTxt("kpiAnomalies", `${kpi.critical_events || 0} Crit / ${kpi.warning_events || 0} Warn`);

            // 2. Fetch Time Series Aggregation for Dual-Axis Chart
            const tsRes = await fetch("/api/analytics/timeseries?minutes=60");
            const tsData = await tsRes.json();
            if (window.visualizer) {
                window.visualizer.updateStreamChart(tsData);
            }

            // 3. Fetch Regional Aggregation
            const regRes = await fetch("/api/analytics/regions");
            const regData = await regRes.json();
            if (window.visualizer) {
                window.visualizer.updateRegionChart(regData);
            }

            // 4. Update Insights
            await this.loadInsights();

        } catch (e) {
            console.error("Overview refresh error:", e);
        }
    }

    async loadInsights() {
        try {
            const res = await fetch("/api/analytics/insights");
            const insights = await res.json();
            const container = document.getElementById("insightsContainer");
            if (!container) return;

            if (insights.length === 0) {
                container.innerHTML = '<div class="text-xs text-slate-500 py-4 mono">All operational parameters within standard variance.</div>';
                return;
            }

            container.innerHTML = insights.map(ins => {
                let badgeClass = "bg-cyan-500/20 text-cyan-300 border-cyan-500/40";
                if (ins.type === "WARNING") badgeClass = "bg-amber-500/20 text-amber-300 border-amber-500/40";
                if (ins.type === "DANGER") badgeClass = "bg-rose-500/20 text-rose-300 border-rose-500/40";
                if (ins.type === "SUCCESS") badgeClass = "bg-emerald-500/20 text-emerald-300 border-emerald-500/40";

                return `
                    <div class="glass-panel p-3.5 border-l-4 ${ins.type === 'DANGER' ? 'border-l-rose-500' : (ins.type === 'WARNING' ? 'border-l-amber-500' : 'border-l-cyan-400')}">
                        <div class="flex items-center justify-between mb-1.5">
                            <span class="px-2 py-0.5 rounded text-[10px] mono font-bold border ${badgeClass}">${ins.badge}</span>
                            <span class="text-xs font-semibold text-slate-200">${ins.title}</span>
                        </div>
                        <p class="text-xs text-slate-400 mb-2 leading-relaxed">${ins.summary}</p>
                        <div class="text-[11px] text-cyan-300/90 font-mono bg-cyan-950/30 p-2 rounded border border-cyan-900/40">
                            <strong>ACTION:</strong> ${ins.recommendation}
                        </div>
                    </div>
                `;
            }).join("");

        } catch (e) {
            console.error("Insights load error:", e);
        }
    }

    async loadAnomalies() {
        try {
            const res = await fetch("/api/analytics/anomalies?limit=50");
            const anomalies = await res.json();
            const container = document.getElementById("anomalyAlertFeed");
            if (!container) return;

            if (anomalies.length === 0) {
                container.innerHTML = '<div class="text-xs text-slate-500 py-8 text-center mono">No active anomaly alerts detected in lakehouse.</div>';
                return;
            }

            container.innerHTML = anomalies.map(anom => {
                const isCrit = anom.severity === "CRITICAL";
                const bg = isCrit ? "bg-rose-950/20 border-rose-800/40" : "bg-amber-950/20 border-amber-800/40";
                const badge = isCrit ? "text-rose-400 bg-rose-500/20 border-rose-500/40" : "text-amber-400 bg-amber-500/20 border-amber-500/40";

                return `
                    <div class="p-3 rounded-lg border ${bg} flex items-center justify-between text-xs transition hover:border-cyan-400/50">
                        <div class="space-y-1">
                            <div class="flex items-center space-x-2">
                                <span class="px-1.5 py-0.5 rounded text-[10px] mono font-bold border ${badge}">${anom.severity}</span>
                                <span class="font-semibold text-slate-200">${anom.source_node}</span>
                                <span class="text-slate-500 mono">[${anom.region}]</span>
                            </div>
                            <div class="text-[11px] text-slate-400 mono">
                                Latency: <strong class="text-amber-300">${anom.metrics.latency_ms}ms</strong> | 
                                CPU: <strong class="text-rose-300">${anom.metrics.cpu_utilization}%</strong> | 
                                Errors: <strong class="text-rose-400">${anom.metrics.error_count}</strong>
                            </div>
                        </div>
                        <div class="text-right mono text-[10px] text-slate-500">
                            ${anom.timestamp ? anom.timestamp.substring(11, 19) : ''}
                        </div>
                    </div>
                `;
            }).join("");

        } catch (e) {
            console.error("Anomalies load error:", e);
        }
    }

    async loadDocuments() {
        const region = document.getElementById("docFilterRegion")?.value || "";
        const category = document.getElementById("docFilterCategory")?.value || "";
        const status = document.getElementById("docFilterStatus")?.value || "";

        let url = `/api/data/documents?skip=${this.docPage * this.docPageSize}&limit=${this.docPageSize}`;
        if (region) url += `&region=${encodeURIComponent(region)}`;
        if (category) url += `&category=${encodeURIComponent(category)}`;
        if (status) url += `&status=${encodeURIComponent(status)}`;

        try {
            const res = await fetch(url);
            const data = await res.json();
            const container = document.getElementById("documentsTableBody");
            const pageIndicator = document.getElementById("docPageIndicator");

            if (pageIndicator) {
                const totalPages = Math.ceil(data.total / this.docPageSize) || 1;
                pageIndicator.textContent = `Page ${this.docPage + 1} of ${totalPages} (${data.total.toLocaleString()} records)`;
            }

            if (!container) return;
            if (data.documents.length === 0) {
                container.innerHTML = '<tr><td colspan="6" class="text-center py-8 text-slate-500 mono">No matching documents found in MongoDB collection.</td></tr>';
                return;
            }

            container.innerHTML = data.documents.map(doc => {
                const m = doc.metrics || {};
                const statusColor = doc.status === "CRITICAL" ? "text-rose-400 font-bold" : (doc.status === "WARNING" ? "text-amber-400" : "text-emerald-400");
                return `
                    <tr class="border-b border-slate-800/60 hover:bg-slate-800/30 transition mono text-xs">
                        <td class="py-2.5 px-3 text-cyan-400">${doc.event_id}</td>
                        <td class="py-2.5 px-3 text-slate-400">${doc.source_node}</td>
                        <td class="py-2.5 px-3 text-slate-300">${doc.region}</td>
                        <td class="py-2.5 px-3 text-amber-300">${m.latency_ms} ms</td>
                        <td class="py-2.5 px-3 text-purple-300">${m.throughput_mbps} Mbps</td>
                        <td class="py-2.5 px-3 ${statusColor}">${doc.status}</td>
                    </tr>
                `;
            }).join("");

        } catch (e) {
            console.error("Documents load error:", e);
        }
    }

    async toggleStreaming() {
        if (this.isStreaming) {
            await this.stopStreaming();
        } else {
            await this.startStreamingFromPanel();
        }
    }

    async startStreamingFromPanel() {
        const rate = parseInt(document.getElementById("streamRateSlider")?.value || "250");
        const anomRate = parseFloat(document.getElementById("streamAnomalySlider")?.value || "0.04");

        try {
            const res = await fetch("/api/ingestion/start", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    events_per_second: rate,
                    anomaly_probability: anomRate
                })
            });
            const data = await res.json();
            if (data.status === "started") {
                this.isStreaming = true;
                this.updateStreamButtonUI();
                if (window.soundEngine) window.soundEngine.playStreamToggle(true);
            }
        } catch (e) {
            console.error("Start stream error:", e);
        }
    }

    async stopStreaming() {
        try {
            const res = await fetch("/api/ingestion/stop", { method: "POST" });
            const data = await res.json();
            if (data.status === "stopped") {
                this.isStreaming = false;
                this.updateStreamButtonUI();
                if (window.soundEngine) window.soundEngine.playStreamToggle(false);
            }
        } catch (e) {
            console.error("Stop stream error:", e);
        }
    }

    updateStreamButtonUI() {
        const topBtn = document.getElementById("toggleStreamBtn");
        const statusPill = document.getElementById("streamStatusPill");

        if (this.isStreaming) {
            if (topBtn) {
                topBtn.className = "px-3 py-1.5 rounded-lg border border-rose-500 bg-rose-500/20 text-rose-300 mono text-xs font-semibold flex items-center space-x-1.5 hover:bg-rose-500/30 transition shadow-lg shadow-rose-500/20";
                topBtn.innerHTML = '<span class="w-2 h-2 rounded-full bg-rose-400 beacon-pulse mr-1"></span> STOP STREAM';
            }
            if (statusPill) {
                statusPill.className = "px-2 py-0.5 rounded text-xs mono font-bold bg-emerald-500/20 text-emerald-300 border border-emerald-500/40";
                statusPill.textContent = "INGESTION STREAM: ACTIVE";
            }
        } else {
            if (topBtn) {
                topBtn.className = "px-3 py-1.5 rounded-lg border border-cyan-500/40 bg-cyan-500/10 text-cyan-300 mono text-xs font-semibold flex items-center space-x-1.5 hover:bg-cyan-500/20 transition";
                topBtn.innerHTML = '<i data-lucide="play" class="w-3.5 h-3.5 mr-1 inline"></i> START STREAM';
            }
            if (statusPill) {
                statusPill.className = "px-2 py-0.5 rounded text-xs mono font-bold bg-slate-800 text-slate-400 border border-slate-700";
                statusPill.textContent = "INGESTION STREAM: IDLE";
            }
        }
        if (window.lucide) window.lucide.createIcons();
    }

    async seedData(count = 5000) {
        const btn = document.getElementById("quickSeedBtn");
        const originalText = btn ? btn.innerHTML : "";
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<span class="animate-spin inline-block mr-1">⚙</span> INGESTING...';
        }
        if (window.soundEngine) window.soundEngine.playExecute();

        try {
            const res = await fetch("/api/ingestion/batch", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ count: count, anomaly_probability: 0.04 })
            });
            const data = await res.json();
            await this.refreshAllData();
            if (window.soundEngine) window.soundEngine.playClick(950);
        } catch (e) {
            console.error("Seed error:", e);
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = originalText;
                if (window.lucide) window.lucide.createIcons();
            }
        }
    }

    async purgeData() {
        if (!confirm("Are you sure you want to truncate the MongoDB big data telemetry collection?")) return;
        try {
            await fetch("/api/data/clear", { method: "DELETE" });
            this.isStreaming = false;
            this.updateStreamButtonUI();
            await this.refreshAllData();
            if (window.soundEngine) window.soundEngine.playAlert();
        } catch (e) {
            console.error("Purge error:", e);
        }
    }

    async updateMongoConnection() {
        const input = document.getElementById("mongoUriInput");
        const statusEl = document.getElementById("mongoConnectStatus");
        if (!input) return;

        const uri = input.value.trim();
        if (!uri) return;

        if (statusEl) statusEl.innerHTML = '<span class="text-cyan-400 mono text-xs">Pinging MongoDB cluster...</span>';

        try {
            const res = await fetch("/api/connect", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ uri: uri })
            });
            const data = await res.json();
            if (statusEl) {
                statusEl.innerHTML = `<span class="text-emerald-400 mono text-xs">${data.message}</span>`;
            }
            await this.checkSystemStatus();
            setTimeout(() => {
                const modal = document.getElementById("mongoConfigModal");
                if (modal) modal.classList.add("hidden");
            }, 1200);
        } catch (e) {
            if (statusEl) {
                statusEl.innerHTML = `<span class="text-rose-400 mono text-xs">Failed to connect: ${e.message}</span>`;
            }
        }
    }
}

// Global bootstrap
window.addEventListener("DOMContentLoaded", () => {
    window.vortex = new VortexApp();
    window.vortex.init();
});
