/**
 * VORTEX // VISUALIZATION ENGINE
 * Canvas particle grid & Chart.js dynamic telemetry charts
 */

class VisualizerEngine {
    constructor() {
        this.streamChart = null;
        this.regionChart = null;
        this.pipelineChart = null;
        this.anomalyChart = null;
        this.particles = [];
        this.animFrame = null;
    }

    /**
     * Initializes the dynamic background particle physics simulation.
     */
    initParticleCanvas() {
        const canvas = document.getElementById("vortex-canvas");
        if (!canvas) return;
        const ctx = canvas.getContext("2d");

        const resize = () => {
            canvas.width = window.innerWidth;
            canvas.height = window.innerHeight;
        };
        resize();
        window.addEventListener("resize", resize);

        // Populate particle swarm
        const count = Math.min(65, Math.floor(window.innerWidth / 25));
        this.particles = [];
        for (let i = 0; i < count; i++) {
            this.particles.push({
                x: Math.random() * canvas.width,
                y: Math.random() * canvas.height,
                vx: (Math.random() - 0.5) * 0.4,
                vy: (Math.random() - 0.5) * 0.4,
                radius: Math.random() * 1.8 + 0.8,
                color: Math.random() > 0.4 ? "rgba(0, 240, 255, " : "rgba(168, 85, 247, ",
                alpha: Math.random() * 0.4 + 0.1
            });
        }

        const render = () => {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            
            // Draw connections
            for (let i = 0; i < this.particles.length; i++) {
                const p1 = this.particles[i];
                p1.x += p1.vx;
                p1.y += p1.vy;

                if (p1.x < 0) p1.x = canvas.width;
                if (p1.x > canvas.width) p1.x = 0;
                if (p1.y < 0) p1.y = canvas.height;
                if (p1.y > canvas.height) p1.y = 0;

                ctx.beginPath();
                ctx.arc(p1.x, p1.y, p1.radius, 0, Math.PI * 2);
                ctx.fillStyle = p1.color + p1.alpha + ")";
                ctx.fill();

                for (let j = i + 1; j < this.particles.length; j++) {
                    const p2 = this.particles[j];
                    const dx = p1.x - p2.x;
                    const dy = p1.y - p2.y;
                    const dist = Math.sqrt(dx * dx + dy * dy);

                    if (dist < 120) {
                        ctx.beginPath();
                        ctx.strokeStyle = `rgba(0, 240, 255, ${0.15 * (1 - dist / 120)})`;
                        ctx.lineWidth = 0.6;
                        ctx.moveTo(p1.x, p1.y);
                        ctx.lineTo(p2.x, p2.y);
                        ctx.stroke();
                    }
                }
            }
            this.animFrame = requestAnimationFrame(render);
        };
        render();
    }

    /**
     * Initializes the Real-time Telemetry Stream Chart (Dual-Axis)
     */
    initStreamChart() {
        const ctx = document.getElementById("streamChart");
        if (!ctx) return;

        this.streamChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Throughput (Mbps)',
                        borderColor: '#00f0ff',
                        backgroundColor: 'rgba(0, 240, 255, 0.08)',
                        borderWidth: 2,
                        pointRadius: 2,
                        pointHoverRadius: 5,
                        fill: true,
                        tension: 0.35,
                        yAxisID: 'yThroughput',
                        data: []
                    },
                    {
                        label: 'Avg Latency (ms)',
                        borderColor: '#ffb800',
                        backgroundColor: 'rgba(255, 184, 0, 0.04)',
                        borderWidth: 2,
                        borderDash: [4, 4],
                        pointRadius: 2,
                        fill: false,
                        tension: 0.35,
                        yAxisID: 'yLatency',
                        data: []
                    },
                    {
                        label: 'Max Latency Peak (ms)',
                        borderColor: '#ff3366',
                        backgroundColor: 'rgba(255, 51, 102, 0.2)',
                        borderWidth: 1.5,
                        pointRadius: 3,
                        pointBackgroundColor: '#ff3366',
                        fill: false,
                        tension: 0.2,
                        yAxisID: 'yLatency',
                        data: []
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 400 },
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: {
                        labels: { color: '#94a3b8', font: { family: 'Space Grotesk', size: 11 } }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(15, 23, 42, 0.9)',
                        titleColor: '#00f0ff',
                        borderColor: 'rgba(0, 240, 255, 0.3)',
                        borderWidth: 1
                    }
                },
                scales: {
                    x: {
                        grid: { color: 'rgba(255, 255, 255, 0.04)' },
                        ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 10 } }
                    },
                    yThroughput: {
                        type: 'linear',
                        position: 'left',
                        title: { display: true, text: 'Mbps', color: '#00f0ff' },
                        grid: { color: 'rgba(255, 255, 255, 0.04)' },
                        ticks: { color: '#00f0ff', font: { family: 'JetBrains Mono', size: 10 } }
                    },
                    yLatency: {
                        type: 'linear',
                        position: 'right',
                        title: { display: true, text: 'ms', color: '#ffb800' },
                        grid: { drawOnChartArea: false },
                        ticks: { color: '#ffb800', font: { family: 'JetBrains Mono', size: 10 } }
                    }
                }
            }
        });
    }

    updateStreamChart(timeSeriesData) {
        if (!this.streamChart || !timeSeriesData || !timeSeriesData.length) return;

        const labels = timeSeriesData.map(d => d.time);
        const throughputs = timeSeriesData.map(d => d.avg_throughput);
        const latencies = timeSeriesData.map(d => d.avg_latency);
        const maxLatencies = timeSeriesData.map(d => d.max_latency);

        this.streamChart.data.labels = labels;
        this.streamChart.data.datasets[0].data = throughputs;
        this.streamChart.data.datasets[1].data = latencies;
        this.streamChart.data.datasets[2].data = maxLatencies;
        this.streamChart.update('none');
    }

    /**
     * Initializes Regional Breakdown Bar Chart
     */
    initRegionChart() {
        const ctx = document.getElementById("regionChart");
        if (!ctx) return;

        this.regionChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Total Events',
                        backgroundColor: 'rgba(0, 240, 255, 0.45)',
                        borderColor: '#00f0ff',
                        borderWidth: 1,
                        borderRadius: 4,
                        data: []
                    },
                    {
                        label: 'Critical Faults',
                        backgroundColor: 'rgba(255, 51, 102, 0.65)',
                        borderColor: '#ff3366',
                        borderWidth: 1,
                        borderRadius: 4,
                        data: []
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        labels: { color: '#94a3b8', font: { family: 'Space Grotesk', size: 11 } }
                    }
                },
                scales: {
                    x: {
                        grid: { color: 'rgba(255, 255, 255, 0.04)' },
                        ticks: { color: '#64748b', font: { family: 'Space Grotesk', size: 10 } }
                    },
                    y: {
                        grid: { color: 'rgba(255, 255, 255, 0.04)' },
                        ticks: { color: '#94a3b8', font: { family: 'JetBrains Mono', size: 10 } }
                    }
                }
            }
        });
    }

    updateRegionChart(regionData) {
        if (!this.regionChart || !regionData) return;
        const labels = regionData.map(r => r.region.replace("us-", "").replace("eu-", "").replace("ap-", "").replace("sa-", ""));
        const totals = regionData.map(r => r.total_events);
        const criticals = regionData.map(r => r.critical_events);

        this.regionChart.data.labels = labels;
        this.regionChart.data.datasets[0].data = totals;
        this.regionChart.data.datasets[1].data = criticals;
        this.regionChart.update();
    }

    /**
     * Dynamically plots user-defined pipeline output
     */
    renderPipelineChart(containerId, results) {
        const container = document.getElementById(containerId);
        if (!container || !results || !results.length) return;

        // Reset canvas
        container.innerHTML = '<canvas id="pipelineResultCanvas" class="w-full h-64"></canvas>';
        const ctx = document.getElementById("pipelineResultCanvas");

        // Inspect first document to discover label key and numeric keys
        const first = results[0];
        let labelKey = "_id";
        const numericKeys = [];

        for (const [k, v] of Object.entries(first)) {
            if (typeof v === "number") {
                numericKeys.push(k);
            }
        }

        if (numericKeys.length === 0) {
            container.innerHTML = '<div class="text-xs text-slate-400 py-6 text-center mono">No numeric metric fields found in aggregation output to plot. See raw JSON below.</div>';
            return;
        }

        const labels = results.map(r => {
            const v = r[labelKey];
            if (typeof v === "object" && v !== null) return JSON.stringify(v);
            return String(v || "Row");
        });

        const colors = ['#00f0ff', '#00ffa3', '#ffb800', '#a855f7', '#ff3366'];
        const datasets = numericKeys.slice(0, 3).map((key, idx) => ({
            label: key,
            data: results.map(r => r[key] || 0),
            backgroundColor: colors[idx % colors.length] + '40',
            borderColor: colors[idx % colors.length],
            borderWidth: 1.5,
            borderRadius: 4
        }));

        if (this.pipelineChart) {
            this.pipelineChart.destroy();
        }

        this.pipelineChart = new Chart(ctx, {
            type: 'bar',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { labels: { color: '#cbd5e1', font: { family: 'Space Grotesk', size: 11 } } }
                },
                scales: {
                    x: {
                        grid: { color: 'rgba(255, 255, 255, 0.04)' },
                        ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 10 } }
                    },
                    y: {
                        grid: { color: 'rgba(255, 255, 255, 0.04)' },
                        ticks: { color: '#94a3b8', font: { family: 'JetBrains Mono', size: 10 } }
                    }
                }
            }
        });
    }
}

window.visualizer = new VisualizerEngine();
