/**
 * VORTEX // PIPELINE STUDIO CONTROLLER
 * Interactive MongoDB Aggregation framework workbench
 */

class PipelineStudio {
    constructor() {
        this.presets = [];
        this.currentPipeline = [];
        this.editorEl = null;
    }

    async init() {
        this.editorEl = document.getElementById("pipelineJsonEditor");
        await this.loadPresets();
        this.setupEventListeners();
    }

    async loadPresets() {
        try {
            const res = await fetch("/api/pipelines/presets");
            this.presets = await res.json();
            const selectEl = document.getElementById("presetSelector");
            if (selectEl) {
                selectEl.innerHTML = this.presets.map((p, idx) => `
                    <option value="${p.id}" ${idx === 0 ? "selected" : ""}>${p.name}</option>
                `).join("");
                
                // Load first preset into editor
                if (this.presets.length > 0) {
                    this.applyPreset(this.presets[0].id);
                }
            }
        } catch (e) {
            console.error("Error loading pipeline presets:", e);
        }
    }

    applyPreset(presetId) {
        const found = this.presets.find(p => p.id === presetId);
        if (!found || !this.editorEl) return;

        this.currentPipeline = found.stages;
        this.editorEl.value = JSON.stringify(found.stages, null, 2);
        
        const descEl = document.getElementById("presetDescription");
        if (descEl) {
            descEl.textContent = found.description;
        }
    }

    setupEventListeners() {
        const selectEl = document.getElementById("presetSelector");
        if (selectEl) {
            selectEl.addEventListener("change", (e) => {
                this.applyPreset(e.target.value);
                if (window.soundEngine) window.soundEngine.playClick(750);
            });
        }

        const runBtn = document.getElementById("runPipelineBtn");
        if (runBtn) {
            runBtn.addEventListener("click", () => this.execute());
        }

        const formatBtn = document.getElementById("formatJsonBtn");
        if (formatBtn) {
            formatBtn.addEventListener("click", () => this.formatJson());
        }

        const copyBtn = document.getElementById("copyResultsBtn");
        if (copyBtn) {
            copyBtn.addEventListener("click", () => this.copyResults());
        }
    }

    formatJson() {
        if (!this.editorEl) return;
        try {
            const parsed = JSON.parse(this.editorEl.value);
            this.editorEl.value = JSON.stringify(parsed, null, 2);
            if (window.soundEngine) window.soundEngine.playClick(900);
        } catch (e) {
            alert("Invalid JSON: " + e.message);
        }
    }

    async execute() {
        if (!this.editorEl) return;
        
        let stages;
        try {
            stages = JSON.parse(this.editorEl.value);
            if (!Array.isArray(stages)) {
                throw new Error("Pipeline must be a JSON array of stage objects (e.g. [{$match: ...}, {$group: ...}])");
            }
        } catch (e) {
            alert("JSON Syntax Error: " + e.message);
            return;
        }

        const runBtn = document.getElementById("runPipelineBtn");
        const statusEl = document.getElementById("pipelineStatusHUD");
        if (runBtn) {
            runBtn.disabled = true;
            runBtn.innerHTML = '<span class="animate-spin inline-block mr-2">⚙</span> EXECUTING...';
        }

        if (window.soundEngine) window.soundEngine.playExecute();

        try {
            const res = await fetch("/api/pipelines/execute", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ pipeline_json: stages })
            });
            const data = await res.json();

            // Update Metrics HUD
            if (statusEl) {
                if (data.status === "success") {
                    statusEl.innerHTML = `
                        <div class="flex items-center space-x-4 mono text-xs">
                            <span class="text-emerald-400 font-semibold">STATUS: OK</span>
                            <span class="text-slate-400">EXEC TIME: <strong class="text-cyan-400">${data.execution_time_ms} ms</strong></span>
                            <span class="text-slate-400">DOCS: <strong class="text-purple-400">${data.documents_returned}</strong></span>
                        </div>
                    `;
                } else {
                    statusEl.innerHTML = `
                        <div class="mono text-xs text-rose-400 font-semibold">
                            ERROR: ${data.error_message || "Execution failed"} (${data.execution_time_ms} ms)
                        </div>
                    `;
                }
            }

            // Display Results in Code View
            const rawResultsEl = document.getElementById("pipelineRawResults");
            if (rawResultsEl) {
                rawResultsEl.textContent = JSON.stringify(data.data, null, 2);
            }

            // Render Dynamic Chart
            if (data.status === "success" && data.data && data.data.length > 0) {
                window.visualizer.renderPipelineChart("pipelineChartContainer", data.data);
            } else if (data.status === "success") {
                const chartCont = document.getElementById("pipelineChartContainer");
                if (chartCont) chartCont.innerHTML = '<div class="text-xs text-slate-500 py-8 text-center mono">Pipeline completed successfully with 0 documents returned matching criteria.</div>';
            }

        } catch (e) {
            console.error("Pipeline run error:", e);
        } finally {
            if (runBtn) {
                runBtn.disabled = false;
                runBtn.innerHTML = '<i data-lucide="play" class="w-4 h-4 mr-1.5 inline"></i> EXECUTE PIPELINE';
                if (window.lucide) window.lucide.createIcons();
            }
        }
    }

    copyResults() {
        const rawEl = document.getElementById("pipelineRawResults");
        if (!rawEl) return;
        navigator.clipboard.writeText(rawEl.textContent).then(() => {
            const btn = document.getElementById("copyResultsBtn");
            if (btn) {
                const original = btn.innerHTML;
                btn.innerHTML = '<span class="text-emerald-400 mono text-xs">COPIED!</span>';
                setTimeout(() => { btn.innerHTML = original; }, 1500);
            }
        });
    }
}

window.pipelineStudio = new PipelineStudio();
