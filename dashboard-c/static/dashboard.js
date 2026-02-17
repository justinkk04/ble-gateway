// dashboard.js — BLE Mesh Dashboard v0.8 (Tabbed)

// =====================================================================
//  Global State
// =====================================================================

let currentState = null;
let simulation = null;
let nodeSelection = null;
let linkSelection = null;
let topoSelectedId = null;  // Topology tab selection

// Nodes tab
let nodesSelectedId = null;  // Which node card is selected
let chartData = [];
let chartMetric = 'power_mw';
let chartWindow = 30;  // minutes

// History tab
let historyOffset = 0;
let historyTotal = 0;
let historyLimit = 200;
let historyNodeFilter = '';
let knownNodeIds = new Set();

// Console tab
let consoleHistory = [];    // Command history for up/down
let consoleHistIdx = -1;
let consoleLogCount = 0;

// Settings / Aliases
let nodeAliases = {};       // node_id -> alias name

// Active tab tracking
let activeTab = 'topology';

// Helper: get display name for a node
function nodeLabel(nid) {
    return nodeAliases[nid] || `Node ${nid}`;
}

// Load saved theme + aliases on startup
(async function initSettings() {
    try {
        const [settingsResp, aliasResp] = await Promise.all([
            fetch('/api/settings'),
            fetch('/api/nodes/aliases')
        ]);
        if (settingsResp.ok) {
            const s = await settingsResp.json();
            if (s.theme === 'light') {
                document.body.classList.add('light');
            }
        }
        if (aliasResp.ok) {
            nodeAliases = await aliasResp.json();
        }
    } catch (e) { /* settings load failed, use defaults */ }
})();

const COLORS = {
    pi5: '#bc8cff',
    gateway: '#d29922',
    sensing: '#3fb950',
    relay: '#58a6ff',
    disconnected: '#f85149',
    stale: '#d29922'
};

const METRIC_COLORS = {
    power_mw: '#3fb950',
    voltage: '#58a6ff',
    current_ma: '#d29922',
    duty: '#bc8cff',
};

const METRIC_LABELS = {
    power_mw: 'Power (mW)',
    voltage: 'Voltage (V)',
    current_ma: 'Current (mA)',
    duty: 'Duty (%)',
};


// =====================================================================
//  Tab Switching
// =====================================================================

document.querySelectorAll('#tab-bar .tab').forEach(btn => {
    btn.addEventListener('click', () => {
        const tab = btn.dataset.tab;
        if (tab === activeTab) return;

        // Update buttons
        document.querySelectorAll('#tab-bar .tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');

        // Update panels
        document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
        document.getElementById(`panel-${tab}`).classList.add('active');

        activeTab = tab;

        // Tab-specific init
        if (tab === 'topology') {
            // Restart simulation gently since container was hidden
            setTimeout(() => {
                if (simulation) simulation.alpha(0.3).restart();
            }, 50);
        } else if (tab === 'nodes' && nodesSelectedId) {
            fetchNodeChart(nodesSelectedId);
        } else if (tab === 'history') {
            fetchHistory();
        } else if (tab === 'console') {
            document.getElementById('console-input').focus();
        } else if (tab === 'settings') {
            loadSettingsTab();
        }
    });
});


// =====================================================================
//  Polling
// =====================================================================

async function pollState() {
    try {
        const resp = await fetch('/api/state');
        if (resp.ok) {
            const data = await resp.json();
            updateDashboard(data);
        }
    } catch (e) { console.error("Fetch error:", e); }
}

setInterval(pollState, 2000);
pollState();

// History auto-refresh (only when tab is active)
setInterval(() => { if (activeTab === 'history') fetchHistory(); }, 5000);
setTimeout(fetchHistory, 600);

// Nodes chart auto-refresh
setInterval(() => {
    if (activeTab === 'nodes' && nodesSelectedId) fetchNodeChart(nodesSelectedId);
}, 5000);


// =====================================================================
//  Master Update
// =====================================================================

function updateDashboard(state) {
    currentState = state;
    updateHeader(state);
    updateStatusBar(state);
    updateNodeFilterDropdown(state);

    // Only update visible tab's heavy rendering
    if (activeTab === 'topology') {
        updateGraph(state);
        updateTopoSidebar(state);
    }
    if (activeTab === 'nodes') {
        updateNodeCards(state);
    }
    // History and console have their own polling
}


// =====================================================================
//  Header / Footer
// =====================================================================

function updateHeader(state) {
    const dot = document.querySelector('.status-dot');
    const txt = document.getElementById('connection-text');
    const overlay = document.getElementById('disconnect-overlay');

    const age = (Date.now() - new Date(state.timestamp).getTime()) / 1000;

    if (state.gateway.connected) {
        if (age > 10) {
            dot.className = 'status-dot stale';
            txt.textContent = `Stale ${age.toFixed(0)}s`;
        } else {
            dot.className = 'status-dot connected';
            txt.textContent = 'Connected';
        }
        overlay.style.display = 'none';
    } else {
        dot.className = 'status-dot disconnected';
        txt.textContent = 'Disconnected';
        overlay.style.display = 'flex';
    }
}

function updateStatusBar(state) {
    const n = Object.keys(state.nodes).length;
    const r = state.relay_nodes || 0;
    document.getElementById('node-summary').textContent = `${n} Sensing, ${r} Relay`;
    document.getElementById('last-update').textContent = state.timestamp;
}


// =====================================================================
//  TAB 1: Topology (D3 force graph)
// =====================================================================

// Lazy-init D3 on first use
let topoInitialized = false;

function initTopology() {
    if (topoInitialized) return;
    topoInitialized = true;

    const container = document.getElementById('graph-container');
    const w = container.clientWidth;
    const h = container.clientHeight;
    const svg = d3.select('#mesh-graph');

    svg.on('click', (e) => {
        if (e.target === svg.node()) {
            topoSelectedId = null;
            document.getElementById('topo-node-content').innerHTML =
                '<p class="placeholder">Click a node to inspect</p>';
        }
    });

    simulation = d3.forceSimulation()
        .force('link', d3.forceLink().id(d => d.id).distance(d => {
            if (d.linkType === 'gatt') return 60;
            if (d.linkType === 'mesh-relay') return 120;
            return 100;
        }))
        .force('charge', d3.forceManyBody().strength(-300))
        .force('center', d3.forceCenter(w / 2, h / 2))
        .force('collision', d3.forceCollide().radius(40));

    const ro = new ResizeObserver(entries => {
        for (const entry of entries) {
            const { width, height } = entry.contentRect;
            if (width > 0 && height > 0) {
                simulation.force('center', d3.forceCenter(width / 2, height / 2));
                simulation.alpha(0.3).restart();
            }
        }
    });
    ro.observe(container);
}

function updateGraph(state) {
    initTopology();

    const now = Date.now() / 1000;
    const svg = d3.select('#mesh-graph');
    const nodes = [
        { id: 'pi5', label: 'Pi 5', type: 'pi5' },
        { id: 'gateway', label: 'Gateway', type: 'gateway', connected: state.gateway.connected }
    ];

    for (const [nid, data] of Object.entries(state.nodes)) {
        const age = now - data.last_seen;
        let status = 'online';
        if (age > 20) status = 'offline';
        else if (age > 12) status = 'stale';
        nodes.push({ id: `node-${nid}`, label: nodeLabel(nid), type: 'sensing', status, ...data });
    }

    for (let i = 0; i < state.relay_nodes; i++) {
        nodes.push({ id: `relay-${i+1}`, label: `Relay ${i+1}`, type: 'relay' });
    }

    const topology = state.topology || { node_roles: {} };
    const links = [{ source: 'pi5', target: 'gateway', linkType: 'gatt' }];
    const relays = nodes.filter(n => n.type === 'relay');
    const sensing = nodes.filter(n => n.type === 'sensing');

    relays.forEach(r => links.push({ source: 'gateway', target: r.id, linkType: 'mesh' }));
    sensing.forEach(n => {
        const nid = n.id.replace('node-', '');
        const role = topology.node_roles[nid] || 'direct';
        if (role === 'relayed' && relays.length > 0) {
            links.push({ source: relays[0].id, target: n.id, linkType: 'mesh-relay' });
        } else {
            links.push({ source: 'gateway', target: n.id, linkType: 'mesh' });
        }
    });

    // Preserve positions
    const oldNodes = new Map(simulation.nodes().map(d => [d.id, d]));
    nodes.forEach(d => {
        const old = oldNodes.get(d.id);
        if (old) { d.x = old.x; d.y = old.y; d.vx = old.vx; d.vy = old.vy; }
    });

    // Links
    const lg = svg.selectAll('.link-group').data([0]).join('g').attr('class', 'link-group');
    linkSelection = lg.selectAll('.link')
        .data(links, d => {
            const s = typeof d.source === 'object' ? d.source.id : d.source;
            const t = typeof d.target === 'object' ? d.target.id : d.target;
            return `${s}-${t}`;
        });
    linkSelection.exit().remove();
    linkSelection = linkSelection.enter().append('line').attr('class', 'link').merge(linkSelection);

    linkSelection
        .attr('stroke', d => d.linkType === 'gatt' ? '#bc8cff' : d.linkType === 'mesh-relay' ? '#58a6ff' : '#30363d')
        .attr('stroke-dasharray', d => d.linkType === 'gatt' ? '6,3' : 'none')
        .attr('stroke-width', d => d.linkType === 'gatt' ? 2 : 1.5)
        .attr('stroke-opacity', d => d.linkType === 'mesh-relay' ? 0.8 : 0.6);

    // Nodes
    const ng = svg.selectAll('.node-group').data([0]).join('g').attr('class', 'node-group');
    nodeSelection = ng.selectAll('.node').data(nodes, d => d.id);
    nodeSelection.exit().remove();

    const enter = nodeSelection.enter().append('g').attr('class', 'node')
        .call(d3.drag().on('start', ds).on('drag', dd).on('end', de))
        .on('click', (e, d) => { e.stopPropagation(); topoSelectedId = d.id; updateTopoNodeDetails(d); });
    enter.append('circle').attr('r', 15);
    enter.append('text').attr('dy', 25).attr('text-anchor', 'middle').text(d => d.label);

    nodeSelection = enter.merge(nodeSelection);

    // Update labels (aliases may change)
    nodeSelection.select('text').text(d => d.label);

    nodeSelection.select('circle')
        .attr('fill', d => {
            if (d.type === 'gateway' && !d.connected) return COLORS.disconnected;
            if (d.type === 'sensing') {
                if (d.status === 'offline') return COLORS.disconnected;
                if (d.status === 'stale' || !d.responsive) return COLORS.stale;
                return COLORS.sensing;
            }
            return COLORS[d.type] || '#ccc';
        })
        .attr('opacity', d => {
            if (d.type === 'sensing' && d.status === 'offline') return 0.4;
            if (d.type === 'sensing' && d.status === 'stale') return 0.7;
            return 1;
        });

    nodeSelection.attr('class', d => {
        let c = 'node';
        if (d.type === 'sensing' && d.status === 'offline') c += ' offline';
        if (topoSelectedId && d.id === topoSelectedId) c += ' selected';
        return c;
    });

    const changed = nodes.length !== oldNodes.size;
    simulation.nodes(nodes).on('tick', () => {
        linkSelection.attr('x1', d => d.source.x).attr('y1', d => d.source.y)
            .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
        nodeSelection.attr('transform', d => `translate(${d.x},${d.y})`);
    });
    simulation.force('link').links(links);
    simulation.alpha(changed ? 1 : 0.3).restart();
}

function ds(e, d) { if (!e.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; }
function dd(e, d) { d.fx = e.x; d.fy = e.y; }
function de(e, d) { if (!e.active) simulation.alphaTarget(0); d.fx = null; d.fy = null; }

function updateTopoNodeDetails(node) {
    const el = document.getElementById('topo-node-content');
    let html = `<h3 style="margin-bottom:0.25rem">${node.label}</h3>`;
    html += `<p style="color:${COLORS[node.type]};font-size:0.75rem;text-transform:uppercase;margin-bottom:0.75rem">${node.type}</p>`;

    if (node.type === 'sensing') {
        const age = (Date.now() / 1000) - node.last_seen;
        let st = 'OK', sc = '';
        if (age > 20) { st = 'OFFLINE'; sc = 'offline-error'; }
        else if (age > 12) { st = 'STALE'; sc = 'stale-warning'; }
        html += `<div class="data-grid">
            <span>Duty:</span><span class="value">${node.duty}%</span>
            <span>Voltage:</span><span class="value">${node.voltage.toFixed(2)} V</span>
            <span>Current:</span><span class="value">${node.current.toFixed(1)} mA</span>
            <span>Power:</span><span class="value">${node.power.toFixed(0)} mW</span>
            <span>Target:</span><span class="value">${node.target_duty}%</span>
            <span>Status:</span><span class="value ${sc}">${st}</span>
            <span>Seen:</span><span class="value" style="font-size:0.75rem">${age.toFixed(0)}s ago</span>
        </div>`;
    } else if (node.type === 'gateway') {
        html += `<p>${node.connected ? 'Connected' : 'Disconnected'}</p>`;
    } else if (node.type === 'pi5') {
        html += `<p style="color:var(--text-secondary)">Raspberry Pi 5 Host</p>`;
    } else if (node.type === 'relay') {
        html += `<p style="color:var(--text-secondary)">BLE Mesh Relay</p>`;
    }
    el.innerHTML = html;
}

function updateTopoSidebar(state) {
    // Auto-refresh selected node
    if (topoSelectedId && currentState) {
        let nd = null;
        if (topoSelectedId === 'pi5') nd = { id: 'pi5', label: 'Pi 5', type: 'pi5' };
        else if (topoSelectedId === 'gateway') nd = { id: 'gateway', label: 'Gateway', type: 'gateway', connected: state.gateway.connected };
        else if (topoSelectedId.startsWith('node-')) {
            const nid = topoSelectedId.replace('node-', '');
            if (state.nodes[nid]) {
                const age = (Date.now() / 1000) - state.nodes[nid].last_seen;
                nd = { id: topoSelectedId, label: nodeLabel(nid), type: 'sensing',
                    status: age > 20 ? 'offline' : age > 12 ? 'stale' : 'online', ...state.nodes[nid] };
            }
        } else if (topoSelectedId.startsWith('relay-')) {
            nd = { id: topoSelectedId, label: topoSelectedId.replace('relay-', 'Relay '), type: 'relay' };
        }
        if (nd) updateTopoNodeDetails(nd);
    }

    // Power manager
    const pm = state.power_manager;
    const el = document.getElementById('pm-content');
    if (!pm || !pm.active) {
        el.innerHTML = '<p class="placeholder">Power Manager Disabled</p>';
        return;
    }
    const pct = pm.budget_mw ? Math.min(100, (pm.total_power_mw / pm.budget_mw) * 100) : 0;
    el.innerHTML = `
        <div class="data-grid">
            <span>Threshold:</span><span class="value">${pm.threshold_mw} mW</span>
            <span>Budget:</span><span class="value">${pm.budget_mw} mW</span>
            <span>Current:</span><span class="value">${pm.total_power_mw.toFixed(0)} mW</span>
            <span>Priority:</span><span class="value">${pm.priority_node ? nodeLabel(pm.priority_node) : 'None'}</span>
        </div>
        <div class="progress-bar"><div class="progress-fill" style="width:${pct}%"></div></div>`;
}


// =====================================================================
//  TAB 2: Nodes
// =====================================================================

function updateNodeCards(state) {
    const container = document.getElementById('nodes-cards');
    const now = Date.now() / 1000;
    const nodeIds = Object.keys(state.nodes).sort((a, b) => parseInt(a) - parseInt(b));

    document.getElementById('nodes-count').textContent = `${nodeIds.length} node${nodeIds.length !== 1 ? 's' : ''}`;

    // Build cards
    let html = '';
    for (const nid of nodeIds) {
        const d = state.nodes[nid];
        const age = now - d.last_seen;
        let st = 'ok', stLabel = 'OK';
        if (age > 20) { st = 'offline'; stLabel = 'OFFLINE'; }
        else if (age > 12) { st = 'stale'; stLabel = 'STALE'; }

        const isActive = nodesSelectedId === nid;

        html += `
        <div class="node-card ${isActive ? 'active' : ''}" data-nid="${nid}">
            <div class="node-card-header">
                <span class="node-name">${nodeLabel(nid)}</span>
                <span class="node-status ${st}">${stLabel}</span>
            </div>
            <div class="node-card-metrics">
                <div><span class="metric-label">Duty</span><br><span class="metric-value">${d.duty}%</span></div>
                <div><span class="metric-label">Voltage</span><br><span class="metric-value">${d.voltage.toFixed(2)}V</span></div>
                <div><span class="metric-label">Current</span><br><span class="metric-value">${d.current.toFixed(1)}mA</span></div>
                <div><span class="metric-label">Power</span><br><span class="metric-value">${d.power.toFixed(0)}mW</span></div>
            </div>
        </div>`;
    }

    // Also show relay nodes
    for (let i = 0; i < (state.relay_nodes || 0); i++) {
        html += `
        <div class="node-card" style="opacity:0.6">
            <div class="node-card-header">
                <span class="node-name">Relay ${i + 1}</span>
                <span class="node-status ok" style="background:rgba(88,166,255,0.15);color:var(--accent-blue)">RELAY</span>
            </div>
            <div style="font-size:0.75rem;color:var(--text-secondary);padding-top:0.25rem">Packet forwarder — no sensor data</div>
        </div>`;
    }

    container.innerHTML = html;

    // Bind click handlers
    container.querySelectorAll('.node-card[data-nid]').forEach(card => {
        card.addEventListener('click', () => {
            const nid = card.dataset.nid;
            nodesSelectedId = nid;
            // Update active state visually
            container.querySelectorAll('.node-card').forEach(c => c.classList.remove('active'));
            card.classList.add('active');
            showNodeDetail(nid, state);
            fetchNodeChart(nid);
        });
    });

    // Update detail panel if a node is already selected
    if (nodesSelectedId && state.nodes[nodesSelectedId]) {
        showNodeDetail(nodesSelectedId, state);
    }
}

function showNodeDetail(nid, state) {
    const d = state.nodes[nid];
    if (!d) return;

    const now = Date.now() / 1000;
    const age = now - d.last_seen;
    let st = 'OK', sc = '';
    if (age > 20) { st = 'OFFLINE'; sc = 'offline-error'; }
    else if (age > 12) { st = 'STALE'; sc = 'stale-warning'; }

    const el = document.getElementById('node-detail-content');
    el.innerHTML = `
        <h3 style="font-size:1.1rem;margin-bottom:0.5rem">${nodeLabel(nid)}</h3>
        <div class="data-grid" style="max-width:360px">
            <span>Duty:</span><span class="value">${d.duty}%</span>
            <span>Target:</span><span class="value">${d.target_duty}%</span>
            <span>Commanded:</span><span class="value">${d.commanded_duty}%</span>
            <span>Voltage:</span><span class="value">${d.voltage.toFixed(3)} V</span>
            <span>Current:</span><span class="value">${d.current.toFixed(1)} mA</span>
            <span>Power:</span><span class="value">${d.power.toFixed(1)} mW</span>
            <span>Status:</span><span class="value ${sc}">${st}</span>
            <span>Last seen:</span><span class="value" style="font-size:0.78rem">${age.toFixed(0)}s ago</span>
        </div>`;
}

async function fetchNodeChart(nid) {
    try {
        const resp = await fetch(`/api/history/node/${nid}?minutes=${chartWindow}`);
        if (!resp.ok) return;
        const data = await resp.json();
        chartData = data.points || [];
        renderNodeChart();
    } catch (e) { console.error("Chart fetch:", e); }
}

function renderNodeChart() {
    const container = document.getElementById('node-chart-area');
    const svgEl = document.getElementById('node-chart-svg');
    const svg = d3.select(svgEl);
    svg.selectAll('*').remove();

    if (!chartData.length) {
        svg.append('text')
            .attr('x', '50%').attr('y', '50%')
            .attr('text-anchor', 'middle').attr('fill', '#656d76')
            .attr('font-size', '13px').attr('font-family', 'var(--font)')
            .text('No data in this time window');
        return;
    }

    const rect = container.getBoundingClientRect();
    const W = rect.width || 600;
    const H = rect.height || 250;
    const margin = { top: 16, right: 20, bottom: 32, left: 55 };
    const w = W - margin.left - margin.right;
    const h = H - margin.top - margin.bottom;

    const metric = chartMetric;
    const color = METRIC_COLORS[metric] || '#3fb950';

    const parseTime = d => new Date(d.timestamp.replace('T', ' '));
    const getValue = d => d[metric];

    const xScale = d3.scaleTime()
        .domain(d3.extent(chartData, parseTime))
        .range([0, w]);

    const yExtent = d3.extent(chartData, getValue);
    const yPad = (yExtent[1] - yExtent[0]) * 0.1 || 1;
    const yScale = d3.scaleLinear()
        .domain([yExtent[0] - yPad, yExtent[1] + yPad])
        .range([h, 0]);

    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    // Grid
    g.append('g').attr('class', 'chart-grid')
        .call(d3.axisLeft(yScale).tickSize(-w).tickFormat(''))
        .call(g => g.selectAll('.tick line').attr('stroke-opacity', 0.15));

    // Area
    const area = d3.area()
        .x(d => xScale(parseTime(d)))
        .y0(h)
        .y1(d => yScale(getValue(d)))
        .curve(d3.curveMonotoneX);

    g.append('path')
        .datum(chartData)
        .attr('class', 'chart-area')
        .attr('fill', color)
        .attr('d', area);

    // Line
    const line = d3.line()
        .x(d => xScale(parseTime(d)))
        .y(d => yScale(getValue(d)))
        .curve(d3.curveMonotoneX);

    g.append('path')
        .datum(chartData)
        .attr('class', 'chart-line')
        .attr('stroke', color)
        .attr('d', line);

    // Axes
    g.append('g').attr('class', 'chart-axis')
        .attr('transform', `translate(0,${h})`)
        .call(d3.axisBottom(xScale).ticks(6).tickFormat(d3.timeFormat('%H:%M')));

    g.append('g').attr('class', 'chart-axis')
        .call(d3.axisLeft(yScale).ticks(5));

    // Label
    svg.append('text')
        .attr('x', margin.left + 6).attr('y', 12)
        .attr('fill', color).attr('font-size', '10px').attr('font-family', 'var(--mono)')
        .text(METRIC_LABELS[metric]);
}

// Chart controls
document.getElementById('chart-metric').addEventListener('change', e => {
    chartMetric = e.target.value;
    renderNodeChart();
});
document.getElementById('chart-window').addEventListener('change', e => {
    chartWindow = parseInt(e.target.value);
    if (nodesSelectedId) fetchNodeChart(nodesSelectedId);
});

// Re-render chart on resize
const chartRO = new ResizeObserver(() => { if (activeTab === 'nodes' && chartData.length) renderNodeChart(); });
chartRO.observe(document.getElementById('node-chart-area'));


// =====================================================================
//  TAB 3: History
// =====================================================================

function updateNodeFilterDropdown(state) {
    const select = document.getElementById('history-node-filter');
    let changed = false;
    for (const nid of Object.keys(state.nodes)) {
        if (!knownNodeIds.has(nid)) { knownNodeIds.add(nid); changed = true; }
    }
    if (changed) {
        const val = select.value;
        select.innerHTML = '<option value="">All Nodes</option>';
        [...knownNodeIds].sort((a, b) => parseInt(a) - parseInt(b)).forEach(nid => {
            const opt = document.createElement('option');
            opt.value = nid; opt.textContent = nodeLabel(nid);
            select.appendChild(opt);
        });
        select.value = val;
    }
}

async function fetchHistory() {
    const p = new URLSearchParams();
    if (historyNodeFilter) p.set('node_id', historyNodeFilter);
    p.set('limit', historyLimit);
    p.set('offset', historyOffset);

    try {
        const resp = await fetch(`/api/history?${p}`);
        if (!resp.ok) return;
        const data = await resp.json();
        historyTotal = data.total;
        renderHistoryTable(data.readings);
        updatePagination();
        updateDbStats();
    } catch (e) { console.error("History fetch:", e); }
}

function renderHistoryTable(readings) {
    const tbody = document.getElementById('history-body');
    if (!readings || !readings.length) {
        tbody.innerHTML = '<tr><td colspan="9" class="placeholder">No readings recorded yet.</td></tr>';
        document.getElementById('history-info').textContent = '';
        return;
    }

    tbody.innerHTML = readings.map(r => {
        const badge = r.responsive
            ? '<span class="badge badge-ok">OK</span>'
            : '<span class="badge badge-offline">OFF</span>';
        return `<tr>
            <td>${fmtTs(r.timestamp)}</td>
            <td><span class="node-badge">${nodeAliases[r.node_id] || 'N' + r.node_id}</span></td>
            <td>${r.duty}%</td>
            <td>${r.voltage.toFixed(3)}</td>
            <td>${r.current_ma.toFixed(1)}</td>
            <td>${r.power_mw.toFixed(1)}</td>
            <td>${r.target_duty}%</td>
            <td>${r.commanded_duty}%</td>
            <td>${badge}</td>
        </tr>`;
    }).join('');

    document.getElementById('history-info').textContent =
        `${historyOffset + 1}-${historyOffset + readings.length} of ${historyTotal}`;
}

function fmtTs(ts) {
    if (!ts) return '-';
    const p = ts.split('T');
    return p.length === 2 ? p[0].slice(5) + ' ' + p[1] : ts;
}

function updatePagination() {
    const pages = Math.ceil(historyTotal / historyLimit) || 1;
    const cur = Math.floor(historyOffset / historyLimit) + 1;
    document.getElementById('page-prev').disabled = historyOffset === 0;
    document.getElementById('page-next').disabled = historyOffset + historyLimit >= historyTotal;
    document.getElementById('page-info').textContent = `Page ${cur} / ${pages}`;
}

async function updateDbStats() {
    try {
        const resp = await fetch('/api/db/stats');
        if (!resp.ok) return;
        const d = await resp.json();
        document.getElementById('db-stats').textContent = `DB: ${d.total_rows} rows (${d.db_size_mb} MB)`;
    } catch (e) { /* */ }
}

// Event listeners
document.getElementById('history-node-filter').addEventListener('change', e => {
    historyNodeFilter = e.target.value; historyOffset = 0; fetchHistory();
});
document.getElementById('history-limit').addEventListener('change', e => {
    historyLimit = parseInt(e.target.value); historyOffset = 0; fetchHistory();
});
document.getElementById('history-refresh').addEventListener('click', () => {
    historyOffset = 0; fetchHistory();
});
document.getElementById('page-prev').addEventListener('click', () => {
    historyOffset = Math.max(0, historyOffset - historyLimit); fetchHistory();
});
document.getElementById('page-next').addEventListener('click', () => {
    if (historyOffset + historyLimit < historyTotal) { historyOffset += historyLimit; fetchHistory(); }
});


// =====================================================================
//  TAB 4: Console
// =====================================================================

const conOutput = document.getElementById('console-output');
const conInput = document.getElementById('console-input');

// Seed with welcome message
appendConLine('system', 'BLE Mesh Gateway Console — type "help" for commands');

conInput.addEventListener('keydown', async (e) => {
    if (e.key === 'Enter') {
        const cmd = conInput.value.trim();
        conInput.value = '';
        if (!cmd) return;

        // Push to history
        consoleHistory.push(cmd);
        consoleHistIdx = consoleHistory.length;

        appendConLine('cmd', cmd);

        // Local-only commands
        if (cmd.toLowerCase() === 'clear') {
            conOutput.innerHTML = '';
            appendConLine('system', 'Console cleared');
            return;
        }

        // Send to API
        try {
            const resp = await fetch('/api/command', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ command: cmd })
            });
            const data = await resp.json();
            if (data.error) {
                appendConLine('error', data.error);
            } else if (data.response) {
                appendConLine('resp', data.response);
            }
        } catch (err) {
            appendConLine('error', `Network error: ${err.message}`);
        }
    } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        if (consoleHistIdx > 0) {
            consoleHistIdx--;
            conInput.value = consoleHistory[consoleHistIdx];
        }
    } else if (e.key === 'ArrowDown') {
        e.preventDefault();
        if (consoleHistIdx < consoleHistory.length - 1) {
            consoleHistIdx++;
            conInput.value = consoleHistory[consoleHistIdx];
        } else {
            consoleHistIdx = consoleHistory.length;
            conInput.value = '';
        }
    }
});

function appendConLine(type, text) {
    const div = document.createElement('div');
    div.className = `con-line con-${type}`;
    div.textContent = text;
    conOutput.appendChild(div);
    conOutput.scrollTop = conOutput.scrollHeight;
}


// =====================================================================
//  TAB 5: Settings
// =====================================================================

async function loadSettingsTab() {
    // Fetch current settings, aliases, and DB stats in parallel
    try {
        const [settingsResp, aliasResp, dbResp] = await Promise.all([
            fetch('/api/settings'),
            fetch('/api/nodes/aliases'),
            fetch('/api/db/stats')
        ]);

        if (settingsResp.ok) {
            const settings = await settingsResp.json();
            // Sync theme toggle buttons
            document.querySelectorAll('#theme-toggle .toggle-btn').forEach(btn => {
                btn.classList.toggle('active', btn.dataset.value === (settings.theme || 'dark'));
            });
        }

        if (aliasResp.ok) {
            nodeAliases = await aliasResp.json();
        }

        if (dbResp.ok) {
            const db = await dbResp.json();
            document.getElementById('settings-db-info').textContent =
                `${db.total_rows.toLocaleString()} readings — ${db.db_size_mb} MB`;
        }
    } catch (e) { console.error('Settings load error:', e); }

    // Render node management list
    renderSettingsNodes();
}

function renderSettingsNodes() {
    const container = document.getElementById('settings-nodes-list');
    if (!currentState || !currentState.nodes || Object.keys(currentState.nodes).length === 0) {
        container.innerHTML = '<p class="placeholder">No nodes discovered yet</p>';
        return;
    }

    const nodeIds = Object.keys(currentState.nodes).sort((a, b) => parseInt(a) - parseInt(b));
    let html = '';
    for (const nid of nodeIds) {
        const alias = nodeAliases[nid] || '';
        const displayName = alias || `Node ${nid}`;
        html += `
        <div class="settings-node-row" data-nid="${nid}" id="sn-row-${nid}">
            <span class="sn-badge">N${nid}</span>
            <span class="sn-alias" id="sn-alias-${nid}">${displayName}</span>
            <div class="sn-actions" id="sn-actions-${nid}">
                <button class="btn-sm" onclick="startRenameNode('${nid}')">Rename</button>
                <button class="btn-sm btn-sm-danger" onclick="confirmRemoveNode('${nid}')">Remove</button>
            </div>
        </div>`;
    }
    container.innerHTML = html;
}

// --- Theme toggle ---
document.getElementById('theme-toggle').addEventListener('click', async (e) => {
    const btn = e.target.closest('.toggle-btn');
    if (!btn) return;
    const theme = btn.dataset.value;

    // Update toggle buttons
    document.querySelectorAll('#theme-toggle .toggle-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');

    // Apply theme
    if (theme === 'light') {
        document.body.classList.add('light');
    } else {
        document.body.classList.remove('light');
    }

    // Persist to backend
    try {
        await fetch('/api/settings', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ theme })
        });
    } catch (e) { console.error('Theme save error:', e); }
});

// --- Rename node ---
function startRenameNode(nid) {
    const aliasEl = document.getElementById(`sn-alias-${nid}`);
    const actionsEl = document.getElementById(`sn-actions-${nid}`);
    const currentAlias = nodeAliases[nid] || '';

    aliasEl.outerHTML = `<input class="sn-alias-input" id="sn-alias-${nid}"
        value="${currentAlias}" placeholder="Node ${nid}"
        maxlength="50" autofocus>`;

    actionsEl.innerHTML = `
        <button class="btn-sm" onclick="saveNodeAlias('${nid}')">Save</button>
        <button class="btn-sm" onclick="cancelRenameNode('${nid}')">Cancel</button>`;

    const input = document.getElementById(`sn-alias-${nid}`);
    input.focus();
    input.select();
    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') saveNodeAlias(nid);
        if (e.key === 'Escape') cancelRenameNode(nid);
    });
}

async function saveNodeAlias(nid) {
    const input = document.getElementById(`sn-alias-${nid}`);
    const alias = input.value.trim();
    const actionsEl = document.getElementById(`sn-actions-${nid}`);

    try {
        if (alias) {
            await fetch(`/api/nodes/${nid}/rename`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ alias })
            });
            nodeAliases[nid] = alias;
        } else {
            // Empty alias = reset to default
            await fetch(`/api/nodes/${nid}/alias`, { method: 'DELETE' });
            delete nodeAliases[nid];
        }

        // Show success flash
        const displayName = alias || `Node ${nid}`;
        input.outerHTML = `<span class="sn-alias" id="sn-alias-${nid}">${displayName}</span>`;
        actionsEl.innerHTML = `
            <span class="flash-success">Saved ✓</span>`;
        setTimeout(() => {
            actionsEl.innerHTML = `
                <button class="btn-sm" onclick="startRenameNode('${nid}')">Rename</button>
                <button class="btn-sm btn-sm-danger" onclick="confirmRemoveNode('${nid}')">Remove</button>`;
        }, 1500);

        // Force filter dropdown to re-render with new name
        knownNodeIds.clear();
        if (currentState) updateNodeFilterDropdown(currentState);
    } catch (e) {
        console.error('Rename error:', e);
        actionsEl.innerHTML = `<span class="con-error">Error</span>`;
    }
}

function cancelRenameNode(nid) {
    const displayName = nodeAliases[nid] || `Node ${nid}`;
    const input = document.getElementById(`sn-alias-${nid}`);
    input.outerHTML = `<span class="sn-alias" id="sn-alias-${nid}">${displayName}</span>`;
    document.getElementById(`sn-actions-${nid}`).innerHTML = `
        <button class="btn-sm" onclick="startRenameNode('${nid}')">Rename</button>
        <button class="btn-sm btn-sm-danger" onclick="confirmRemoveNode('${nid}')">Remove</button>`;
}

// --- Remove node ---
function confirmRemoveNode(nid) {
    const actionsEl = document.getElementById(`sn-actions-${nid}`);
    actionsEl.innerHTML = `
        <div class="confirm-inline">
            <span>Delete?</span>
            <button class="confirm-yes" onclick="removeNode('${nid}')">Yes</button>
            <button class="confirm-no" onclick="cancelRemoveNode('${nid}')">No</button>
        </div>`;
}

function cancelRemoveNode(nid) {
    document.getElementById(`sn-actions-${nid}`).innerHTML = `
        <button class="btn-sm" onclick="startRenameNode('${nid}')">Rename</button>
        <button class="btn-sm btn-sm-danger" onclick="confirmRemoveNode('${nid}')">Remove</button>`;
}

async function removeNode(nid) {
    try {
        const resp = await fetch(`/api/nodes/${nid}`, { method: 'DELETE' });
        const data = await resp.json();
        if (resp.ok) {
            // Remove row with animation
            const row = document.getElementById(`sn-row-${nid}`);
            if (row) {
                row.style.transition = 'opacity 0.3s, transform 0.3s';
                row.style.opacity = '0';
                row.style.transform = 'translateX(20px)';
                setTimeout(() => row.remove(), 300);
            }
            delete nodeAliases[nid];
            // Clear from filter dropdown
            knownNodeIds.delete(nid);
            if (currentState) {
                delete currentState.nodes[nid];
                updateNodeFilterDropdown(currentState);
            }
            // Update DB stats
            const dbResp = await fetch('/api/db/stats');
            if (dbResp.ok) {
                const db = await dbResp.json();
                document.getElementById('settings-db-info').textContent =
                    `${db.total_rows.toLocaleString()} readings — ${db.db_size_mb} MB`;
            }
        } else {
            alert(data.error || 'Failed to remove node');
        }
    } catch (e) {
        console.error('Remove error:', e);
        alert('Network error removing node');
    }
}

// --- Clear all history ---
document.getElementById('btn-clear-all-history').addEventListener('click', function() {
    const btn = this;
    const orig = btn.textContent;

    if (btn.dataset.confirming) {
        // Second click — do it
        delete btn.dataset.confirming;
        btn.textContent = 'Clearing...';
        btn.disabled = true;

        fetch('/api/history/clear', { method: 'DELETE' })
            .then(r => r.json())
            .then(data => {
                btn.textContent = `Cleared ${data.deleted.toLocaleString()} rows ✓`;
                btn.className = 'btn-sm';
                btn.style.color = 'var(--accent-green)';
                btn.style.borderColor = 'var(--accent-green)';
                // Update DB stats
                fetch('/api/db/stats').then(r => r.json()).then(db => {
                    document.getElementById('settings-db-info').textContent =
                        `${db.total_rows.toLocaleString()} readings — ${db.db_size_mb} MB`;
                });
                setTimeout(() => {
                    btn.textContent = orig;
                    btn.className = 'btn-danger';
                    btn.style.color = '';
                    btn.style.borderColor = '';
                    btn.disabled = false;
                }, 2500);
            })
            .catch(e => {
                btn.textContent = 'Error';
                btn.disabled = false;
                setTimeout(() => { btn.textContent = orig; btn.className = 'btn-danger'; }, 2000);
            });
        return;
    }

    // First click — ask confirmation
    btn.dataset.confirming = 'true';
    btn.textContent = 'Are you sure? Click again';
    btn.style.background = 'rgba(248,81,73,0.25)';
    setTimeout(() => {
        if (btn.dataset.confirming) {
            delete btn.dataset.confirming;
            btn.textContent = orig;
            btn.style.background = '';
        }
    }, 4000);
});
