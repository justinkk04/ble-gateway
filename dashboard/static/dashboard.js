// dashboard.js

// --- State & Config ---
let currentState = null;
let simulation = null;
let nodeSelection = null;
let linkSelection = null;
let selectedNodeId = null;  // Track which node is selected for auto-refresh

// Color Palette
const COLORS = {
    pi5: '#bc8cff',
    gateway: '#d29922',
    sensing: '#3fb950',
    relay: '#58a6ff',
    disconnected: '#f85149',
    stale: '#d29922'
};

// --- D3 Setup ---
const width = document.getElementById('graph-container').clientWidth;
const height = document.getElementById('graph-container').clientHeight;
const svg = d3.select('#mesh-graph');

// Click SVG background to deselect node
svg.on('click', (e) => {
    if (e.target === svg.node()) {
        selectedNodeId = null;
        document.getElementById('node-content').innerHTML =
            '<p class="placeholder">Click a node to inspect</p>';
    }
});

// Force Simulation — link distance varies by type
simulation = d3.forceSimulation()
    .force('link', d3.forceLink().id(d => d.id).distance(d => {
        if (d.linkType === 'gatt') return 60;
        if (d.linkType === 'mesh-relay') return 120;
        return 100;
    }))
    .force('charge', d3.forceManyBody().strength(-300))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('collision', d3.forceCollide().radius(40));

// --- Resize Observer ---
const resizeObserver = new ResizeObserver(entries => {
    for (const entry of entries) {
        const { width, height } = entry.contentRect;
        simulation.force('center', d3.forceCenter(width / 2, height / 2));
        simulation.alpha(0.3).restart();
    }
});
resizeObserver.observe(document.getElementById('graph-container'));

// --- Main Loop: Poll & Update ---
async function pollState() {
    try {
        const resp = await fetch('/api/state');
        if (resp.ok) {
            const data = await resp.json();
            updateDashboard(data);
        } else {
            console.error("API error:", resp.status);
        }
    } catch (e) {
        console.error("Fetch error:", e);
    }
}

// Poll every 2 seconds
setInterval(pollState, 2000);
pollState(); // Initial call

// --- Update Logic ---
function updateDashboard(state) {
    currentState = state;
    updateGraph(state);
    updateSidebar(state);
    updateStatusBar(state);
    updateHeader(state);

    // Auto-refresh selected node details
    if (selectedNodeId) {
        refreshSelectedNode(state);
    }
}

function updateGraph(state) {
    // 1. Transform state into nodes & links
    const now = Date.now() / 1000;
    const nodes = [
        { id: 'pi5', label: 'Pi 5', type: 'pi5' },
        { id: 'gateway', label: 'Gateway', type: 'gateway', connected: state.gateway.connected }
    ];

    // Sensing nodes — always show, never hide. Use status for visual state.
    for (const [nid, data] of Object.entries(state.nodes)) {
        const age = now - data.last_seen;
        let status = 'online';
        if (age > 20) status = 'offline';
        else if (age > 12) status = 'stale';

        nodes.push({
            id: `node-${nid}`,
            label: `Node ${nid}`,
            type: 'sensing',
            status: status,
            ...data
        });
    }

    // Relay nodes (inferred)
    for (let i = 0; i < state.relay_nodes; i++) {
        nodes.push({ id: `relay-${i + 1}`, label: `Relay ${i + 1}`, type: 'relay' });
    }

    // 2. Build topology-aware links
    const topology = state.topology || { node_roles: {} };
    const links = [
        { source: 'pi5', target: 'gateway', linkType: 'gatt' }
    ];

    const relayNodes = nodes.filter(n => n.type === 'relay');
    const sensingNodes = nodes.filter(n => n.type === 'sensing');

    // Connect relay nodes to gateway
    relayNodes.forEach(r => {
        links.push({ source: 'gateway', target: r.id, linkType: 'mesh' });
    });

    // Connect sensing nodes — direct or through relay
    sensingNodes.forEach(n => {
        const nid = n.id.replace('node-', '');
        const role = topology.node_roles[nid] || 'direct';

        if (role === 'relayed' && relayNodes.length > 0) {
            links.push({ source: relayNodes[0].id, target: n.id, linkType: 'mesh-relay' });
        } else {
            links.push({ source: 'gateway', target: n.id, linkType: 'mesh' });
        }
    });

    // Preserve simulation state (x, y, vx, vy) to prevent snapping
    const oldNodes = new Map(simulation.nodes().map(d => [d.id, d]));
    nodes.forEach(d => {
        const old = oldNodes.get(d.id);
        if (old) {
            d.x = old.x;
            d.y = old.y;
            d.vx = old.vx;
            d.vy = old.vy;
        }
    });

    // 3. D3 Enter/Update/Exit

    // Links — use composite key that works before and after D3 resolves references
    const linkGroup = svg.selectAll('.link-group').data([0]).join('g').attr('class', 'link-group');
    linkSelection = linkGroup.selectAll('.link')
        .data(links, d => {
            const s = typeof d.source === 'object' ? d.source.id : d.source;
            const t = typeof d.target === 'object' ? d.target.id : d.target;
            return `${s}-${t}`;
        });

    linkSelection.exit().remove();
    const linkEnter = linkSelection.enter().append('line').attr('class', 'link');
    linkSelection = linkEnter.merge(linkSelection);

    // Style links by type
    linkSelection
        .attr('stroke', d => {
            if (d.linkType === 'gatt') return '#bc8cff';
            if (d.linkType === 'mesh-relay') return '#58a6ff';
            return '#30363d';
        })
        .attr('stroke-dasharray', d => {
            if (d.linkType === 'gatt') return '6,3';
            return 'none';
        })
        .attr('stroke-width', d => {
            if (d.linkType === 'gatt') return 2;
            return 1.5;
        })
        .attr('stroke-opacity', d => {
            if (d.linkType === 'mesh-relay') return 0.8;
            return 0.6;
        });

    // Nodes
    const nodeGroup = svg.selectAll('.node-group').data([0]).join('g').attr('class', 'node-group');
    nodeSelection = nodeGroup.selectAll('.node')
        .data(nodes, d => d.id);

    nodeSelection.exit().remove();

    const nodeEnter = nodeSelection.enter().append('g')
        .attr('class', 'node')
        .call(d3.drag()
            .on('start', dragstarted)
            .on('drag', dragged)
            .on('end', dragended))
        .on('click', (e, d) => {
            e.stopPropagation();
            selectedNodeId = d.id;
            updateNodeDetails(d);
        });

    // Circle
    nodeEnter.append('circle').attr('r', 15);

    // Label
    nodeEnter.append('text')
        .attr('dy', 25)
        .attr('text-anchor', 'middle')
        .text(d => d.label);

    nodeSelection = nodeEnter.merge(nodeSelection);

    // Update styling based on state
    nodeSelection.select('circle')
        .attr('fill', d => {
            if (d.type === 'gateway' && !d.connected) return COLORS.disconnected;
            if (d.type === 'sensing') {
                if (d.status === 'offline') return COLORS.disconnected;
                if (d.status === 'stale') return COLORS.stale;
                if (!d.responsive) return COLORS.stale;
                return COLORS.sensing;
            }
            return COLORS[d.type] || '#ccc';
        })
        .attr('opacity', d => {
            if (d.type === 'sensing' && d.status === 'offline') return 0.4;
            if (d.type === 'sensing' && d.status === 'stale') return 0.7;
            return 1.0;
        })
        .attr('stroke-opacity', d => {
            if (d.type === 'sensing' && d.status === 'offline') return 0.3;
            if (d.type === 'sensing' && d.status === 'stale') return 0.5;
            return 1;
        });

    // Add CSS class for animations (offline pulse)
    nodeSelection.attr('class', d => {
        let cls = 'node';
        if (d.type === 'sensing' && d.status === 'offline') cls += ' offline';
        if (d.type === 'sensing' && d.status === 'stale') cls += ' stale';
        // Highlight selected node
        if (selectedNodeId && d.id === selectedNodeId) cls += ' selected';
        return cls;
    });

    // Restart simulation with new data
    const nodeCountChanged = nodes.length !== oldNodes.size;

    simulation.nodes(nodes).on('tick', ticked);
    simulation.force('link').links(links);

    if (nodeCountChanged) {
        simulation.alpha(1).restart();
    } else {
        simulation.alpha(0.3).restart();
    }
}

function ticked() {
    linkSelection
        .attr('x1', d => d.source.x)
        .attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x)
        .attr('y2', d => d.target.y);

    nodeSelection.attr('transform', d => `translate(${d.x},${d.y})`);
}

// --- Drag Handlers ---
function dragstarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
}
function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
}
function dragended(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
}

// --- Sidebar UI ---
function updateNodeDetails(node) {
    const el = document.getElementById('node-content');
    if (!node) return;

    let html = `<h3>${node.label}</h3>`;
    html += `<p class="label" style="color:${COLORS[node.type]}">${node.type.toUpperCase()}</p>`;

    if (node.type === 'sensing') {
        const lastSeen = (Date.now() / 1000) - node.last_seen;
        let statusText = 'OK';
        let staleClass = '';
        if (node.status === 'offline' || lastSeen > 20) {
            statusText = 'OFFLINE';
            staleClass = 'offline-error';
        } else if (node.status === 'stale' || lastSeen > 12) {
            statusText = 'STALE';
            staleClass = 'stale-warning';
        }

        html += `
        <div class="data-grid" style="margin-top:1rem">
            <span>Duty:</span> <span class="value">${node.duty}%</span>
            <span>Voltage:</span> <span class="value">${node.voltage.toFixed(2)} V</span>
            <span>Current:</span> <span class="value">${node.current.toFixed(1)} mA</span>
            <span>Power:</span> <span class="value">${node.power.toFixed(0)} mW</span>
            <span>Target:</span> <span class="value">${node.target_duty}%</span>
            <span>Status:</span> <span class="value ${staleClass}">${statusText}</span>
            <span>Seen:</span> <span class="value" style="font-size:0.8em">${lastSeen.toFixed(0)}s ago</span>
        </div>`;
    } else if (node.type === 'gateway') {
        html += `<p style="margin-top:0.5rem">Status: ${node.connected ? 'Connected' : 'Disconnected'}</p>`;
        html += `<p class="value" style="font-size:0.8rem">${node.device_address || ''}</p>`;
    } else if (node.type === 'pi5') {
        html += `<p style="margin-top:0.5rem">Raspberry Pi 5 Gateway Host</p>`;
    } else if (node.type === 'relay') {
        html += `<p style="margin-top:0.5rem">BLE Mesh Relay Node</p>`;
        html += `<p class="label" style="font-size:0.8rem">Forwards packets for distant nodes</p>`;
    }
    el.innerHTML = html;
}

// Auto-refresh selected node details on every poll
function refreshSelectedNode(state) {
    if (!selectedNodeId) return;

    let nodeData = null;

    if (selectedNodeId === 'pi5') {
        nodeData = { id: 'pi5', label: 'Pi 5', type: 'pi5' };
    } else if (selectedNodeId === 'gateway') {
        nodeData = {
            id: 'gateway', label: 'Gateway', type: 'gateway',
            connected: state.gateway.connected,
            device_address: state.gateway.device_address
        };
    } else if (selectedNodeId.startsWith('node-')) {
        const nid = selectedNodeId.replace('node-', '');
        if (state.nodes[nid]) {
            const now = Date.now() / 1000;
            const age = now - state.nodes[nid].last_seen;
            let status = 'online';
            if (age > 5) status = 'offline';
            else if (age > 3) status = 'stale';

            nodeData = {
                id: selectedNodeId,
                label: `Node ${nid}`,
                type: 'sensing',
                status: status,
                ...state.nodes[nid]
            };
        }
    } else if (selectedNodeId.startsWith('relay-')) {
        nodeData = {
            id: selectedNodeId,
            label: selectedNodeId.replace('relay-', 'Relay '),
            type: 'relay'
        };
    }

    if (nodeData) {
        updateNodeDetails(nodeData);
    }
}

function updateSidebar(state) {
    const pm = state.power_manager;
    const el = document.getElementById('pm-content');

    if (!pm || !pm.active) {
        el.innerHTML = '<p class="placeholder">Power Manager Disabled</p>';
        return;
    }

    const pct = pm.budget_mw ? Math.min(100, (pm.total_power_mw / pm.budget_mw) * 100) : 0;

    el.innerHTML = `
        <div class="data-grid">
            <span>Threshold:</span> <span class="value">${pm.threshold_mw} mW</span>
            <span>Budget:</span> <span class="value">${pm.budget_mw} mW</span>
            <span>Current:</span> <span class="value">${pm.total_power_mw.toFixed(0)} mW</span>
            <span>Priority:</span> <span class="value">Node ${pm.priority_node || 'None'}</span>
        </div>
        <div class="progress-bar">
            <div class="progress-fill" style="width: ${pct}%"></div>
        </div>
    `;
}

function updateHeader(state) {
    const statusDot = document.querySelector('.status-dot');
    const statusText = document.getElementById('connection-text');
    const overlay = document.getElementById('disconnect-overlay');

    // Check data age (stale if > 10s)
    const dataAge = (Date.now() - new Date(state.timestamp).getTime()) / 1000;
    const isStale = dataAge > 10;

    if (state.gateway.connected) {
        if (isStale) {
            statusDot.className = 'status-dot stale';
            statusText.textContent = `Connected (Stale ${dataAge.toFixed(0)}s)`;
        } else {
            statusDot.className = 'status-dot connected';
            statusText.textContent = 'Connected';
        }
        overlay.style.display = 'none';
    } else {
        statusDot.className = 'status-dot disconnected';
        statusText.textContent = 'Gateway Disconnected';
        overlay.style.display = 'flex';
    }
}

function updateStatusBar(state) {
    const summary = `${Object.keys(state.nodes).length} Sensing, ${state.relay_nodes} Relay`;
    document.getElementById('node-summary').textContent = summary;
    document.getElementById('last-update').textContent = `Last update: ${state.timestamp}`;
}
