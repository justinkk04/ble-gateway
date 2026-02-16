#!/usr/bin/env python3
"""
BLE Mesh Dashboard — Standalone web visualizer

Reads mesh_state.json (exported by gateway.py) and serves a web UI
showing the mesh network topology, node status, and sensor data.

Usage:
    python dashboard.py                    # Default: port 5555
    python dashboard.py --port 8888        # Custom port
    python dashboard.py --mock             # Mock data for UI development
"""

import argparse
import json
import os
import time
from pathlib import Path

from flask import Flask, jsonify, render_template, send_from_directory

app = Flask(__name__,
            template_folder='templates',
            static_folder='static')

# Path to mesh_state.json (one directory up, next to gateway.py)
STATE_FILE = Path(__file__).parent.parent / "mesh_state.json"

# Mock data for development without a live gateway
MOCK_STATE = {
    "timestamp": "2026-02-15T18:30:00",
    "gateway": {
        "connected": True,
        "device_name": "ESP-BLE-MESH",
        "device_address": "98:A3:16:B1:C9:8A"
    },
    "power_manager": {
        "active": True,
        "threshold_mw": 5000,
        "budget_mw": 4500,
        "priority_node": "2",
        "total_power_mw": 4243
    },
    "nodes": {
        "1": {
            "role": "sensing",
            "duty": 100,
            "voltage": 12.294,
            "current": 1.25,
            "power": 15.4,
            "responsive": True,
            "last_seen": time.time(),
            "commanded_duty": 100,
            "target_duty": 100
        },
        "2": {
            "role": "sensing",
            "duty": 0,
            "voltage": 11.735,
            "current": 502.5,
            "power": 5896.8,
            "responsive": True,
            "last_seen": time.time(),
            "commanded_duty": 0,
            "target_duty": 0
        }
    },
    "relay_nodes": 1,
    "sensing_node_count": 3,
    "topology": {
        "node_roles": {
            "1": "direct",
            "2": "direct"
        }
    }
}


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/state')
def get_state():
    """Return current mesh state as JSON."""
    if app.config.get('MOCK_MODE'):
        # Update timestamps in mock data to keep it "alive"
        MOCK_STATE["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        for node in MOCK_STATE["nodes"].values():
            node["last_seen"] = time.time()
        return jsonify(MOCK_STATE)

    try:
        if STATE_FILE.exists():
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
            return jsonify(state)
        else:
            return jsonify({"error": "mesh_state.json not found", "hint": "Is gateway.py running?"}), 404
    except json.JSONDecodeError:
        return jsonify({"error": "mesh_state.json is malformed"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)


def main():
    parser = argparse.ArgumentParser(description='BLE Mesh Dashboard')
    parser.add_argument('--port', type=int, default=5555, help='Port to serve on (default: 5555)')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--mock', action='store_true', help='Use mock data for UI development')
    args = parser.parse_args()

    app.config['MOCK_MODE'] = args.mock

    if args.mock:
        print(f"\n  🎨 MOCK MODE — using fake mesh data for UI development\n")
    else:
        print(f"\n  Reading mesh state from: {STATE_FILE}")
        if not STATE_FILE.exists():
            print(f"  ⚠  mesh_state.json not found — start gateway.py first")
        print()

    print(f"  ╚══════════════════════════════════════╝\n")

    # Disable debug/reloader to prevent restarting when mesh_state.json changes
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == '__main__':
    main()
