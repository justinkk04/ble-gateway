#!/usr/bin/env python3
"""
BLE Mesh Dashboard v0.8 — Tabbed web interface

Reads mesh_state.json (exported by gateway.py) and serves a web UI with
four tabs: Topology, Nodes, History, Console.

The Console tab writes commands to mesh_commands.json which gateway.py
can poll to execute. Responses come back via mesh_state.json updates.

Usage:
    python dashboard.py                    # Default: port 5555
    python dashboard.py --port 8888        # Custom port
    python dashboard.py --mock             # Mock data for UI development
"""

import argparse
import json
import os
import sqlite3
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, jsonify, render_template, request, send_from_directory

app = Flask(__name__,
            template_folder='templates',
            static_folder='static')

# Paths
STATE_FILE = Path(__file__).parent.parent / "mesh_state.json"
DB_FILE = Path(__file__).parent / "mesh_data.db"
COMMAND_FILE = Path(__file__).parent.parent / "mesh_commands.json"

# Console log buffer (in-memory ring buffer for the UI)
_console_log = deque(maxlen=500)
_console_lock = threading.Lock()

# --------------------------------------------------------------------------- #
#  SQLite helpers
# --------------------------------------------------------------------------- #

def get_db():
    db = sqlite3.connect(str(DB_FILE), timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=3000")
    return db


def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now','localtime')),
            node_id     TEXT    NOT NULL,
            duty        INTEGER NOT NULL DEFAULT 0,
            voltage     REAL    NOT NULL DEFAULT 0.0,
            current_ma  REAL    NOT NULL DEFAULT 0.0,
            power_mw    REAL    NOT NULL DEFAULT 0.0,
            responsive  INTEGER NOT NULL DEFAULT 1,
            commanded_duty INTEGER NOT NULL DEFAULT 0,
            target_duty INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_readings_node    ON sensor_readings(node_id);
        CREATE INDEX IF NOT EXISTS idx_readings_ts      ON sensor_readings(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_readings_node_ts ON sensor_readings(node_id, timestamp DESC);

        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS node_aliases (
            node_id TEXT PRIMARY KEY,
            alias   TEXT NOT NULL
        );
    """)

    # Seed default settings if not present
    defaults = [
        ("theme", "dark"),
        ("poll_interval", "2"),
    ]
    for k, v in defaults:
        db.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))
    db.commit()
    db.close()


_last_ingested_ts = None
_ingest_lock = threading.Lock()


def ingest_state(state: dict):
    global _last_ingested_ts
    ts = state.get("timestamp")
    if not ts:
        return
    with _ingest_lock:
        if ts == _last_ingested_ts:
            return
        _last_ingested_ts = ts

    nodes = state.get("nodes", {})
    if not nodes:
        return

    db = get_db()
    try:
        rows = []
        for nid, data in nodes.items():
            rows.append((
                ts, str(nid),
                data.get("duty", 0), data.get("voltage", 0.0),
                data.get("current", 0.0), data.get("power", 0.0),
                1 if data.get("responsive", True) else 0,
                data.get("commanded_duty", 0), data.get("target_duty", 0),
            ))
        db.executemany("""
            INSERT INTO sensor_readings
                (timestamp, node_id, duty, voltage, current_ma, power_mw,
                 responsive, commanded_duty, target_duty)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        db.commit()
    except Exception as e:
        print(f"[DB] ingest error: {e}")
    finally:
        db.close()


# --------------------------------------------------------------------------- #
#  Background ingestion thread
# --------------------------------------------------------------------------- #

def _bg_ingest_loop():
    while True:
        try:
            if app.config.get("MOCK_MODE"):
                ingest_state(_build_mock_state())
            elif STATE_FILE.exists():
                with open(STATE_FILE, "r") as f:
                    state = json.load(f)
                ingest_state(state)
        except Exception:
            pass
        time.sleep(2)


# --------------------------------------------------------------------------- #
#  Mock data
# --------------------------------------------------------------------------- #

MOCK_STATE_TEMPLATE = {
    "timestamp": "",
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
            "role": "sensing", "duty": 100, "voltage": 12.294,
            "current": 1.25, "power": 15.4, "responsive": True,
            "last_seen": 0, "commanded_duty": 100, "target_duty": 100
        },
        "2": {
            "role": "sensing", "duty": 0, "voltage": 11.735,
            "current": 502.5, "power": 5896.8, "responsive": True,
            "last_seen": 0, "commanded_duty": 0, "target_duty": 0
        }
    },
    "relay_nodes": 1,
    "sensing_node_count": 3,
    "topology": {"node_roles": {"1": "direct", "2": "direct"}}
}


def _build_mock_state():
    import copy, math, random
    state = copy.deepcopy(MOCK_STATE_TEMPLATE)
    now = time.time()
    state["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    for node in state["nodes"].values():
        node["last_seen"] = now
    state["nodes"]["1"]["voltage"] = round(12.2 + 0.1 * math.sin(now / 5), 3)
    state["nodes"]["1"]["current"] = round(1.2 + 0.1 * random.random(), 2)
    state["nodes"]["1"]["power"] = round(
        state["nodes"]["1"]["voltage"] * state["nodes"]["1"]["current"], 1)
    state["nodes"]["2"]["voltage"] = round(11.7 + 0.05 * math.sin(now / 7), 3)
    state["nodes"]["2"]["current"] = round(500 + 10 * random.random(), 1)
    state["nodes"]["2"]["power"] = round(
        state["nodes"]["2"]["voltage"] * state["nodes"]["2"]["current"], 1)
    state["power_manager"]["total_power_mw"] = round(
        state["nodes"]["1"]["power"] + state["nodes"]["2"]["power"], 0)
    return state


# --------------------------------------------------------------------------- #
#  Routes
# --------------------------------------------------------------------------- #

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/state')
def get_state():
    if app.config.get('MOCK_MODE'):
        state = _build_mock_state()
        ingest_state(state)
        return jsonify(state)
    try:
        if STATE_FILE.exists():
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
            ingest_state(state)
            return jsonify(state)
        else:
            return jsonify({"error": "mesh_state.json not found",
                            "hint": "Is gateway.py running?"}), 404
    except json.JSONDecodeError:
        return jsonify({"error": "mesh_state.json is malformed"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/history')
def get_history():
    node_id = request.args.get("node_id")
    limit = min(int(request.args.get("limit", 200)), 2000)
    offset = int(request.args.get("offset", 0))
    since = request.args.get("since")
    until = request.args.get("until")

    clauses, params = [], []
    if node_id:
        clauses.append("node_id = ?"); params.append(node_id)
    if since:
        clauses.append("timestamp >= ?"); params.append(since)
    if until:
        clauses.append("timestamp <= ?"); params.append(until)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    db = get_db()
    try:
        total = db.execute(
            f"SELECT COUNT(*) as cnt FROM sensor_readings{where}", params
        ).fetchone()["cnt"]

        rows = db.execute(
            f"SELECT * FROM sensor_readings{where} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            params + [limit, offset]
        ).fetchall()

        readings = [{
            "id": r["id"], "timestamp": r["timestamp"], "node_id": r["node_id"],
            "duty": r["duty"], "voltage": r["voltage"],
            "current_ma": r["current_ma"], "power_mw": r["power_mw"],
            "responsive": bool(r["responsive"]),
            "commanded_duty": r["commanded_duty"], "target_duty": r["target_duty"],
        } for r in rows]

        return jsonify({"readings": readings, "total": total, "limit": limit, "offset": offset})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route('/api/history/node/<node_id>')
def get_node_history(node_id):
    """Return time-series data for a single node (for charts).

    Query params:
        minutes – lookback window (default 30, 0 = all history)
        limit   – max points (default 500, 0 minutes bumps to 5000)
    """
    minutes = int(request.args.get("minutes", 30))
    default_limit = 5000 if minutes == 0 else 500
    limit = min(int(request.args.get("limit", default_limit)), 10000)

    db = get_db()
    try:
        if minutes > 0:
            cutoff = (datetime.now() - timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S")
            rows = db.execute("""
                SELECT timestamp, duty, voltage, current_ma, power_mw
                FROM sensor_readings
                WHERE node_id = ? AND timestamp >= ?
                ORDER BY timestamp ASC
                LIMIT ?
            """, [node_id, cutoff, limit]).fetchall()
        else:
            cutoff = None
            rows = db.execute("""
                SELECT timestamp, duty, voltage, current_ma, power_mw
                FROM sensor_readings
                WHERE node_id = ?
                ORDER BY timestamp ASC
                LIMIT ?
            """, [node_id, limit]).fetchall()

        points = [dict(r) for r in rows]
        return jsonify({"node_id": node_id, "points": points, "since": cutoff})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route('/api/history/summary')
def get_history_summary():
    minutes = int(request.args.get("minutes", 60))
    cutoff = (datetime.now() - timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S")
    db = get_db()
    try:
        rows = db.execute("""
            SELECT node_id, COUNT(*) as readings,
                ROUND(AVG(duty), 1) as avg_duty,
                ROUND(AVG(voltage), 3) as avg_voltage,
                ROUND(AVG(current_ma), 1) as avg_current,
                ROUND(AVG(power_mw), 1) as avg_power,
                ROUND(MIN(power_mw), 1) as min_power,
                ROUND(MAX(power_mw), 1) as max_power,
                MIN(timestamp) as first_reading,
                MAX(timestamp) as last_reading
            FROM sensor_readings WHERE timestamp >= ?
            GROUP BY node_id ORDER BY node_id
        """, [cutoff]).fetchall()
        return jsonify({"summary": [dict(r) for r in rows], "since": cutoff, "minutes": minutes})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route('/api/db/stats')
def get_db_stats():
    db = get_db()
    try:
        total_rows = db.execute("SELECT COUNT(*) as cnt FROM sensor_readings").fetchone()["cnt"]
        db_size = DB_FILE.stat().st_size if DB_FILE.exists() else 0
        return jsonify({
            "total_rows": total_rows,
            "db_size_bytes": db_size,
            "db_size_mb": round(db_size / (1024 * 1024), 2),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


# --------------------------------------------------------------------------- #
#  Console / Command API
# --------------------------------------------------------------------------- #

@app.route('/api/command', methods=['POST'])
def post_command():
    """Accept a command from the console tab.

    Writes to mesh_commands.json for gateway.py to pick up.
    In mock mode, simulates a response.
    """
    body = request.get_json(force=True)
    cmd_text = body.get("command", "").strip()
    if not cmd_text:
        return jsonify({"error": "Empty command"}), 400

    ts = datetime.now().strftime("%H:%M:%S")
    log_entry = {"time": ts, "type": "cmd", "text": cmd_text}

    with _console_lock:
        _console_log.append(log_entry)

    if app.config.get("MOCK_MODE"):
        # Simulate responses
        resp_text = _mock_command_response(cmd_text)
        resp_entry = {"time": ts, "type": "resp", "text": resp_text}
        with _console_lock:
            _console_log.append(resp_entry)
        return jsonify({"status": "ok", "response": resp_text})

    # Write command to file for gateway.py to pick up
    try:
        cmd_data = {
            "timestamp": datetime.now().isoformat(),
            "command": cmd_text,
            "status": "pending"
        }
        tmp = str(COMMAND_FILE) + ".tmp"
        with open(tmp, 'w') as f:
            json.dump(cmd_data, f)
        os.replace(tmp, str(COMMAND_FILE))

        resp_entry = {"time": ts, "type": "info", "text": f"Sent: {cmd_text}"}
        with _console_lock:
            _console_log.append(resp_entry)
        return jsonify({"status": "ok", "response": f"Command queued: {cmd_text}"})
    except Exception as e:
        err_entry = {"time": ts, "type": "error", "text": str(e)}
        with _console_lock:
            _console_log.append(err_entry)
        return jsonify({"error": str(e)}), 500


@app.route('/api/command/log')
def get_command_log():
    """Return recent console log entries."""
    since_idx = int(request.args.get("since", 0))
    with _console_lock:
        entries = list(_console_log)
    # Return entries after the given index
    if since_idx > 0 and since_idx < len(entries):
        entries = entries[since_idx:]
    return jsonify({"entries": entries, "total": len(list(_console_log))})


def _mock_command_response(cmd: str) -> str:
    """Generate a fake response for mock mode."""
    parts = cmd.upper().split()
    if not parts:
        return "ERROR: empty command"
    c = parts[0]
    if c in ('HELP', '?'):
        return ("Commands: node <id>, duty <0-100>, ramp, stop, read, status, "
                "monitor, threshold <mW>, priority <id>, power")
    if c == 'READ':
        return "NODE1:DATA:D:75%,V:12.30V,I:250.5mA,P:3081.2mW"
    if c == 'STATUS':
        return "NODE1: RUNNING duty=75% | NODE2: RUNNING duty=50%"
    if c == 'STOP':
        return "SENT:ALL:STOP"
    if c == 'RAMP':
        return "SENT:0:RAMP"
    if c == 'POWER':
        return ("Threshold: 5000 mW | Budget: 4500 mW | "
                "Total: 4243 mW | Headroom: 757 mW")
    if c.startswith('DUTY'):
        val = parts[1] if len(parts) > 1 else '?'
        return f"SENT:0:DUTY:{val}"
    if c.startswith('NODE'):
        nid = parts[1] if len(parts) > 1 else '0'
        return f"Target node: {nid}"
    if c.startswith('THRESHOLD'):
        val = parts[1] if len(parts) > 1 else '?'
        return f"Threshold set: {val} mW"
    if c.startswith('PRIORITY'):
        val = parts[1] if len(parts) > 1 else '?'
        return f"Priority node: {val}"
    return f"OK: {cmd}"


@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)


# --------------------------------------------------------------------------- #
#  Settings API
# --------------------------------------------------------------------------- #

@app.route('/api/settings')
def get_settings():
    """Return all settings as a key-value dict."""
    db = get_db()
    try:
        rows = db.execute("SELECT key, value FROM settings").fetchall()
        settings = {r["key"]: r["value"] for r in rows}
        return jsonify(settings)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route('/api/settings', methods=['PUT'])
def put_settings():
    """Update one or more settings. Body: {"key": "value", ...}"""
    body = request.get_json(force=True)
    if not body or not isinstance(body, dict):
        return jsonify({"error": "Expected JSON object"}), 400

    db = get_db()
    try:
        for k, v in body.items():
            db.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(k), str(v)))
        db.commit()
        # Return all settings
        rows = db.execute("SELECT key, value FROM settings").fetchall()
        return jsonify({r["key"]: r["value"] for r in rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


# --------------------------------------------------------------------------- #
#  Node Aliases API
# --------------------------------------------------------------------------- #

@app.route('/api/nodes/aliases')
def get_node_aliases():
    """Return node_id -> alias mapping."""
    db = get_db()
    try:
        rows = db.execute("SELECT node_id, alias FROM node_aliases").fetchall()
        aliases = {r["node_id"]: r["alias"] for r in rows}
        return jsonify(aliases)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route('/api/nodes/<node_id>/rename', methods=['PUT'])
def rename_node(node_id):
    """Set or update alias for a node. Body: {"alias": "My Sensor"}"""
    body = request.get_json(force=True)
    alias = body.get("alias", "").strip()
    if not alias:
        return jsonify({"error": "alias is required"}), 400
    if len(alias) > 50:
        return jsonify({"error": "alias must be 50 chars or less"}), 400

    db = get_db()
    try:
        db.execute(
            "INSERT INTO node_aliases (node_id, alias) VALUES (?, ?) "
            "ON CONFLICT(node_id) DO UPDATE SET alias = excluded.alias",
            (str(node_id), alias))
        db.commit()
        return jsonify({"node_id": node_id, "alias": alias})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route('/api/nodes/<node_id>/alias', methods=['DELETE'])
def delete_node_alias(node_id):
    """Remove alias for a node (revert to default name)."""
    db = get_db()
    try:
        db.execute("DELETE FROM node_aliases WHERE node_id = ?", (str(node_id),))
        db.commit()
        return jsonify({"status": "ok", "node_id": node_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


# --------------------------------------------------------------------------- #
#  Node Removal API
# --------------------------------------------------------------------------- #

@app.route('/api/nodes/<node_id>', methods=['DELETE'])
def delete_node(node_id):
    """Remove a node: deletes its history and alias."""
    db = get_db()
    try:
        hist = db.execute(
            "SELECT COUNT(*) as cnt FROM sensor_readings WHERE node_id = ?",
            (str(node_id),)).fetchone()["cnt"]
        db.execute("DELETE FROM sensor_readings WHERE node_id = ?", (str(node_id),))
        db.execute("DELETE FROM node_aliases WHERE node_id = ?", (str(node_id),))
        db.commit()
        return jsonify({
            "status": "ok",
            "node_id": node_id,
            "readings_deleted": hist
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


# --------------------------------------------------------------------------- #
#  History Clear API
# --------------------------------------------------------------------------- #

@app.route('/api/history/clear', methods=['DELETE'])
def clear_history():
    """Delete ALL sensor readings. Optional: ?node_id=X for one node only."""
    node_id = request.args.get("node_id")
    db = get_db()
    try:
        if node_id:
            cnt = db.execute(
                "SELECT COUNT(*) as cnt FROM sensor_readings WHERE node_id = ?",
                (node_id,)).fetchone()["cnt"]
            db.execute("DELETE FROM sensor_readings WHERE node_id = ?", (node_id,))
        else:
            cnt = db.execute(
                "SELECT COUNT(*) as cnt FROM sensor_readings").fetchone()["cnt"]
            db.execute("DELETE FROM sensor_readings")
        db.commit()
        try:
            db.execute("VACUUM")
        except Exception:
            pass  # VACUUM may fail in some contexts; non-critical
        return jsonify({"status": "ok", "deleted": cnt})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


# --------------------------------------------------------------------------- #
#  Main
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(description='BLE Mesh Dashboard')
    parser.add_argument('--port', type=int, default=5555)
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--mock', action='store_true')
    args = parser.parse_args()

    app.config['MOCK_MODE'] = args.mock
    init_db()
    print(f"  Database: {DB_FILE}")

    t = threading.Thread(target=_bg_ingest_loop, daemon=True, name="db-ingest")
    t.start()

    if args.mock:
        print(f"\n  MOCK MODE - fake mesh data\n")
    else:
        print(f"\n  State: {STATE_FILE}")
        if not STATE_FILE.exists():
            print(f"  WARNING: mesh_state.json not found")
        print()

    app.run(host=args.host, port=args.port, debug=False)


if __name__ == '__main__':
    main()
