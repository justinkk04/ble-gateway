#!/usr/bin/env python3
"""
BLE Gateway for DC Monitor Mesh Network
Connects to ESP32-C6 GATT gateway and sends commands to mesh nodes

Usage:
    python gateway.py                       # TUI interactive mode (default)
    python gateway.py --scan                # Just scan for gateways
    python gateway.py --node 0 --ramp       # Send RAMP to node 0
    python gateway.py --node 1 --duty 50    # Set duty 50% on node 1
    python gateway.py --node all --stop     # Stop all nodes
    python gateway.py --node 0 --read       # Single sensor reading
    python gateway.py --node 0 --monitor    # Continuous monitoring
    python gateway.py --no-tui              # Plain CLI mode (legacy)

Commands are sent as NODE_ID:COMMAND[:VALUE] to the ESP32-C6 mesh gateway,
which forwards them to the targeted mesh node via BLE Mesh.
"""

import asyncio
import argparse
import re
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

try:
    from bleak import BleakClient, BleakScanner
    from bleak.backends.characteristic import BleakGATTCharacteristic
except ImportError:
    print("ERROR: bleak not installed. Run: pip install bleak")
    sys.exit(1)

# Textual TUI imports — only needed for interactive mode
_HAS_TEXTUAL = False
try:
    from textual.app import App, ComposeResult
    from textual.containers import Horizontal, Vertical
    from textual.message import Message
    from textual.widgets import Header, Footer, Input, RichLog, DataTable, Static
    from textual import work, on
    _HAS_TEXTUAL = True
except ImportError as e:
    print(f"Note: textual not available ({e}). Install with: pip install textual")
    print("      Falling back to plain CLI mode.\n")

# Custom UUIDs matching ESP32-C6 ble_service.h
DC_MONITOR_SERVICE_UUID = "0000dc01-0000-1000-8000-00805f9b34fb"
SENSOR_DATA_CHAR_UUID = "0000dc02-0000-1000-8000-00805f9b34fb"
COMMAND_CHAR_UUID = "0000dc03-0000-1000-8000-00805f9b34fb"

# Device name prefixes to look for
# Before provisioning: "Mesh-Gateway" (custom GATT advert)
# After provisioning: "ESP-BLE-MESH" (mesh GATT proxy advert)
DEVICE_NAME_PREFIXES = ["Mesh-Gateway", "ESP-BLE-MESH"]

# Sensor data parsing regex (case-insensitive for mA/mW/MA/MW)
SENSOR_RE = re.compile(r'D:(\d+)%,V:([\d.]+)V,I:([\d.]+)mA,P:([\d.]+)mW', re.IGNORECASE)
NODE_ID_RE = re.compile(r'NODE(\d+)', re.IGNORECASE)


class BleThread:
    """Dedicated thread with a persistent asyncio event loop for bleak BLE operations.

    Bleak on Linux/BlueZ uses D-Bus signals for GATT notifications. These
    signals require a running event loop to be pumped. Textual's @work workers
    create short-lived event loops that die when the worker returns, orphaning
    the D-Bus signal handlers. BleThread provides a single persistent loop.
    """

    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """Spawn the daemon thread and block until its loop is running."""
        ready = threading.Event()

        def _run():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.set_exception_handler(self._exception_handler)
            ready.set()
            self._loop.run_forever()
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()

        self._thread = threading.Thread(target=_run, daemon=True, name="ble-io")
        self._thread.start()
        ready.wait()

    def submit(self, coro) -> 'asyncio.Future':
        """Submit a coroutine to the BLE loop. Returns concurrent.futures.Future."""
        if self._loop is None:
            raise RuntimeError("BleThread not started")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    async def submit_async(self, coro):
        """Submit a coroutine and await its result from another async context."""
        future = self.submit(coro)
        return await asyncio.wrap_future(future)

    def stop(self):
        """Stop the event loop and join the thread."""
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=5.0)
        self._loop = None
        self._thread = None

    def _exception_handler(self, loop, context):
        msg = context.get("message", "Unhandled exception in BLE thread")
        exc = context.get("exception")
        if exc:
            tb = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            print(f"[BLE THREAD ERROR] {msg}\n{tb}")
        else:
            print(f"[BLE THREAD ERROR] {msg}")


@dataclass
class NodeState:
    """Tracks the last known state of a single mesh node."""
    node_id: str
    duty: int = 0              # Current duty from sensor reading
    target_duty: int = 0       # User-requested duty % (restored when threshold off)
    commanded_duty: int = 0    # Last duty % sent by PowerManager (not from sensor)
    voltage: float = 0.0       # V
    current: float = 0.0       # mA
    power: float = 0.0         # mW
    last_seen: float = field(default_factory=time.monotonic)
    responsive: bool = True
    poll_gen: int = 0          # Which poll cycle this data is from


class PowerManager:
    """Equilibrium-based power balancer for mesh nodes.

    Maintains total power near (threshold - headroom) by nudging node
    duty cycles up or down each poll cycle:
      - No priority: all nodes get equal power share (budget/N)
      - With priority: priority node gets PRIORITY_WEIGHT x normal share
      - Bidirectional: increases duty when under budget, decreases when over
      - Gradual: max STEP_SIZE% change per cycle prevents oscillation
    """

    POLL_INTERVAL = 3.0    # Seconds between poll cycles
    READ_STAGGER = 2.5     # Seconds between READ commands (must exceed mesh SEND_COMP time)
    STALE_TIMEOUT = 45.0   # Seconds before marking node unresponsive (relay round trips are slow)
    COOLDOWN = 5.0         # Seconds between adjustments (give mesh time to settle)
    HEADROOM_MW = 500.0    # Target buffer below threshold (budget = threshold - headroom)
    PRIORITY_WEIGHT = 2.0  # Priority node gets this many "shares" vs 1 for normal nodes

    def __init__(self, gateway):
        self.gateway = gateway
        self.nodes: dict[str, NodeState] = {}
        self.threshold_mw: Optional[float] = None
        self.priority_node: Optional[str] = None
        self._adjusting = False
        self._last_adjustment: float = 0
        self._poll_generation: int = 0
        self._polling = False  # True while a poll cycle is active
        self._needs_bootstrap = False

    # ---- Public API ----

    def set_threshold(self, mw: float):
        """Enable power management with the given threshold in mW."""
        self.threshold_mw = mw
        self._needs_bootstrap = not self.nodes
        # Snapshot current duty as target for any node not yet explicitly set
        # (handles case where user set duty BEFORE enabling threshold)
        for ns in self.nodes.values():
            if ns.target_duty == 0 and ns.duty > 0:
                ns.target_duty = ns.duty
        # Force immediate evaluation on next poll cycle
        self._last_adjustment = 0
        self._adjusting = False  # Clear any in-progress flag (race with BLE thread)
        budget = mw - self.HEADROOM_MW
        self.gateway.log(
            f"[POWER] Threshold: {mw:.0f} mW (budget: {budget:.0f} mW, headroom: {self.HEADROOM_MW:.0f} mW)")

    async def disable(self):
        """Disable power management and restore original duty cycles."""
        self.threshold_mw = None
        self._polling = False
        # Wait for any in-flight mesh commands to complete before restoring
        await asyncio.sleep(2.0)
        # Restore all nodes to their target duty with adequate spacing
        for ns in self.nodes.values():
            if ns.commanded_duty != ns.target_duty and ns.target_duty > 0:
                self.gateway.log(
                    f"[POWER] Restoring node {ns.node_id}: {ns.commanded_duty}% -> {ns.target_duty}%")
                await self.gateway.set_duty(
                    ns.node_id, ns.target_duty, _from_power_mgr=True, _silent=True)
                await asyncio.sleep(2.5)  # Must exceed mesh SEND_COMP time for relay nodes
            ns.commanded_duty = 0  # Reset commanded state
        self.gateway.log("[POWER] Threshold disabled")

    def set_priority(self, node_id: str):
        """Set the priority node. Triggers immediate rebalance."""
        self.priority_node = node_id
        self._last_adjustment = 0  # Force rebalance on next cycle
        self.gateway.log(f"[POWER] Priority node: {node_id}")

    def clear_priority(self):
        """Remove priority designation. Triggers immediate rebalance."""
        self.priority_node = None
        self._last_adjustment = 0  # Force rebalance on next cycle
        self.gateway.log("[POWER] Priority cleared")

    def set_target_duty(self, node_id: str, duty: int):
        """Record the user-requested duty for a node."""
        if node_id not in self.nodes:
            self.nodes[node_id] = NodeState(node_id=node_id)
        self.nodes[node_id].target_duty = duty

    def status(self) -> str:
        """Return a human-readable status summary."""
        lines = []
        lines.append("--- Power Manager ---")
        if self.threshold_mw is not None:
            budget = self.threshold_mw - self.HEADROOM_MW
            lines.append(f"Threshold: {self.threshold_mw:.0f} mW")
            lines.append(f"Budget:    {budget:.0f} mW (headroom: {self.HEADROOM_MW:.0f} mW)")
        else:
            lines.append("Threshold: OFF")
        if self.priority_node is not None:
            lines.append(f"Priority:  node {self.priority_node}")
        else:
            lines.append("Priority:  none")

        total = 0.0
        responsive_count = sum(1 for ns in self.nodes.values() if ns.responsive)
        if self.nodes:
            # Calculate shares for display
            share_info = {}
            if self.threshold_mw is not None and responsive_count > 0:
                budget = self.threshold_mw - self.HEADROOM_MW
                if self.priority_node and self.priority_node in self.nodes:
                    total_shares = self.PRIORITY_WEIGHT + (responsive_count - 1)
                    for nid in self.nodes:
                        if nid == self.priority_node:
                            share_info[nid] = budget * (self.PRIORITY_WEIGHT / total_shares)
                        else:
                            share_info[nid] = budget * (1.0 / total_shares)
                else:
                    per_share = budget / responsive_count
                    for nid in self.nodes:
                        share_info[nid] = per_share

            lines.append("Nodes:")
            for nid in sorted(self.nodes.keys()):
                ns = self.nodes[nid]
                st = "ok" if ns.responsive else "stale"
                target = f" (target:{ns.target_duty}%)" if ns.target_duty != ns.duty else ""
                share = f" share:{share_info[nid]:.0f}mW" if nid in share_info else ""
                lines.append(
                    f"  Node {nid}: D:{ns.duty}%{target} "
                    f"V:{ns.voltage:.2f}V I:{ns.current:.1f}mA "
                    f"P:{ns.power:.0f}mW [{st}]{share}"
                )
                if ns.responsive:
                    total += ns.power
            lines.append(f"Total power: {total:.0f} mW")
            if self.threshold_mw is not None:
                headroom = self.threshold_mw - total
                lines.append(f"Headroom:    {headroom:.0f} mW")
        else:
            lines.append("No nodes discovered yet")
        lines.append("--------------------")
        return "\n".join(lines)

    # ---- Notification Hook ----

    def on_sensor_data(self, node_id: str, duty: int, voltage: float,
                       current: float, power: float):
        """Update node state from parsed sensor data."""
        if node_id not in self.nodes:
            self.nodes[node_id] = NodeState(node_id=node_id)

        ns = self.nodes[node_id]
        ns.duty = duty
        ns.voltage = voltage
        ns.current = current
        ns.power = power
        ns.last_seen = time.monotonic()
        ns.responsive = True
        ns.poll_gen = self._poll_generation

        # Only sync commanded_duty when PM is OFF — when PM is active,
        # only _nudge_node() updates commanded_duty (avoids stale sensor
        # data overwriting what PM just sent, which causes oscillation)
        if self.threshold_mw is None:
            ns.commanded_duty = duty

        # Don't auto-sync target_duty from sensor data — it must only be set
        # by explicit user commands (set_target_duty). Auto-sync caused PM to
        # "forget" the original target after disable() because sensor data
        # reported the reduced duty, overwriting target_duty.

    # ---- Internal Control Loop ----

    async def _bootstrap_discovery(self):
        """Discover mesh nodes by sending READ to each expected address individually.

        Avoids ALL:READ because the GATT gateway serializes it with 2.5s delays
        plus probes for undiscovered nodes (causing 5s timeouts).
        Individual sends are faster and more reliable.
        """
        EXPECTED_NODES = 2  # Increase if you add more mesh nodes
        MAX_RETRIES = 3
        self.gateway.log("[POWER] Discovering mesh nodes...")
        for nid in range(1, EXPECTED_NODES + 1):
            if self.threshold_mw is None:
                return
            nid_str = str(nid)
            for attempt in range(MAX_RETRIES):
                if nid_str in self.nodes:
                    break  # Node responded
                if attempt > 0:
                    self.gateway.log(
                        f"[POWER] Node {nid} not found, attempt {attempt+1}/{MAX_RETRIES}...")
                await self.gateway.send_to_node(nid_str, "READ", _silent=True)
                await asyncio.sleep(self.READ_STAGGER)

    async def poll_loop(self):
        """Periodic poll-and-adjust cycle. Called by TUI @work or asyncio task."""
        if self._polling:
            return  # Already running — threshold changes are picked up automatically
        try:
            if getattr(self, '_needs_bootstrap', False):
                self._needs_bootstrap = False
                await self._bootstrap_discovery()
                await asyncio.sleep(2.0)
            self._polling = True
            while self.threshold_mw is not None:
                await self._poll_all_nodes()
                await self._wait_for_responses(timeout=4.0)
                self._mark_stale_nodes()
                await asyncio.sleep(1.0)  # Relay breathing gap — let radio catch up
                await self._evaluate_and_adjust()
                await asyncio.sleep(self.POLL_INTERVAL)
            self._polling = False
        except asyncio.CancelledError:
            self._polling = False

    async def _poll_all_nodes(self):
        """Poll each known node individually with READ.

        Sends 1:READ, waits, 2:READ, waits — instead of ALL:READ which
        routes through the GATT gateway's slow sequential handler.
        """
        self._poll_generation += 1
        if not self.nodes:
            return
        node_ids = sorted(self.nodes.keys(), key=lambda x: int(x) if x.isdigit() else 999)
        for node_id in node_ids:
            if self.threshold_mw is None:
                return
            if not node_id.isdigit():
                continue
            await self.gateway.send_to_node(node_id, "READ", _silent=True)
            await asyncio.sleep(self.READ_STAGGER)

    async def _wait_for_responses(self, timeout: float = 3.0):
        """Wait until all responsive nodes report for this poll cycle, or timeout."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.threshold_mw is None:
                return
            all_fresh = all(
                ns.poll_gen == self._poll_generation
                for ns in self.nodes.values()
                if ns.responsive
            )
            if all_fresh:
                return
            await asyncio.sleep(0.1)

    def _mark_stale_nodes(self):
        """Mark nodes that haven't responded recently as unresponsive."""
        now = time.monotonic()
        for ns in self.nodes.values():
            if not ns.node_id.isdigit():
                continue  # Skip phantom nodes like "ALL"
            age = now - ns.last_seen
            if age > self.STALE_TIMEOUT:
                if ns.responsive:
                    self.gateway.log(
                        f"[POWER] Node {ns.node_id} unresponsive ({age:.0f}s)")
                ns.responsive = False

    async def _evaluate_and_adjust(self):
        """Bidirectional equilibrium: nudge nodes toward their power budget share.

        Increases duty when under budget, decreases when over.
        Dead band prevents jitter when close to target.
        """
        if self.threshold_mw is None or self._adjusting:
            return

        since = time.monotonic() - self._last_adjustment
        if since < self.COOLDOWN:
            return

        responsive = {nid: ns for nid, ns in self.nodes.items() if ns.responsive}
        if not responsive:
            return

        budget = self.threshold_mw - self.HEADROOM_MW
        if budget <= 0:
            return

        total_power = sum(ns.power for ns in responsive.values())

        # Dead band: skip if within 5% of budget (prevents constant jitter)
        deadband = budget * 0.05
        if abs(total_power - budget) < deadband:
            return

        # Skip if all nodes are at/above target_duty and total under budget
        all_at_target = all(
            (ns.commanded_duty >= ns.target_duty or ns.target_duty == 0)
            for ns in responsive.values()
        )
        if all_at_target and total_power <= budget:
            return

        self.gateway.log(
            f"[POWER] Adjusting: {total_power:.0f}/{budget:.0f}mW, "
            f"nodes: {list(responsive.keys())}")

        self._adjusting = True
        try:
            if self.priority_node and self.priority_node in responsive:
                await self._balance_with_priority(responsive, budget)
            else:
                await self._balance_proportional(responsive, budget)

            self._last_adjustment = time.monotonic()
        finally:
            self._adjusting = False

    def _estimate_mw_per_pct(self, ns: NodeState, all_nodes: dict) -> float:
        """Estimate milliwatts per duty% for a node.

        Uses commanded_duty (what PM sent) instead of sensor-reported duty
        to avoid oscillation from stale sensor data lagging PM commands.
        """
        duty_value = ns.commanded_duty if ns.commanded_duty > 0 else ns.duty
        if duty_value > 0 and ns.power > 0:
            return ns.power / duty_value
        # Fallback: average from other nodes that have data
        estimates = []
        for n in all_nodes.values():
            d = n.commanded_duty if n.commanded_duty > 0 else n.duty
            if d > 0 and n.power > 0:
                estimates.append(n.power / d)
        if estimates:
            return sum(estimates) / len(estimates)
        return 50.0  # Last resort default

    async def _nudge_node(self, nid: str, ns: NodeState, target_share_mw: float,
                          all_nodes: dict) -> str | None:
        """Nudge a single node's duty toward its target power share.

        Returns a change description string, or None if no change needed.
        Sends the duty command once — retries happen on the next poll cycle
        instead of blocking here (which caused cascading delays).
        """
        mw_per_pct = self._estimate_mw_per_pct(ns, all_nodes)
        ideal_duty = target_share_mw / mw_per_pct

        # Clamp to [0, target_duty] — never exceed user's original setting
        ceiling = ns.target_duty if ns.target_duty > 0 else 100
        ideal_duty = max(0, min(ceiling, ideal_duty))

        current = ns.commanded_duty if ns.commanded_duty > 0 else ns.duty
        new_duty = int(ideal_duty)
        if new_duty == current:
            return None

        new_duty = max(0, min(100, new_duty))
        if new_duty == current:
            return None

        change = f"N{nid}:{current}->{new_duty}%"
        await self.gateway.set_duty(nid, new_duty, _from_power_mgr=True, _silent=True)
        ns.commanded_duty = new_duty
        await asyncio.sleep(2.5)  # Single wait for mesh round trip
        return change

    async def _balance_proportional(self, nodes: dict, budget: float):
        """Equal power shares: each node gets budget/N."""
        n = len(nodes)
        share_mw = budget / n

        changes = []
        for nid, ns in sorted(nodes.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 999):
            change = await self._nudge_node(nid, ns, share_mw, nodes)
            if change:
                changes.append(change)

        total_power = sum(ns.power for ns in nodes.values())
        if changes:
            self.gateway.log(
                f"[POWER] Balancing {total_power:.0f}/{budget:.0f}mW "
                f"(share:{share_mw:.0f}mW each) — {', '.join(changes)}")

    async def _balance_with_priority(self, nodes: dict, budget: float):
        """Weighted power shares: priority node gets PRIORITY_WEIGHT x normal share."""
        priority_ns = nodes[self.priority_node]
        non_priority = {nid: ns for nid, ns in nodes.items()
                        if nid != self.priority_node}

        # Calculate weighted shares
        total_shares = self.PRIORITY_WEIGHT + len(non_priority)
        priority_budget = budget * (self.PRIORITY_WEIGHT / total_shares)

        # If priority can't use its full share (limited by target_duty), redistribute
        pri_mw_per_pct = self._estimate_mw_per_pct(priority_ns, nodes)
        pri_ceiling = priority_ns.target_duty if priority_ns.target_duty > 0 else 100
        pri_max_power = pri_ceiling * pri_mw_per_pct
        if pri_max_power < priority_budget and non_priority:
            # Priority can't fill its share — surplus goes to non-priority
            priority_budget = pri_max_power
            remaining = budget - priority_budget
        else:
            remaining = budget - priority_budget

        non_pri_share = remaining / len(non_priority) if non_priority else 0

        changes = []
        # Nudge priority node
        change = await self._nudge_node(self.priority_node, priority_ns,
                                        priority_budget, nodes)
        if change:
            changes.append(change + "(pri)")

        # Nudge non-priority nodes
        for nid, ns in sorted(non_priority.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 999):
            change = await self._nudge_node(nid, ns, non_pri_share, nodes)
            if change:
                changes.append(change)

        total_power = sum(ns.power for ns in nodes.values())
        if changes:
            self.gateway.log(
                f"[POWER] Balancing {total_power:.0f}/{budget:.0f}mW "
                f"(pri:{priority_budget:.0f}mW, others:{non_pri_share:.0f}mW each) "
                f"— {', '.join(changes)}")


class DCMonitorGateway:
    def __init__(self):
        self.client = None
        self.connected_device = None
        self.running = True
        self.target_node = "0"
        self._chunk_buf = ""  # Buffer for reassembling chunked notifications
        self._power_manager = None  # PowerManager instance
        self._monitoring = False  # True when monitor mode is active
        self.app = None  # Reference to TUI app (set by MeshGatewayApp)
        self.ble_thread = None  # BleThread instance (set by TUI app)

    def log(self, text: str, style: str = "", _from_thread: bool = False):
        """Post a log message to the TUI, or print() if no TUI.

        Args:
            _from_thread: Set True when calling from a non-Textual thread
                         (e.g. bleak notification callback). Uses call_from_thread
                         for safe cross-thread posting.
        """
        if self.app and _HAS_TEXTUAL:
            try:
                msg = self.app.LogMsg(text, style)
                if _from_thread:
                    self.app.call_from_thread(self.app.post_message, msg)
                else:
                    self.app.post_message(msg)
            except Exception as e:
                print(f"  {text}  [log error: {e}]")
        else:
            print(f"  {text}")

    async def scan_for_nodes(self, timeout=10.0, target_address=None):
        """Scan for DC Monitor gateway nodes.

        If target_address is given, skip name/UUID matching and just find that device.
        Otherwise, match by name prefix or service UUID.
        """
        self.log(f"Scanning for BLE devices ({timeout}s)...")

        devices = await BleakScanner.discover(timeout=timeout, return_adv=True)

        nodes = []
        for address, (device, adv_data) in devices.items():
            # If a specific address was requested, match it directly
            if target_address and device.address.upper() == target_address.upper():
                nodes.append(device)
                self.log(f"Found target: {device.name or '(no name)'} [{device.address}]")
                continue

            # Match by known name prefixes
            if device.name and any(p in device.name for p in DEVICE_NAME_PREFIXES):
                nodes.append(device)
                self.log(f"Found: {device.name} [{device.address}]")
            # Match by service UUID (pre-provisioning)
            elif adv_data.service_uuids:
                if DC_MONITOR_SERVICE_UUID.lower() in [str(u).lower() for u in adv_data.service_uuids]:
                    nodes.append(device)
                    self.log(f"Found: {device.name or 'Unknown'} [{device.address}] (by service UUID)")

        if not nodes:
            self.log("No DC Monitor gateways found")
            self.log("Tip: Make sure ESP32-C6 is powered and advertising")

        return nodes

    def notification_handler(self, characteristic: BleakGATTCharacteristic, data: bytearray):
        """Handle incoming notifications from GATT gateway.

        IMPORTANT: This runs on bleak's callback thread, NOT the Textual event loop.
        All UI updates must use call_from_thread() or log(_from_thread=True).

        Messages > 20 bytes are chunked by the gateway:
          - Continuation chunks start with '+' (data follows after the '+')
          - Final (or only) chunk has no '+' prefix
        We accumulate '+' chunks and process the full message on the final chunk.
        """
        decoded = data.decode('utf-8', errors='replace').strip()

        # Chunked reassembly: '+' prefix means more data follows
        if decoded.startswith('+'):
            self._chunk_buf += decoded[1:]  # Accumulate without the '+' prefix
            return  # Wait for final chunk

        # Final (or only) chunk - combine with any buffered data
        if self._chunk_buf:
            decoded = self._chunk_buf + decoded
            self._chunk_buf = ""

        timestamp = datetime.now().strftime("%H:%M:%S")

        # Parse vendor model responses: NODE<id>:DATA:<sensor payload>
        if ":DATA:" in decoded:
            parts = decoded.split(":DATA:", 1)
            node_tag = parts[0]  # e.g. "NODE0"
            payload = parts[1]   # e.g. "D:50%,V:12.345V,I:1234.5MA,P:15234.5MW"

            # Parse sensor values
            sensor_match = SENSOR_RE.match(payload)
            node_match = NODE_ID_RE.match(node_tag)

            if sensor_match and node_match:
                node_id = node_match.group(1)
                duty = int(sensor_match.group(1))
                voltage = float(sensor_match.group(2))
                current = float(sensor_match.group(3))
                power = float(sensor_match.group(4))

                # Feed PowerManager
                if self._power_manager:
                    self._power_manager.on_sensor_data(
                        node_id, duty, voltage, current, power)

                # Post to TUI for UI update (always use call_from_thread — we're on bleak's thread)
                if self.app and _HAS_TEXTUAL:
                    try:
                        msg = self.app.SensorDataMsg(
                            node_id, duty, voltage, current, power,
                            f"[{timestamp}] {node_tag} >> {payload}"
                        )
                        self.app.call_from_thread(self.app.post_message, msg)
                    except Exception as e:
                        print(f"  [{timestamp}] {node_tag} >> {payload}  [post error: {e}]")
                else:
                    print(f"[{timestamp}] {node_tag} >> {payload}")
            else:
                self.log(f"[{timestamp}] {node_tag} >> {payload}", _from_thread=True)

        elif decoded.startswith("ERROR:"):
            # Suppress MESH_TIMEOUT during PM polling — it's just discovery probes
            pm = self._power_manager
            if pm and pm._polling:
                pass  # Swallow errors during background polling (reduces TUI noise)
            else:
                self.log(f"[{timestamp}] !! {decoded}", style="bold red", _from_thread=True)
        elif decoded.startswith("SENT:"):
            # Only show in debug mode
            if self.app and _HAS_TEXTUAL:
                if self.app.debug_mode:
                    self.log(f"[{timestamp}] -> {decoded}", style="dim", _from_thread=True)
            else:
                print(f"[{timestamp}] -> {decoded}")
        elif decoded.startswith("MESH_READY"):
            self.log(f"[{timestamp}] {decoded}", _from_thread=True)
        elif decoded.startswith("TIMEOUT:"):
            pm = self._power_manager
            if pm and pm._polling:
                pass  # Swallow timeouts during background polling
            else:
                self.log(f"[{timestamp}] !! {decoded}", style="yellow", _from_thread=True)
        else:
            self.log(f"[{timestamp}] {decoded}", _from_thread=True)

    async def connect_to_node(self, device):
        """Connect to a specific node and subscribe to notifications"""
        self.log(f"Connecting to {device.name or device.address}...")

        self.client = BleakClient(device.address)
        try:
            await self.client.connect()
        except Exception as e:
            self.log(f"Connection failed: {e}")
            self.client = None
            return False

        if not self.client.is_connected:
            self.log("Connection failed")
            self.client = None
            return False

        self.connected_device = device

        try:
            await self.client.start_notify(SENSOR_DATA_CHAR_UUID, self.notification_handler)
            self.log("Subscribed to sensor notifications")
        except Exception as e:
            self.log(f"Could not subscribe: {e}")

        # Report negotiated MTU
        mtu = self.client.mtu_size
        self.log(f"MTU: {mtu}")

        return True

    async def disconnect(self):
        """Disconnect from current node"""
        if self.client and self.client.is_connected:
            try:
                await self.client.disconnect()
            except (EOFError, Exception) as e:
                # BlueZ/dbus can throw EOFError if connection already dropped
                pass
            self.log("Disconnected")
        self._chunk_buf = ""  # Clear stale partial data on disconnect

    async def send_command(self, cmd: str, _silent: bool = False):
        """Send raw command string to GATT gateway"""
        if not self.client or not self.client.is_connected:
            self.log("Not connected")
            return False

        try:
            await self.client.write_gatt_char(COMMAND_CHAR_UUID, cmd.encode('utf-8'))
            if not _silent:
                self.log(f"Sent: {cmd}")
            return True
        except Exception as e:
            if not _silent:
                self.log(f"Failed to send command: {e}")
            return False

    async def send_to_node(self, node: str, command: str, value: str = None,
                           _silent: bool = False):
        """Send command to a specific mesh node.

        When node is 'ALL', expands to individual sends to each known node
        (avoids the GATT gateway's slow sequential ALL handler + probe).

        Args:
            node: Node ID (0-9) or "ALL"
            command: RAMP, STOP, ON, OFF, DUTY, STATUS, READ
            value: Optional value (e.g. duty percentage)
        """
        if str(node).upper() == "ALL":
            # Expand ALL to individual sends on the Pi 5 side
            pm = self._power_manager
            if pm and pm.nodes:
                node_ids = sorted(pm.nodes.keys(),
                                  key=lambda x: int(x) if x.isdigit() else 999)
            else:
                # Fallback: try nodes 1 and 2 if PM has no state yet
                node_ids = ["1", "2"]
            for nid in node_ids:
                if not nid.isdigit():
                    continue
                if value is not None:
                    cmd = f"{nid}:{command}:{value}"
                else:
                    cmd = f"{nid}:{command}"
                await self.send_command(cmd, _silent=_silent)
                await asyncio.sleep(2.5)  # Wait for mesh round trip before next
            return True

        if value is not None:
            cmd = f"{node}:{command}:{value}"
        else:
            cmd = f"{node}:{command}"
        return await self.send_command(cmd, _silent=_silent)

    async def set_duty(self, node: str, percent: int, _from_power_mgr: bool = False,
                       _silent: bool = False):
        """Set duty cycle (0-100%) on a mesh node"""
        percent = max(0, min(100, percent))
        if self._power_manager and not _from_power_mgr:
            if str(node).upper() == "ALL":
                # Track target for ALL known nodes
                pm = self._power_manager
                if pm.nodes:
                    for nid in pm.nodes:
                        pm.set_target_duty(nid, percent)
                else:
                    for nid in ["1", "2"]:
                        pm.set_target_duty(nid, percent)
            else:
                self._power_manager.set_target_duty(str(node), percent)
        return await self.send_to_node(node, "DUTY", str(percent), _silent=_silent)

    async def start_ramp(self, node: str):
        """Start ramp test on a mesh node"""
        return await self.send_to_node(node, "RAMP")

    async def stop_node(self, node: str):
        """Stop load on a mesh node. Also stops monitoring if active."""
        self._monitoring = False
        return await self.send_to_node(node, "STOP")

    async def read_status(self, node: str):
        """Request current status from a mesh node"""
        return await self.send_to_node(node, "STATUS")

    async def read_sensor(self, node: str):
        """Request single sensor reading from a mesh node"""
        return await self.send_to_node(node, "READ")

    async def start_monitor(self, node: str):
        """Start continuous monitoring on a mesh node"""
        self._monitoring = True
        return await self.send_to_node(node, "MONITOR")

    # ---- Legacy plain CLI interactive mode (--no-tui) ----

    async def interactive_mode(self, default_node: str = "0"):
        """Interactive command mode with mesh node targeting (plain CLI)"""
        self.target_node = default_node

        print("\n" + "=" * 50)
        print("  Mesh Gateway - Interactive Mode (Plain CLI)")
        print("=" * 50)
        print(f"  Target node: {self.target_node}")
        print()
        print("Commands:")
        print("  node <id>    Switch target (0-9 or ALL)")
        print("  ramp / r     Send RAMP to target node")
        print("  stop / s     Send STOP to target node")
        print("  duty <0-100> Set duty cycle on target node")
        print("  status       Get status from target node")
        print("  read         Single sensor reading from node")
        print("  monitor / m  Start continuous monitoring")
        print("  raw <cmd>    Send raw command string")
        print("Power Management:")
        print("  threshold <mW>  Set total power limit (auto-manages duty)")
        print("  priority <id>   Set priority node (preserved during reduction)")
        print("  threshold off   Disable power management")
        print("  priority off    Clear priority node")
        print("  power           Show power manager status")
        print()
        print("  q/quit       Exit")
        print("=" * 50)
        print()

        while self.running and self.client.is_connected:
            try:
                prompt = f"[node {self.target_node}]> "
                cmd = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: input(prompt).strip().lower()
                )

                if not cmd:
                    continue
                elif cmd in ['q', 'quit', 'exit']:
                    break
                elif cmd.startswith('node'):
                    parts = cmd.split(None, 1)
                    if len(parts) < 2:
                        print("  Usage: node <0-9 or ALL>")
                        continue
                    new_node = parts[1].strip().upper()
                    if new_node == 'ALL' or (new_node.isdigit() and 0 <= int(new_node) <= 9):
                        self.target_node = new_node.lower() if new_node != 'ALL' else 'ALL'
                        print(f"  Target node: {self.target_node}")
                    else:
                        print("  Invalid node ID (use 0-9 or ALL)")
                elif cmd in ['s', 'stop']:
                    await self.stop_node(self.target_node)
                elif cmd in ['r', 'ramp']:
                    await self.start_ramp(self.target_node)
                elif cmd == 'status':
                    await self.read_status(self.target_node)
                elif cmd == 'read':
                    await self.read_sensor(self.target_node)
                elif cmd in ['m', 'monitor']:
                    await self.start_monitor(self.target_node)
                elif cmd.startswith('duty'):
                    parts = cmd.split(None, 1)
                    if len(parts) < 2:
                        print("  Usage: duty <0-100>")
                        continue
                    val = int(parts[1])
                    if val < 0 or val > 100:
                        print(f"  Note: duty clamped to {max(0, min(100, val))}%")
                    await self.set_duty(self.target_node, val)
                elif cmd.isdigit():
                    await self.set_duty(self.target_node, int(cmd))
                elif cmd.startswith('raw'):
                    parts = cmd.split(None, 1)
                    if len(parts) < 2:
                        print("  Usage: raw <command>")
                        continue
                    await self.send_command(parts[1].upper())
                elif cmd.startswith('threshold'):
                    parts = cmd.split(None, 1)
                    if len(parts) < 2:
                        print("  Usage: threshold <mW> or threshold off")
                        continue
                    arg = parts[1].strip()
                    if arg == 'off':
                        if self._power_manager:
                            await self._power_manager.disable()
                    else:
                        mw = float(arg)
                        if not self._power_manager:
                            self._power_manager = PowerManager(self)
                        self._power_manager.set_threshold(mw)
                        # Start poll loop as asyncio task (legacy mode)
                        asyncio.ensure_future(self._power_manager.poll_loop())
                elif cmd.startswith('priority'):
                    parts = cmd.split(None, 1)
                    if len(parts) < 2:
                        print("  Usage: priority <node_id> or priority off")
                        continue
                    arg = parts[1].strip()
                    if arg == 'off':
                        if self._power_manager:
                            self._power_manager.clear_priority()
                    elif self._power_manager:
                        if not (arg.isdigit() and 0 <= int(arg) <= 9):
                            print(f"  Warning: '{arg}' may not be a valid node ID (expected 0-9)")
                        self._power_manager.set_priority(arg)
                    else:
                        print("  Set a threshold first")
                elif cmd == 'power':
                    if self._power_manager:
                        print(self._power_manager.status())
                    else:
                        print("  Power management not active. Use: threshold <mW>")
                else:
                    print("  Unknown command. Type 'q' to quit.")

            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except (ValueError, IndexError):
                print("  Invalid value or missing argument")
            except Exception as e:
                print(f"  Error: {e}")

        if self._power_manager:
            await self._power_manager.disable()
        await self.disconnect()


# =============================================================================
# Textual TUI Application
# =============================================================================

if _HAS_TEXTUAL:

    class MeshGatewayApp(App):
        """Textual TUI for the BLE Mesh Gateway."""

        TITLE = "DC Monitor Mesh Gateway"

        CSS = """
        #sidebar {
            width: 26;
            dock: left;
            border-right: solid $accent;
            padding: 1;
            background: $surface;
        }
        #log {
            height: 1fr;
            border: solid $primary;
        }
        #nodes-table {
            height: auto;
            max-height: 10;
            border: solid $primary;
        }
        #cmd-input {
            dock: bottom;
        }
        """

        BINDINGS = [
            ("f2", "toggle_debug", "Debug"),
            ("f3", "clear_log", "Clear"),
            ("escape", "focus_input", "Input"),
        ]

        # ---- Custom Messages ----

        class SensorDataMsg(Message):
            """Sensor data arrived from a mesh node."""
            def __init__(self, node_id: str, duty: int, voltage: float,
                         current: float, power: float, raw: str):
                super().__init__()
                self.node_id = node_id
                self.duty = duty
                self.voltage = voltage
                self.current = current
                self.power = power
                self.raw = raw

        class LogMsg(Message):
            """Generic log line for the RichLog panel."""
            def __init__(self, text: str, style: str = ""):
                super().__init__()
                self.text = text
                self.style = style

        class PowerAdjustMsg(Message):
            """PowerManager made an adjustment."""
            def __init__(self, summary: str):
                super().__init__()
                self.summary = summary

        # ---- Init ----

        def __init__(self, gateway: DCMonitorGateway, target_address: str = None,
                     default_node: str = "0", scan_timeout: float = 10.0):
            super().__init__()
            self.gateway = gateway
            self.gateway.app = self  # Back-reference for callbacks
            self.gateway.target_node = default_node
            self.target_address = target_address
            self.scan_timeout = scan_timeout
            self.debug_mode = False
            self._connected = False
            self._ble_thread = BleThread()
            self.gateway.ble_thread = self._ble_thread

        # ---- Layout ----

        def compose(self) -> ComposeResult:
            yield Header()
            with Horizontal():
                yield Static("Connecting...", id="sidebar")
                yield RichLog(id="log", wrap=True, highlight=True, markup=True)
            yield DataTable(id="nodes-table")
            yield Input(placeholder="Enter command (type 'help' for list)", id="cmd-input")
            yield Footer()

        def on_mount(self) -> None:
            """Initialize table and start BLE connection."""
            table = self.query_one("#nodes-table", DataTable)
            table.add_columns("ID", "Duty", "Target", "Voltage", "Current", "Power", "Status")
            table.cursor_type = "none"
            # Focus the input
            self.query_one("#cmd-input", Input).focus()
            # Start BLE I/O thread before any BLE operations
            self._ble_thread.start()
            # Start BLE connection
            self.connect_ble()

        # ---- BLE Connection Worker ----

        @work(exclusive=True, group="ble_connect")
        async def connect_ble(self) -> None:
            """Scan and connect to BLE gateway via BLE thread."""
            gw = self.gateway
            bt = self._ble_thread

            devices = await bt.submit_async(
                gw.scan_for_nodes(timeout=self.scan_timeout, target_address=self.target_address)
            )

            if not devices:
                self.log_message("No gateways found. Restart to try again.", style="bold red")
                return

            # Select device
            if self.target_address:
                device = next(
                    (d for d in devices if d.address.upper() == self.target_address.upper()),
                    devices[0],
                )
            else:
                device = devices[0]

            # Connect (start_notify binds D-Bus handlers to BLE thread's loop)
            success = await bt.submit_async(gw.connect_to_node(device))
            if success:
                self._connected = True
                self.log_message(
                    f"Connected to {device.name or device.address}",
                    style="bold green")
                self.update_status()
            else:
                self.log_message("Connection failed", style="bold red")

        # ---- Command Handling ----

        @on(Input.Submitted, "#cmd-input")
        def on_cmd_submitted(self, event: Input.Submitted) -> None:
            """Handle command input."""
            cmd = event.value.strip()
            event.input.value = ""
            if cmd:
                self.log_message(f"> {cmd}", style="bold cyan")
                self.dispatch_command(cmd.lower())

        @work(exclusive=True, group="cmd")
        async def dispatch_command(self, cmd: str) -> None:
            """Parse and execute a user command via BLE thread."""
            gw = self.gateway
            bt = self._ble_thread
            try:
                if cmd in ['q', 'quit', 'exit']:
                    if gw._power_manager:
                        await bt.submit_async(gw._power_manager.disable())
                    await bt.submit_async(gw.disconnect())
                    bt.stop()
                    self.exit()

                elif cmd.startswith('node'):
                    parts = cmd.split(None, 1)
                    if len(parts) < 2:
                        self.log_message("Usage: node <0-9 or ALL>")
                        return
                    new_node = parts[1].strip().upper()
                    if new_node == 'ALL' or (new_node.isdigit() and 0 <= int(new_node) <= 9):
                        gw.target_node = new_node.lower() if new_node != 'ALL' else 'ALL'
                        self.log_message(f"Target node: {gw.target_node}")
                    else:
                        self.log_message("Invalid node ID (use 0-9 or ALL)")

                elif cmd in ['s', 'stop']:
                    was_monitoring = gw._monitoring
                    await bt.submit_async(gw.stop_node(gw.target_node))
                    if was_monitoring:
                        self.log_message("Monitoring stopped")

                elif cmd in ['r', 'ramp']:
                    await bt.submit_async(gw.start_ramp(gw.target_node))

                elif cmd == 'status':
                    await bt.submit_async(gw.read_status(gw.target_node))

                elif cmd == 'read':
                    await bt.submit_async(gw.read_sensor(gw.target_node))

                elif cmd in ['m', 'monitor']:
                    await bt.submit_async(gw.start_monitor(gw.target_node))

                elif cmd.startswith('duty'):
                    parts = cmd.split(None, 1)
                    if len(parts) < 2:
                        self.log_message("Usage: duty <0-100>")
                        return
                    val = int(parts[1])
                    if val < 0 or val > 100:
                        self.log_message(f"Note: duty clamped to {max(0, min(100, val))}%")
                    await bt.submit_async(gw.set_duty(gw.target_node, val))

                elif cmd.isdigit():
                    await bt.submit_async(gw.set_duty(gw.target_node, int(cmd)))

                elif cmd.startswith('raw'):
                    parts = cmd.split(None, 1)
                    if len(parts) < 2:
                        self.log_message("Usage: raw <command>")
                        return
                    await bt.submit_async(gw.send_command(parts[1].upper()))

                elif cmd.startswith('threshold'):
                    parts = cmd.split(None, 1)
                    if len(parts) < 2:
                        self.log_message("Usage: threshold <mW> or threshold off")
                        return
                    arg = parts[1].strip()
                    if arg == 'off':
                        if gw._power_manager:
                            await bt.submit_async(gw._power_manager.disable())
                            self.workers.cancel_group(self, "power_poll")
                            self.notify("Threshold disabled", severity="information")
                    else:
                        mw = float(arg)
                        if not gw._power_manager:
                            gw._power_manager = PowerManager(gw)
                        gw._power_manager.set_threshold(mw)
                        self.start_power_poll()
                        self.notify(f"Threshold: {mw:.0f} mW", severity="information")

                elif cmd.startswith('priority'):
                    parts = cmd.split(None, 1)
                    if len(parts) < 2:
                        self.log_message("Usage: priority <node_id> or priority off")
                        return
                    arg = parts[1].strip()
                    if arg == 'off':
                        if gw._power_manager:
                            gw._power_manager.clear_priority()
                            self.notify("Priority cleared", severity="information")
                    elif gw._power_manager:
                        if not (arg.isdigit() and 0 <= int(arg) <= 9):
                            self.log_message(f"Warning: '{arg}' may not be a valid node ID (expected 0-9)")
                        gw._power_manager.set_priority(arg)
                        self.notify(f"Priority: node {arg}", severity="information")
                    else:
                        self.log_message("Set a threshold first")

                elif cmd == 'power':
                    if gw._power_manager:
                        self.log_message(gw._power_manager.status())
                    else:
                        self.log_message("Power management not active. Use: threshold <mW>")

                elif cmd in ['d', 'debug']:
                    self.action_toggle_debug()

                elif cmd in ['clear', 'cls']:
                    self.action_clear_log()

                elif cmd == 'help':
                    self._show_help()

                else:
                    self.log_message("Unknown command. Type 'help' for list.")

            except (ValueError, IndexError):
                self.log_message("Invalid value or missing argument")
            except Exception as e:
                self.log_message(f"Error: {e}", style="bold red")

            self.update_status()

        # ---- Power Poll Worker ----

        @work(exclusive=True, group="power_poll")
        async def start_power_poll(self) -> None:
            """Run PowerManager poll loop on the BLE thread."""
            pm = self.gateway._power_manager
            if pm:
                future = self._ble_thread.submit(pm.poll_loop())
                try:
                    await asyncio.wrap_future(future)
                except asyncio.CancelledError:
                    future.cancel()
                    raise

        # ---- Message Handlers ----
        # Textual auto-discovers handlers named on_<namespace>_<message_name>
        # where namespace = snake_case of outermost widget class.

        def on_mesh_gateway_app_sensor_data_msg(self, msg: SensorDataMsg) -> None:
            """Handle incoming sensor data — update table and optionally log."""
            self._update_node_table(msg)
            self.update_status()

            # Show in log unless it's a background PM poll
            pm = self.gateway._power_manager
            is_bg_poll = pm and pm._polling and pm.threshold_mw is not None
            if not is_bg_poll or self.debug_mode:
                log = self.query_one("#log", RichLog)
                log.write(msg.raw)

        def on_mesh_gateway_app_log_msg(self, msg: LogMsg) -> None:
            """Handle generic log messages."""
            log = self.query_one("#log", RichLog)
            if msg.style:
                log.write(f"[{msg.style}]{msg.text}[/{msg.style}]")
            else:
                log.write(msg.text)

        def on_mesh_gateway_app_power_adjust_msg(self, msg: PowerAdjustMsg) -> None:
            """Handle power adjustment notification."""
            self.update_status()

        # ---- UI Updates ----

        def _update_node_table(self, msg: SensorDataMsg) -> None:
            """Update or insert a row in the nodes DataTable."""
            table = self.query_one("#nodes-table", DataTable)
            pm = self.gateway._power_manager
            row_key = f"node_{msg.node_id}"

            # Get target duty
            if pm and msg.node_id in pm.nodes:
                target = pm.nodes[msg.node_id].target_duty
            else:
                target = msg.duty

            # Get responsive status
            if pm and msg.node_id in pm.nodes:
                status_icon = "ok" if pm.nodes[msg.node_id].responsive else "STALE"
            else:
                status_icon = "ok"

            row_data = [
                msg.node_id,
                f"{msg.duty}%",
                f"{target}%",
                f"{msg.voltage:.2f}V",
                f"{msg.current:.1f}mA",
                f"{msg.power:.0f}mW",
                status_icon,
            ]

            # Try to update existing row, add if not found
            if row_key in table.rows:
                col_keys = list(table.columns.keys())
                for col_idx, val in enumerate(row_data):
                    table.update_cell(row_key, col_keys[col_idx], val)
            else:
                table.add_row(*row_data, key=row_key)

        def update_status(self) -> None:
            """Refresh the sidebar with current state."""
            gw = self.gateway
            pm = gw._power_manager
            lines = ["[bold]Status[/bold]", ""]

            # Connection
            if self._connected:
                name = gw.connected_device.name if gw.connected_device else "?"
                lines.append(f"[green]Connected[/green]")
                lines.append(f"{name}")
            else:
                lines.append("[yellow]Connecting...[/yellow]")

            lines.append(f"\nTarget: [bold]{gw.target_node}[/bold]")

            # Monitoring
            if gw._monitoring:
                lines.append("[cyan]Monitoring ●[/cyan]")

            # Power management
            lines.append("")
            if pm and pm.threshold_mw is not None:
                budget = pm.threshold_mw - pm.HEADROOM_MW
                lines.append("[bold]Power Mgmt[/bold]")
                lines.append(f"Threshold: {pm.threshold_mw:.0f}mW")
                lines.append(f"Budget:    {budget:.0f}mW")
                if pm.priority_node:
                    lines.append(f"Priority:  N{pm.priority_node}")
                else:
                    lines.append("Priority:  none")

                total = sum(ns.power for ns in pm.nodes.values() if ns.responsive)
                headroom = pm.threshold_mw - total
                lines.append(f"\nTotal: {total:.0f}mW")
                if headroom >= pm.HEADROOM_MW:
                    lines.append(f"Headroom: [green]{headroom:.0f}mW[/green]")
                elif headroom >= 0:
                    lines.append(f"Headroom: [yellow]{headroom:.0f}mW[/yellow]")
                else:
                    lines.append(f"Headroom: [red]{headroom:.0f}mW[/red]")

                # Node count
                responsive = sum(1 for ns in pm.nodes.values() if ns.responsive)
                total_nodes = len(pm.nodes)
                lines.append(f"Nodes: {responsive}/{total_nodes}")
            else:
                lines.append("[dim]Power: OFF[/dim]")

            # Debug mode
            if self.debug_mode:
                lines.append("\n[yellow]DEBUG ON[/yellow]")

            try:
                self.query_one("#sidebar", Static).update("\n".join(lines))
            except Exception:
                pass

        def _show_help(self):
            """Display help text in the log."""
            help_text = (
                "[bold]--- Commands ---[/bold]\n"
                "  node <id>      Switch target (0-9 or ALL)\n"
                "  ramp / r       Send RAMP to target node\n"
                "  stop / s       Send STOP to target node\n"
                "  duty <0-100>   Set duty cycle on target node\n"
                "  status         Get status from target node\n"
                "  read           Single sensor reading\n"
                "  monitor / m    Start continuous monitoring\n"
                "  raw <cmd>      Send raw command string\n"
                "\n"
                "[bold]--- Power Management ---[/bold]\n"
                "  threshold <mW> Set total power limit\n"
                "  priority <id>  Set priority node\n"
                "  threshold off  Disable power management\n"
                "  priority off   Clear priority node\n"
                "  power          Show power manager status\n"
                "\n"
                "[bold]--- Keys / Misc ---[/bold]\n"
                "  debug / d      Toggle debug mode (or F2)\n"
                "  clear / cls    Clear log (or F3)\n"
                "  Esc            Focus input\n"
                "  q / quit       Quit"
            )
            log = self.query_one("#log", RichLog)
            log.write(help_text)

        # ---- Actions ----

        def action_toggle_debug(self) -> None:
            """Toggle debug mode."""
            self.debug_mode = not self.debug_mode
            self.notify(f"Debug: {'ON' if self.debug_mode else 'OFF'}")
            self.update_status()

        def action_clear_log(self) -> None:
            """Clear the log panel."""
            self.query_one("#log", RichLog).clear()

        def action_focus_input(self) -> None:
            """Focus the command input."""
            self.query_one("#cmd-input", Input).focus()

        def log_message(self, text: str, style: str = ""):
            """Convenience: post a LogMsg."""
            self.post_message(self.LogMsg(text, style))

        def on_unmount(self) -> None:
            """Clean up BLE thread when app exits."""
            if self._ble_thread:
                try:
                    if self.gateway.client and self.gateway.client.is_connected:
                        f = self._ble_thread.submit(self.gateway.disconnect())
                        f.result(timeout=3.0)
                except Exception:
                    pass
                self._ble_thread.stop()


# =============================================================================
# Main entry point
# =============================================================================

def main():
    """Entry point — decides between TUI and CLI mode."""
    parser = argparse.ArgumentParser(description="BLE Gateway for DC Monitor Mesh")
    parser.add_argument("--scan", action="store_true", help="Scan for gateways only")
    parser.add_argument("--address", type=str, help="Connect to specific MAC address")
    parser.add_argument("--node", type=str, default="0",
                        help="Target mesh node ID (0-9 or ALL, default: 0)")
    parser.add_argument("--duty", type=int, help="Set duty cycle (0-100%%)")
    parser.add_argument("--ramp", action="store_true", help="Run ramp test")
    parser.add_argument("--stop", action="store_true", help="Stop load")
    parser.add_argument("--status", action="store_true", help="Get node status")
    parser.add_argument("--read", action="store_true", help="Single sensor reading")
    parser.add_argument("--monitor", action="store_true", help="Start continuous monitoring")
    parser.add_argument("--timeout", type=float, default=10.0, help="Scan timeout")
    parser.add_argument("--no-tui", action="store_true",
                        help="Use plain CLI mode instead of TUI")
    args = parser.parse_args()

    # Validate --node argument
    if args.node.upper() != "ALL" and not (args.node.isdigit() and 0 <= int(args.node) <= 9):
        parser.error(f"Invalid node ID '{args.node}': use 0-9 or ALL")

    node = args.node.upper() if args.node.upper() == "ALL" else args.node
    is_oneshot = args.scan or args.stop or args.ramp or args.status or args.read \
        or args.monitor or args.duty is not None

    # If TUI available and not one-shot and not --no-tui, launch TUI
    # Textual's app.run() manages its own event loop, so call it directly (not from asyncio.run)
    if _HAS_TEXTUAL and not is_oneshot and not args.no_tui:
        gateway = DCMonitorGateway()
        app = MeshGatewayApp(
            gateway,
            target_address=args.address,
            default_node=node,
            scan_timeout=args.timeout,
        )
        app.run()
        return

    # Otherwise: one-shot or legacy CLI mode (needs asyncio.run)
    asyncio.run(_run_cli(args, node))


async def _run_cli(args, node: str):
    """Run one-shot CLI commands or legacy interactive mode."""
    gateway = DCMonitorGateway()

    print("\n" + "=" * 50)
    print("  DC Monitor Mesh Gateway (Pi 5)")
    print("=" * 50)

    devices = await gateway.scan_for_nodes(
        timeout=args.timeout, target_address=args.address
    )

    if args.scan:
        print(f"\nFound {len(devices)} gateway(s)")
        return

    if not devices:
        return

    # Select device
    if args.address:
        device = next(
            (d for d in devices if d.address.upper() == args.address.upper()),
            devices[0],
        )
    else:
        device = devices[0]

    # Connect
    if not await gateway.connect_to_node(device):
        return

    # Handle one-shot CLI commands
    if args.stop:
        await gateway.stop_node(node)
        await asyncio.sleep(1)
    elif args.duty is not None:
        await gateway.set_duty(node, args.duty)
        await asyncio.sleep(2)
    elif args.ramp:
        await gateway.start_ramp(node)
        await asyncio.sleep(2)
    elif args.status:
        await gateway.read_status(node)
        await asyncio.sleep(2)
    elif args.read:
        await gateway.read_sensor(node)
        await asyncio.sleep(2)
    elif args.monitor:
        await gateway.start_monitor(node)
        print("Monitoring... press Ctrl+C to stop")
        try:
            while gateway.client and gateway.client.is_connected:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            pass
    else:
        # Legacy interactive mode (--no-tui)
        await gateway.interactive_mode(default_node=node)

    await gateway.disconnect()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nGoodbye!")
