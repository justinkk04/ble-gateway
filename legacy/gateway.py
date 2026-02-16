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
except Exception as e:
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
    duty: int = 0              # Current (possibly throttled) duty %
    target_duty: int = 0       # User-requested duty % (restored when threshold off)
    voltage: float = 0.0       # V
    current: float = 0.0       # mA
    power: float = 0.0         # mW
    last_seen: datetime = field(default_factory=datetime.now)
    responsive: bool = True
    poll_gen: int = 0          # Which poll cycle this data is from


class PowerManager:
    """Enforces a total power threshold across all mesh nodes.

    When total measured power exceeds the threshold:
      - No priority: all nodes reduce duty proportionally
      - With priority: priority node keeps duty, others reduce more
      - If priority node alone exceeds threshold, it also reduces
    """

    POLL_INTERVAL = 2.0
    READ_STAGGER = 0.5
    STALE_TIMEOUT = 10.0
    DAMPING = 0.9
    COOLDOWN = 3.0

    def __init__(self, gateway):
        self.gateway = gateway
        self.nodes: dict[str, NodeState] = {}
        self.threshold_mw: Optional[float] = None
        self.priority_node: Optional[str] = None
        self._adjusting = False
        self._last_adjustment: float = 0
        self._poll_generation: int = 0
        self._polling = False  # True while a poll cycle is active

    # ---- Public API ----

    def set_threshold(self, mw: float):
        """Enable power management with the given threshold in mW."""
        self.threshold_mw = mw
        self._needs_bootstrap = not self.nodes
        self.gateway.log(f"[POWER] Threshold set: {mw:.0f} mW")

    async def disable(self):
        """Disable power management and restore original duty cycles."""
        self.threshold_mw = None
        self._polling = False
        # Restore all nodes to their target duty
        for ns in self.nodes.values():
            if ns.responsive and ns.duty != ns.target_duty:
                self.gateway.log(
                    f"[POWER] Restoring node {ns.node_id}: {ns.duty}% -> {ns.target_duty}%")
                await self.gateway.set_duty(
                    ns.node_id, ns.target_duty, _from_power_mgr=True, _silent=True)
                await asyncio.sleep(0.5)
        self.gateway.log("[POWER] Threshold disabled")

    def set_priority(self, node_id: str):
        """Set the priority node."""
        self.priority_node = node_id
        self.gateway.log(f"[POWER] Priority node: {node_id}")

    def clear_priority(self):
        """Remove priority designation."""
        self.priority_node = None
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
            lines.append(f"Threshold: {self.threshold_mw:.0f} mW")
        else:
            lines.append("Threshold: OFF")
        if self.priority_node is not None:
            lines.append(f"Priority:  node {self.priority_node}")
        else:
            lines.append("Priority:  none")

        total = 0.0
        if self.nodes:
            lines.append("Nodes:")
            for nid in sorted(self.nodes.keys()):
                ns = self.nodes[nid]
                st = "ok" if ns.responsive else "stale"
                target = f" (target:{ns.target_duty}%)" if ns.target_duty != ns.duty else ""
                lines.append(
                    f"  Node {nid}: D:{ns.duty}%{target} "
                    f"V:{ns.voltage:.2f}V I:{ns.current:.1f}mA "
                    f"P:{ns.power:.0f}mW [{st}]"
                )
                if ns.responsive:
                    total += ns.power
            lines.append(f"Total power: {total:.0f} mW")
            if self.threshold_mw is not None:
                remaining = self.threshold_mw - total
                lines.append(f"Headroom:    {remaining:.0f} mW")
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
        ns.last_seen = datetime.now()
        ns.responsive = True
        ns.poll_gen = self._poll_generation

        # Only auto-sync target_duty when power management is OFF
        # (handles RAMP and other Pico-side duty changes)
        if self.threshold_mw is None:
            ns.target_duty = duty

    # ---- Internal Control Loop ----

    async def _bootstrap_discovery(self):
        """Send ALL:READ to discover mesh nodes."""
        self.gateway.log("[POWER] Discovering mesh nodes...")
        await self.gateway.send_to_node("ALL", "READ", _silent=True)

    async def poll_loop(self):
        """Periodic poll-and-adjust cycle. Called by TUI @work or asyncio task."""
        try:
            if getattr(self, '_needs_bootstrap', False):
                self._needs_bootstrap = False
                await self._bootstrap_discovery()
                await asyncio.sleep(2.0)
            while self.threshold_mw is not None:
                self._polling = True
                await self._poll_all_nodes()
                await self._wait_for_responses(timeout=3.0)
                self._mark_stale_nodes()
                await self._evaluate_and_adjust()
                self._polling = False
                await asyncio.sleep(self.POLL_INTERVAL)
        except asyncio.CancelledError:
            self._polling = False

    async def _poll_all_nodes(self):
        """Send READ to every known node, staggered."""
        self._poll_generation += 1
        node_ids = list(self.nodes.keys())
        if not node_ids:
            return
        for node_id in node_ids:
            if self.threshold_mw is None:
                return
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
        now = datetime.now()
        for ns in self.nodes.values():
            age = (now - ns.last_seen).total_seconds()
            if age > self.STALE_TIMEOUT:
                if ns.responsive:
                    self.gateway.log(
                        f"[POWER] Node {ns.node_id} unresponsive ({age:.0f}s)")
                ns.responsive = False

    async def _evaluate_and_adjust(self):
        """Check total power and adjust if over threshold."""
        if self.threshold_mw is None or self._adjusting:
            return

        if time.monotonic() - self._last_adjustment < self.COOLDOWN:
            return

        responsive = {nid: ns for nid, ns in self.nodes.items() if ns.responsive}
        if not responsive:
            return

        total_power = sum(ns.power for ns in responsive.values())
        if total_power <= self.threshold_mw:
            return

        self._adjusting = True
        try:
            if self.priority_node and self.priority_node in responsive:
                await self._reduce_with_priority(responsive, total_power)
            else:
                await self._reduce_proportional(responsive, total_power)

            self._last_adjustment = time.monotonic()
        finally:
            self._adjusting = False

    async def _reduce_proportional(self, nodes: dict, total_power: float):
        """All nodes reduce proportionally."""
        target_ratio = (self.threshold_mw / total_power) * self.DAMPING
        changes = []
        for nid, ns in nodes.items():
            new_duty = max(0, int(ns.duty * target_ratio))
            if new_duty != ns.duty:
                changes.append(f"N{nid}:{ns.duty}->{new_duty}%")
                await self.gateway.set_duty(
                    nid, new_duty, _from_power_mgr=True, _silent=True)
                await asyncio.sleep(0.5)
        if changes:
            self.gateway.log(
                f"[POWER] Over {total_power:.0f}/{self.threshold_mw:.0f}mW "
                f"- Reduced: {', '.join(changes)}")

    async def _reduce_with_priority(self, nodes: dict, total_power: float):
        """Priority node keeps duty; others absorb the reduction.
        If priority alone exceeds threshold, it also reduces."""
        priority_ns = nodes[self.priority_node]
        non_priority = {nid: ns for nid, ns in nodes.items()
                        if nid != self.priority_node}

        if priority_ns.power >= self.threshold_mw:
            # Priority node alone exceeds threshold - reduce everything
            changes = []
            for nid, ns in non_priority.items():
                if ns.duty > 0:
                    changes.append(f"N{nid}:{ns.duty}->0%")
                    await self.gateway.set_duty(
                        nid, 0, _from_power_mgr=True, _silent=True)
                    await asyncio.sleep(0.5)
            # Also reduce priority node
            ratio = (self.threshold_mw / priority_ns.power) * self.DAMPING
            new_duty = max(0, int(priority_ns.duty * ratio))
            changes.append(f"N{self.priority_node}:{priority_ns.duty}->{new_duty}%(pri)")
            await self.gateway.set_duty(
                self.priority_node, new_duty, _from_power_mgr=True, _silent=True)
            self.gateway.log(
                f"[POWER] Priority alone over threshold - Reduced: {', '.join(changes)}")
            return

        # Normal case: non-priority nodes absorb the excess
        non_priority_power = sum(ns.power for ns in non_priority.values())
        if non_priority_power <= 0:
            return

        allowed = self.threshold_mw - priority_ns.power
        target_ratio = (allowed / non_priority_power) * self.DAMPING
        target_ratio = max(0.0, min(1.0, target_ratio))

        changes = []
        for nid, ns in non_priority.items():
            new_duty = max(0, int(ns.duty * target_ratio))
            if new_duty != ns.duty:
                changes.append(f"N{nid}:{ns.duty}->{new_duty}%")
                await self.gateway.set_duty(
                    nid, new_duty, _from_power_mgr=True, _silent=True)
                await asyncio.sleep(0.5)
        if changes:
            self.gateway.log(
                f"[POWER] Over {total_power:.0f}/{self.threshold_mw:.0f}mW "
                f"(pri N{self.priority_node} kept) - Reduced: {', '.join(changes)}")


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
            # Only show errors if not during a background poll
            pm = self._power_manager
            if pm and pm._polling:
                if self.app and _HAS_TEXTUAL and self.app.debug_mode:
                    self.log(f"[{timestamp}] !! {decoded}", style="dim", _from_thread=True)
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
                if self.app and _HAS_TEXTUAL and self.app.debug_mode:
                    self.log(f"[{timestamp}] !! {decoded}", style="dim", _from_thread=True)
            else:
                self.log(f"[{timestamp}] !! {decoded}", style="yellow", _from_thread=True)
        else:
            self.log(f"[{timestamp}] {decoded}", _from_thread=True)

    async def connect_to_node(self, device):
        """Connect to a specific node and subscribe to notifications"""
        self.log(f"Connecting to {device.name or device.address}...")

        self.client = BleakClient(device.address)
        await self.client.connect()

        if not self.client.is_connected:
            self.log("Connection failed")
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

        Args:
            node: Node ID (0-9) or "ALL"
            command: RAMP, STOP, ON, OFF, DUTY, STATUS, READ
            value: Optional value (e.g. duty percentage)
        """
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
                elif cmd.startswith('node '):
                    new_node = cmd.split(None, 1)[1].strip().upper()
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
                elif cmd.startswith('duty '):
                    val = int(cmd.split(None, 1)[1])
                    await self.set_duty(self.target_node, val)
                elif cmd.isdigit():
                    await self.set_duty(self.target_node, int(cmd))
                elif cmd.startswith('raw '):
                    await self.send_command(cmd.split(None, 1)[1].upper())
                elif cmd.startswith('threshold '):
                    arg = cmd.split(None, 1)[1].strip()
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
                elif cmd.startswith('priority '):
                    arg = cmd.split(None, 1)[1].strip()
                    if arg == 'off':
                        if self._power_manager:
                            self._power_manager.clear_priority()
                    elif self._power_manager:
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
            except ValueError:
                print("  Invalid value")
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

        @work(exclusive=False, group="cmd")
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

                elif cmd.startswith('node '):
                    new_node = cmd.split(None, 1)[1].strip().upper()
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

                elif cmd.startswith('duty '):
                    val = int(cmd.split(None, 1)[1])
                    await bt.submit_async(gw.set_duty(gw.target_node, val))

                elif cmd.isdigit():
                    await bt.submit_async(gw.set_duty(gw.target_node, int(cmd)))

                elif cmd.startswith('raw '):
                    await bt.submit_async(gw.send_command(cmd.split(None, 1)[1].upper()))

                elif cmd.startswith('threshold '):
                    arg = cmd.split(None, 1)[1].strip()
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

                elif cmd.startswith('priority '):
                    arg = cmd.split(None, 1)[1].strip()
                    if arg == 'off':
                        if gw._power_manager:
                            gw._power_manager.clear_priority()
                            self.notify("Priority cleared", severity="information")
                    elif gw._power_manager:
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

            except ValueError:
                self.log_message("Invalid value")
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
            try:
                for col_idx, val in enumerate(row_data):
                    table.update_cell(row_key, table.columns[col_idx].key, val)
            except Exception:
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
                lines.append("[bold]Power Mgmt[/bold]")
                lines.append(f"Threshold: {pm.threshold_mw:.0f}mW")
                if pm.priority_node:
                    lines.append(f"Priority:  N{pm.priority_node}")
                else:
                    lines.append("Priority:  none")

                total = sum(ns.power for ns in pm.nodes.values() if ns.responsive)
                headroom = pm.threshold_mw - total
                lines.append(f"\nTotal: {total:.0f}mW")
                if headroom >= 0:
                    lines.append(f"Headroom: [green]{headroom:.0f}mW[/green]")
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
