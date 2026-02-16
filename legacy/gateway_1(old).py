#!/usr/bin/env python3
"""
BLE Gateway for DC Monitor Mesh Network
Connects to ESP32-C6 GATT gateway and sends commands to mesh nodes

Usage:
    python gateway.py                    # Interactive mode
    python gateway.py --scan             # Just scan for gateways
    python gateway.py --node 0 --ramp    # Send RAMP to node 0
    python gateway.py --node 1 --duty 50 # Set duty 50% on node 1
    python gateway.py --node all --stop  # Stop all nodes

Commands are sent as NODE_ID:COMMAND[:VALUE] to the ESP32-C6 mesh gateway,
which forwards them to the targeted mesh node via BLE Mesh.
"""

import asyncio
import argparse
import sys
from datetime import datetime

try:
    from bleak import BleakClient, BleakScanner
    from bleak.backends.characteristic import BleakGATTCharacteristic
except ImportError:
    print("ERROR: bleak not installed. Run: pip install bleak")
    sys.exit(1)

# Custom UUIDs matching ESP32-C6 ble_service.h
DC_MONITOR_SERVICE_UUID = "0000dc01-0000-1000-8000-00805f9b34fb"
SENSOR_DATA_CHAR_UUID = "0000dc02-0000-1000-8000-00805f9b34fb"
COMMAND_CHAR_UUID = "0000dc03-0000-1000-8000-00805f9b34fb"

# Device name prefix to look for (must match ESP_GATT_BLE_Gateway advertisement)
DEVICE_NAME_PATTERNS = ["Mesh-Gateway", "ESP-BLE-MESH"]


class DCMonitorGateway:
    def __init__(self):
        self.client = None
        self.connected_device = None
        self.running = True
        
    async def scan_for_nodes(self, timeout=10.0):
        """Scan for DC Monitor nodes"""
        print(f"\n🔍 Scanning for BLE devices ({timeout}s)...")
        
        devices = await BleakScanner.discover(timeout=timeout, return_adv=True)
        
        nodes = []
        for address, (device, adv_data) in devices.items():
            if device.name and any(p in device.name for p in DEVICE_NAME_PATTERNS):
                nodes.append(device)
                print(f"  ✓ Found: {device.name} [{device.address}]")
            elif adv_data.service_uuids:
                if DC_MONITOR_SERVICE_UUID.lower() in [str(u).lower() for u in adv_data.service_uuids]:
                    nodes.append(device)
                    print(f"  ✓ Found: {device.name or 'Unknown'} [{device.address}] (by service UUID)")
                
        if not nodes:
            print("  ✗ No DC Monitor nodes found")
            print("\n  Tip: Make sure ESP32-C6 is powered and advertising")
            print("  All devices found:")
            for address, (device, adv_data) in devices.items():
                name = device.name or "(no name)"
                print(f"    - {name} [{address}]")
            
        return nodes
    
    def notification_handler(self, characteristic: BleakGATTCharacteristic, data: bytearray):
        """Handle incoming sensor data notifications"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        decoded = data.decode('utf-8', errors='replace').strip()
        
        # Color-code different message types
        if decoded.startswith("STATUS:"):
            print(f"[{timestamp}] � {decoded}")
        else:
            print(f"[{timestamp}] �📊 {decoded}")
        
    async def connect_to_node(self, device):
        """Connect to a specific node and subscribe to notifications"""
        print(f"\n🔗 Connecting to {device.name or device.address}...")
        
        self.client = BleakClient(device.address)
        await self.client.connect()
        
        if not self.client.is_connected:
            print("  ✗ Connection failed")
            return False
            
        self.connected_device = device
        print(f"  ✓ Connected!")
        
        try:
            await self.client.start_notify(SENSOR_DATA_CHAR_UUID, self.notification_handler)
            print(f"  ✓ Subscribed to sensor notifications")
        except Exception as e:
            print(f"  ⚠ Could not subscribe: {e}")
            
        return True
    
    async def disconnect(self):
        """Disconnect from current node"""
        if self.client and self.client.is_connected:
            await self.client.disconnect()
            print("Disconnected")
            
    async def send_command(self, cmd: str):
        """Send raw command string to GATT gateway"""
        if not self.client or not self.client.is_connected:
            print("Not connected")
            return False

        try:
            await self.client.write_gatt_char(COMMAND_CHAR_UUID, cmd.encode('utf-8'))
            print(f"  Sent: {cmd}")
            return True
        except Exception as e:
            print(f"  Failed to send command: {e}")
            return False

    async def send_to_node(self, node: str, command: str, value: str = None):
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
        return await self.send_command(cmd)

    async def set_duty(self, node: str, percent: int):
        """Set duty cycle (0-100%) on a mesh node"""
        percent = max(0, min(100, percent))
        return await self.send_to_node(node, "DUTY", str(percent))

    async def start_ramp(self, node: str):
        """Start ramp test on a mesh node"""
        return await self.send_to_node(node, "RAMP")

    async def stop_node(self, node: str):
        """Stop load on a mesh node"""
        return await self.send_to_node(node, "STOP")

    async def read_status(self, node: str):
        """Request current status from a mesh node"""
        return await self.send_to_node(node, "STATUS")
            
    async def interactive_mode(self, default_node: str = "0"):
        """Interactive command mode with mesh node targeting"""
        self.target_node = default_node

        print("\n" + "=" * 50)
        print("  Mesh Gateway - Interactive Mode")
        print("=" * 50)
        print(f"  Target node: {self.target_node}")
        print()
        print("Commands:")
        print("  node <id>    Switch target (0-9 or ALL)")
        print("  ramp         Send RAMP to target node")
        print("  stop         Send STOP to target node")
        print("  duty <0-100> Set duty cycle on target node")
        print("  status       Get status from target node")
        print("  raw <cmd>    Send raw command string")
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
                elif cmd in ['status', 'read']:
                    await self.read_status(self.target_node)
                elif cmd.startswith('duty '):
                    val = int(cmd.split(None, 1)[1])
                    await self.set_duty(self.target_node, val)
                elif cmd.isdigit():
                    await self.set_duty(self.target_node, int(cmd))
                elif cmd.startswith('raw '):
                    await self.send_command(cmd.split(None, 1)[1].upper())
                else:
                    print("  Unknown command. Type 'q' to quit.")

            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except ValueError:
                print("  Invalid value")
            except Exception as e:
                print(f"  Error: {e}")

        await self.disconnect()


async def main():
    parser = argparse.ArgumentParser(description="BLE Gateway for DC Monitor Mesh")
    parser.add_argument("--scan", action="store_true", help="Scan for gateways only")
    parser.add_argument("--address", type=str, help="Connect to specific MAC address")
    parser.add_argument("--node", type=str, default="0",
                        help="Target mesh node ID (0-9 or ALL, default: 0)")
    parser.add_argument("--duty", type=int, help="Set duty cycle (0-100%%)")
    parser.add_argument("--ramp", action="store_true", help="Run ramp test")
    parser.add_argument("--stop", action="store_true", help="Stop load")
    parser.add_argument("--status", action="store_true", help="Get node status")
    parser.add_argument("--timeout", type=float, default=10.0, help="Scan timeout")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive mode")
    args = parser.parse_args()

    node = args.node.upper() if args.node.upper() == "ALL" else args.node

    gateway = DCMonitorGateway()

    print("\n" + "=" * 50)
    print("  DC Monitor Mesh Gateway (Pi 5)")
    print("=" * 50)

    devices = await gateway.scan_for_nodes(timeout=args.timeout)

    if args.scan:
        print(f"\nFound {len(devices)} gateway(s)")
        return

    if not devices:
        return

    # Select device
    if args.address:
        device = next((d for d in devices if d.address == args.address), None)
        if not device:
            print(f"Device {args.address} not found")
            return
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
    else:
        # Interactive mode (default)
        await gateway.interactive_mode(default_node=node)

    await gateway.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGoodbye!")
