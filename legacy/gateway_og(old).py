#!/usr/bin/env python3
"""
BLE Gateway for DC Monitor Mesh Network
Connects to ESP32-C6 nodes and receives sensor data via BLE
Sends commands to control Pico (duty, ramp, monitor, stop)

Usage:
    python gateway.py              # Connect and monitor
    python gateway.py --scan       # Just scan for nodes
    python gateway.py --duty 50    # Set duty cycle to 50%
    python gateway.py --ramp       # Run ramp test
    python gateway.py --stop       # Stop load
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

# Device name prefix to look for
DEVICE_NAME_PREFIX = "DC-Monitor"


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
            if device.name and DEVICE_NAME_PREFIX in device.name:
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
        """Send command to node (forwards to Pico via UART)"""
        if not self.client or not self.client.is_connected:
            print("Not connected")
            return False
            
        try:
            await self.client.write_gatt_char(COMMAND_CHAR_UUID, cmd.encode('utf-8'))
            print(f"✓ Sent: {cmd}")
            return True
        except Exception as e:
            print(f"✗ Failed to send command: {e}")
            return False
    
    async def set_duty(self, percent: int):
        """Set duty cycle (0-100%)"""
        percent = max(0, min(100, percent))
        return await self.send_command(f"DUTY:{percent}")
    
    async def start_ramp(self):
        """Start ramp test"""
        return await self.send_command("RAMP")
    
    async def start_monitor(self):
        """Start continuous monitoring"""
        return await self.send_command("MONITOR")
    
    async def stop(self):
        """Stop load"""
        return await self.send_command("STOP")
    
    async def read_status(self):
        """Request current reading"""
        return await self.send_command("READ")
            
    async def interactive_mode(self):
        """Interactive command mode"""
        print("\n" + "=" * 50)
        print("  Interactive Mode")
        print("=" * 50)
        print("Commands:")
        print("  0-100    Set duty cycle")
        print("  r/ramp   Run ramp test")
        print("  m/mon    Start monitoring")
        print("  s/stop   Stop load")
        print("  read     Get current reading")
        print("  q/quit   Exit")
        print("=" * 50)
        print()
        
        while self.running and self.client.is_connected:
            try:
                # Use asyncio-friendly input
                cmd = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: input("> ").strip().lower()
                )
                
                if cmd in ['q', 'quit', 'exit']:
                    await self.stop()
                    break
                elif cmd in ['s', 'stop']:
                    await self.stop()
                elif cmd in ['r', 'ramp']:
                    await self.start_ramp()
                elif cmd in ['m', 'mon', 'monitor']:
                    await self.start_monitor()
                elif cmd == 'read':
                    await self.read_status()
                elif cmd.isdigit():
                    await self.set_duty(int(cmd))
                elif cmd.startswith('duty:') or cmd.startswith('d:'):
                    val = int(cmd.split(':')[1])
                    await self.set_duty(val)
                elif cmd:
                    # Send raw command
                    await self.send_command(cmd.upper())
                    
            except KeyboardInterrupt:
                print("\n\nStopping...")
                await self.stop()
                break
            except Exception as e:
                print(f"Error: {e}")
        
        await self.disconnect()


async def main():
    parser = argparse.ArgumentParser(description="BLE Gateway for DC Monitor Mesh")
    parser.add_argument("--scan", action="store_true", help="Scan for nodes only")
    parser.add_argument("--address", type=str, help="Connect to specific MAC address")
    parser.add_argument("--duty", type=int, help="Set duty cycle (0-100%%)")
    parser.add_argument("--ramp", action="store_true", help="Run ramp test")
    parser.add_argument("--monitor", action="store_true", help="Start monitoring")
    parser.add_argument("--stop", action="store_true", help="Stop load")
    parser.add_argument("--timeout", type=float, default=10.0, help="Scan timeout")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive mode")
    args = parser.parse_args()
    
    gateway = DCMonitorGateway()
    
    print("\n" + "=" * 50)
    print("  DC Monitor BLE Gateway (Pi 5)")
    print("=" * 50)
    
    nodes = await gateway.scan_for_nodes(timeout=args.timeout)
    
    if args.scan:
        print(f"\nFound {len(nodes)} node(s)")
        return
        
    if not nodes:
        return
        
    # Select device
    if args.address:
        device = next((d for d in nodes if d.address == args.address), None)
        if not device:
            print(f"Device {args.address} not found")
            return
    else:
        device = nodes[0]
        
    # Connect
    if not await gateway.connect_to_node(device):
        return
    
    # Handle command-line commands
    if args.stop:
        await gateway.stop()
        await asyncio.sleep(1)
    elif args.duty is not None:
        await gateway.set_duty(args.duty)
        await asyncio.sleep(2)  # Wait for response
    elif args.ramp:
        await gateway.start_ramp()
        await asyncio.sleep(10)  # Wait for ramp to complete
    elif args.monitor:
        await gateway.start_monitor()
        await gateway.interactive_mode()
    elif args.interactive:
        await gateway.interactive_mode()
    else:
        # Default: interactive mode
        await gateway.interactive_mode()
    
    await gateway.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGoodbye!")
