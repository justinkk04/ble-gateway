import asyncio
from bleak import BleakScanner

async def main():
    print("Scanning 10 seconds...")
    devices = await BleakScanner.discover(timeout=8.0)

    if not devices:
        print("No BLE advertisements seen.")
        return

    for d in devices:
        print("----")
        print("address:", getattr(d, "address", None))
        print("name   :", getattr(d, "name", None))
        print("details:", getattr(d, "details", None))
        print("metadata:", getattr(d, "metadata", None))

asyncio.run(main())