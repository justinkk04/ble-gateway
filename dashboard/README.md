# BLE Mesh Dashboard (v0.6.2)

A standalone web dashboard for visualizing the DC Monitor Mesh Network topology and real-time sensor data.

## Features

- **Force D3.js Graph**: Visualizes network topology (Gateway ↔ Nodes ↔ Relays).
- **Real-time Status**: Shows duty cycle, voltage, current, and power for each node.
- **Power Manager Monitor**: Tracks total power usage against the set threshold.
- **Responsive Design**: Works on desktop and mobile.
- **Dark Theme**: Optimized for low-light environments.

## Setup

### Prerequisites

- Python 3.9+
- Flask (`pip install flask`)

### Installation (Pi 5)

1. Ensure `gateway.py` is running (it generates `../mesh_state.json`)
2. Install dependencies:

   ```bash
   cd dashboard
   pip install -r requirements.txt
   ```

### Running the Dashboard

Start the server (default port 5555):

```bash
python dashboard.py
```

Access via browser at `http://<pi5-ip>:5555`.

### Mock Mode (Development)

To test the UI without a live mesh network:

```bash
python dashboard.py --mock
```

This serves fake data where nodes update their timestamps automatically.

## Configuration

- **Port**: Change port with `--port 8000`
- **Host**: Change bind address with `--host 127.0.0.1`

## File Structure

- `dashboard.py`: Flask server and API endpoint (`/api/state`).
- `templates/index.html`: Main HTML file.
- `static/style.css`: Styling and dark theme.
- `static/dashboard.js`: D3.js graph logic and `fetch` polling loop.
