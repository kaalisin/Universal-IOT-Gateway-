import json
import threading
from flask import Flask, jsonify, render_template_string, request
from flask_socketio import SocketIO
import paho.mqtt.client as mqtt

# ==================== CONFIGURATION ====================
MQTT_BROKER = "mqtt.dashboard.hydroscope.in"
MQTT_PORT = 8883  # 8883 for TLS/SSL
MQTT_USER = "hydrosure"
MQTT_PASSWORD = "prod_mqtthydrosure2026"
MQTT_USE_TLS = True  # True since port is 8883

CONFIG_TOPIC = "UIgateway/config/Device/Modbus"
MONITOR_TOPIC = "UIgateway/Device/Modbus/Data"
DEVICE_STATUS_TOPIC =    "UIgateway/Device/Status"
# =======================================================

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

mqtt_client = None
mqtt_connected = False


def on_mqtt_connect(client, userdata, flags, rc):
  global mqtt_connected
  if rc == 0:
    mqtt_connected = True
    print("[MQTT] Connected to broker successfully.")
    client.subscribe(MONITOR_TOPIC)
    print(f"[MQTT] Subscribed to topic: {MONITOR_TOPIC}")
  else:
    mqtt_connected = False
    print(f"[MQTT] Connection failed with code {rc}")


def on_mqtt_disconnect(client, userdata, rc):
  global mqtt_connected
  mqtt_connected = False
  print("[MQTT] Disconnected from broker.")


def on_mqtt_message(client, userdata, msg):
  try:
    payload = json.loads(msg.payload.decode("utf-8"))
    socketio.emit("sensor_data", payload)
  except Exception as e:
    print(f"[MQTT] Error parsing message on {msg.topic}: {e}")


def start_mqtt():
  global mqtt_client
  # Note: For newer paho-mqtt versions (v2.x), use callback_api_version=mqtt.CallbackAPIVersion.VERSION1 if needed
  mqtt_client = mqtt.Client()
  if MQTT_USER and MQTT_PASSWORD:
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

  if MQTT_USE_TLS:
    mqtt_client.tls_set()  # Uses system default certificates

  mqtt_client.on_connect = on_mqtt_connect
  mqtt_client.on_disconnect = on_mqtt_disconnect
  mqtt_client.on_message = on_mqtt_message

  print(f"[MQTT] Connecting to {MQTT_BROKER}:{MQTT_PORT}...")
  try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()
  except Exception as e:
    print(f"[MQTT] Failed to start MQTT client: {e}")


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Universal Industrial IoT Gateway</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.2/socket.io.min.js"></script>
<style>
* { box-sizing: border-box; }
body { margin: 0; font-family: Arial, sans-serif; background: #f3f6fb; color: #1e293b; }
header { padding: 22px 28px; background: #183b63; color: white; }
header h1 { margin: 0 0 7px; font-size: 23px; }
header p { margin: 0; color: #dbeafe; font-size: 14px; }
main { max-width: 1450px; margin: 22px auto; padding: 0 18px; }
.panel { background: white; border: 1px solid #dce4ef; border-radius: 9px; padding: 20px; margin-bottom: 20px; }
h2 { font-size: 19px; margin: 0 0 17px; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 14px; }
label { display: block; font-size: 13px; font-weight: 600; margin-bottom: 7px; }
input, select { width: 100%; padding: 10px; border: 1px solid #cbd5e1; border-radius: 5px; background: white; font-size: 13px; }
button { border: 0; border-radius: 5px; padding: 10px 14px; margin: 5px 4px 5px 0; cursor: pointer; font-weight: 600; }
button:disabled { opacity: .5; cursor: not-allowed; }
.primary { background: #1769d2; color: white; }
.secondary { background: #e8eef7; color: #183b63; }
.danger { background: #fee2e2; color: #991b1b; }
.success { background: #dcfce7; color: #166534; }
.hint { font-size: 12px; line-height: 1.6; color: #64748b; }
#notice { display: none; padding: 12px; border-radius: 5px; margin-bottom: 15px; font-size: 13px; overflow-wrap: anywhere; }
#notice.info { display: block; background: #eaf2ff; color: #174a91; }
#notice.error { display: block; background: #fee2e2; color: #991b1b; }
#notice.success { display: block; background: #dcfce7; color: #166534; }
.status { font-size: 13px; font-weight: 700; }
.online { color: #16834a; }
.offline { color: #c12b36; }
.device-card { border: 1px solid #dce4ef; border-radius: 7px; padding: 16px; margin-top: 16px; }
.device-heading { display: flex; align-items: center; justify-content: space-between; gap: 10px; }
.device-heading h3 { font-size: 15px; }
.table-wrap { overflow-x: auto; margin-top: 12px; }
table { width: 100%; border-collapse: collapse; }
.config-table { min-width: 1050px; }
.monitor-table { min-width: 760px; }
th, td { padding: 10px; border-bottom: 1px solid #e5eaf1; text-align: left; font-size: 12px; }
th { background: #edf3fa; white-space: nowrap; }
td input, td select { min-width: 90px; padding: 8px; }
.metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; }
.metric { padding: 14px; border: 1px solid #dce4ef; border-radius: 7px; background: #f9fbfe; }
.metric span { display: block; color: #64748b; font-size: 12px; margin-bottom: 8px; }
.metric strong { font-size: 22px; }
pre { background: #101b2c; color: #d5f5df; padding: 16px; border-radius: 6px; overflow: auto; max-height: 450px; font-size: 12px; white-space: pre-wrap; overflow-wrap: anywhere; }
</style>
</head>
<body>
<header>
    <h1>Universal Industrial IoT Gateway</h1>
    <p>Modbus configuration and real-time sensor monitoring (Backend MQTT Mode)</p>
</header>

<main>
<div id="notice" role="status" aria-live="polite"></div>

<section class="panel">
    <h2>1. Backend MQTT Status</h2>
    <p class="hint">Connected to mqtts://mqtt.dashboard.hydroscope.in:8883</p>
    <span id="connectionStatus" class="status online">Server Connected</span>
</section>

<section class="panel">
    <h2>2. Modbus Device Configuration</h2>
    <button class="primary" onclick="addDevice()">+ Add Device</button>
    <button class="secondary" onclick="loadExample()">Load Example</button>
    <button class="secondary" onclick="clearDevices()">Clear All</button>

    <div id="deviceContainer"></div>

    <button class="primary" onclick="generateJSON()">Generate JSON</button>
    <button class="success" onclick="publishConfiguration()">Publish Configuration</button>
</section>

<section class="panel">
    <h2>3. Configuration JSON</h2>
    <button class="secondary" onclick="copyJSON()">Copy JSON</button>
    <button class="secondary" onclick="downloadJSON()">Download JSON</button>
    <pre id="jsonPreview">No configuration generated.</pre>
</section>

<section class="panel">
    <h2>4. Live Sensor Monitoring</h2>
    <div class="metric-grid">
        <div class="metric"><span>Devices</span><strong id="deviceMetric">0</strong></div>
        <div class="metric"><span>Registers</span><strong id="registerMetric">0</strong></div>
        <div class="metric"><span>Readings Received</span><strong id="readingMetric">0</strong></div>
        <div class="metric"><span>Last Message</span><strong id="lastMessageMetric" style="font-size:13px">—</strong></div>
    </div>
    <button class="secondary" onclick="clearMonitoring()">Clear Table</button>
    <div class="table-wrap">
        <table class="monitor-table">
            <thead>
                <tr>
                    <th>Device No</th>
                    <th>Device Name</th>
                    <th>Register Name</th>
                    <th>Register Address</th>
                    <th>Value</th>
                    <th>Status</th>
                    <th>Last Update</th>
                </tr>
            </thead>
            <tbody id="monitorBody"></tbody>
        </table>
    </div>
</section>
</main>

<script>
"use strict";

const socket = io();
let lastPayload = null;
let deviceCounter = 0;
let readingCount = 0;

const DATA_TYPES = ["uint8", "int8", "uint16", "int16", "uint32", "int32", "uint64", "int64", "float32", "float64"];
const BYTE_ORDERS = ["ABCD", "BADC", "CDAB", "DCBA"];

function notice(message, type = "info") {
    const el = document.getElementById("notice");
    el.className = type;
    el.textContent = message;
}

function esc(v) {
    return String(v).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}

function integer(input, name, min, max) {
    const s = input.value.trim();
    if (!/^\\d+$/.test(s)) throw Error(name + " must be an integer.");
    const n = Number(s);
    if (!Number.isSafeInteger(n) || n < min || n > max) throw Error(name + " must be between " + min + " and " + max + ".");
    return n;
}

function numberValue(input, name) {
    const s = input.value.trim();
    const n = Number(s);
    if (s === "" || !Number.isFinite(n)) throw Error(name + " must be a finite number.");
    return n;
}

function hexValue(s, name, maxDigits) {
    s = String(s).trim();
    const re = new RegExp("^0x[0-9a-fA-F]{1," + maxDigits + "}$");
    if (!re.test(s)) throw Error(name + " must be hexadecimal, e.g. 0x01.");
    return "0x" + s.slice(2).toUpperCase().padStart(2, "0");
}

function addDevice(d = {}) {
    const id = ++deviceCounter;
    const card = document.createElement("div");
    card.className = "device-card";

    card.innerHTML = `
        <div class="device-heading">
            <h3>Device ${id}</h3>
            <button class="danger remove-device">Remove Device</button>
        </div>
        <div class="grid">
            <div><label>Device No</label><input class="device-no" type="number" min="0" max="255" value="${esc(d.Device_no ?? id)}"></div>
            <div><label>Slave ID (hex)</label><input class="slave-id" value="${esc(d.Slave_ID ?? "0x01")}"></div>
            <div><label>Function ID</label>
                <select class="function-id">
                    ${["01","02","03","04","05","06","0F","10"].map(x => `<option value="0x${x}">0x${x}</option>`).join("")}
                </select></div>
            <div><label>Device Name (max 9 chars)</label><input class="device-name" maxlength="9" value="${esc(d.Device_Name ?? "Sensor" + id)}"></div>
        </div>
        <p><b>Register Configuration</b></p>
        <button class="secondary add-register">+ Add Register</button>
        <div class="table-wrap">
        <table class="config-table">
        <thead><tr>
            <th>Register Name</th><th>Register Address</th><th>Quantity</th>
            <th>Data Type</th><th>Byte Order</th><th>Offset</th>
            <th>Slope</th><th>Decimal</th><th>Action</th>
        </tr></thead><tbody class="register-body"></tbody></table></div>`;

    card.querySelector(".function-id").value = d.Function_ID ?? "0x03";
    card.querySelector(".remove-device").onclick = () => { card.remove(); updateMetrics(); };
    card.querySelector(".add-register").onclick = () => addRegister(card);

    document.getElementById("deviceContainer").appendChild(card);

    if (d.Registers && d.Registers.length) {
        d.Registers.forEach(r => addRegister(card, r));
    } else {
        addRegister(card);
    }
    updateMetrics();
}

function addRegister(card, r = {}) {
    const body = card.querySelector(".register-body");
    if (body.children.length >= 255) { notice("Maximum 255 registers per device.", "error"); return; }

    const row = document.createElement("tr");
    const n = body.children.length + 1;

    row.innerHTML = `
        <td><input class="reg-name" maxlength="63" value="${esc(r.Register_Name ?? "Register" + n)}"></td>
        <td><input class="reg-address" value="${esc(r.Register_Address ?? "0x0000")}"></td>
        <td><input class="quantity" type="number" min="1" max="255" value="${esc(r.Quantity ?? 1)}"></td>
        <td><select class="data-type">${DATA_TYPES.map(x => `<option>${x}</option>`).join("")}</select></td>
        <td><select class="byte-order">${BYTE_ORDERS.map(x => `<option>${x}</option>`).join("")}</select></td>
        <td><input class="offset" type="number" step="any" value="${esc(r.Offset ?? 1)}"></td>
        <td><input class="slope" type="number" step="any" value="${esc(r.Slope ?? 1)}"></td>
        <td><input class="decimal" type="number" min="0" max="255" value="${esc(r.Decimal ?? 1)}"></td>
        <td><button class="danger remove-register">Remove</button></td>`;

    row.querySelector(".data-type").value = r.Data_Type ?? "uint16";
    row.querySelector(".byte-order").value = r.Byte_Order ?? "ABCD";
    row.querySelector(".remove-register").onclick = () => { row.remove(); updateMetrics(); };

    body.appendChild(row);
    updateMetrics();
}

function clearDevices() {
    document.getElementById("deviceContainer").replaceChildren();
    lastPayload = null;
    document.getElementById("jsonPreview").textContent = "No configuration generated.";
    updateMetrics();
}

function buildConfiguration() {
    const cards = [...document.querySelectorAll(".device-card")];
    if (!cards.length) throw Error("Add at least one device.");
    const seen = new Set();

    const Devices = cards.map(card => {
        const Device_no = integer(card.querySelector(".device-no"), "Device No", 0, 255);
        if (seen.has(Device_no)) throw Error("Duplicate Device No: " + Device_no);
        seen.add(Device_no);

        const Slave_ID = hexValue(card.querySelector(".slave-id").value, "Slave ID", 2);
        const Function_ID = hexValue(card.querySelector(".function-id").value, "Function ID", 2);
        const Device_Name = card.querySelector(".device-name").value.trim();

        if (!Device_Name || Device_Name.length > 9) throw Error("Device_Name must contain 1 to 9 characters.");

        const rows = [...card.querySelectorAll(".register-body tr")];
        if (!rows.length) throw Error("Every device needs at least one register.");

        const Register_Name = [], Register_Address = [], Quantity = [], Data_Type = [], Byte_Order = [], Offset = [], Slope = [], Decimal = [];

        rows.forEach((row, i) => {
            const prefix = `Device ${Device_no}, register ${i + 1}: `;
            const name = row.querySelector(".reg-name").value.trim();
            if (!name || name.length > 63) throw Error(prefix + "Invalid register name.");

            const address = row.querySelector(".reg-address").value.trim();
            if (!/^0x[0-9a-fA-F]{1,16}$/.test(address)) throw Error(prefix + "Invalid register address.");

            const addressBig = BigInt(address);
            Register_Name.push(name);
            Register_Address.push("0x" + addressBig.toString(16).toUpperCase().padStart(4, "0"));
            Quantity.push(integer(row.querySelector(".quantity"), prefix + "Quantity", 1, 255));
            Data_Type.push(row.querySelector(".data-type").value);
            Byte_Order.push(row.querySelector(".byte-order").value);
            Offset.push(numberValue(row.querySelector(".offset"), prefix + "Offset"));
            Slope.push(numberValue(row.querySelector(".slope"), prefix + "Slope"));
            Decimal.push(integer(row.querySelector(".decimal"), prefix + "Decimal", 0, 255));
        });

        return { Device_no, Slave_ID, Function_ID, Device_Name, Register_Count: rows.length, Register_Name, Register_Address, Quantity, Data_Type, Byte_Order, Offset, Slope, Decimal };
    });

    return { Device_count: Devices.length, Devices };
}

function generateJSON() {
    try {
        lastPayload = buildConfiguration();
        document.getElementById("jsonPreview").textContent = JSON.stringify(lastPayload, null, 2);
        updateMetrics();
        notice("Configuration JSON generated.", "success");
        return true;
    } catch (e) {
        notice(e.message, "error");
        return false;
    }
}

async function copyJSON() {
    if (!lastPayload && !generateJSON()) return;
    try {
        await navigator.clipboard.writeText(JSON.stringify(lastPayload, null, 2));
        notice("JSON copied.", "success");
    } catch {
        notice("Clipboard unavailable.", "error");
    }
}

function downloadJSON() {
    if (!lastPayload && !generateJSON()) return;
    const blob = new Blob([JSON.stringify(lastPayload, null, 2)], {type:"application/json"});
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "modbus_configuration.json";
    a.click();
    URL.revokeObjectURL(url);
}

function publishConfiguration() {
    if (!generateJSON()) return;

    fetch('/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(lastPayload)
    })
    .then(res => res.json())
    .then(data => {
        if (data.success) {
            notice("Configuration published successfully by backend!", "success");
            clearMonitoring();
        } else {
            notice("Publish failed: " + data.error, "error");
        }
    })
    .catch(err => notice("Publish error: " + err, "error"));
}

function configuredAddress(deviceNo, registerName, index) {
    if (!lastPayload) return "—";
    const d = lastPayload.Devices.find(x => Number(x.Device_no) === Number(deviceNo));
    if (!d) return "—";
    if (d.Register_Name[index] === registerName) return d.Register_Address[index] ?? "—";
    const idx = d.Register_Name.indexOf(registerName);
    return idx >= 0 ? d.Register_Address[idx] : "—";
}

socket.on('sensor_data', function(packet) {
    if (!packet || !Array.isArray(packet.Devices)) return;

    const tbody = document.getElementById("monitorBody");
    const timestamp = new Date().toLocaleString();

    packet.Devices.forEach(d => {
        if (!Array.isArray(d.Register_Name) || !Array.isArray(d.Status) || !Array.isArray(d.Value)) return;
        const count = Math.max(d.Register_Name.length, d.Status.length, d.Value.length);

        for (let i = 0; i < count; i++) {
            const key = String(d.Device_no) + "_" + i;
            let row = [...tbody.rows].find(r => r.dataset.key === key);

            if (!row) {
                row = document.createElement("tr");
                row.dataset.key = key;
                for (let j = 0; j < 7; j++) row.appendChild(document.createElement("td"));
                tbody.appendChild(row);
            }

            row.cells[0].textContent = d.Device_no ?? "";
            row.cells[1].textContent = d.Device_Name ?? "";
            row.cells[2].textContent = d.Register_Name[i] ?? ("Register " + (i + 1));
            row.cells[3].textContent = configuredAddress(d.Device_no, d.Register_Name[i], i);
            row.cells[4].textContent = d.Value[i] == null ? "—" : String(d.Value[i]);
            row.cells[5].textContent = d.Status[i] ?? "Unknown";
            row.cells[5].style.color = String(d.Status[i]).toLowerCase() === "success" ? "#16834a" : "#c12b36";
            row.cells[6].textContent = timestamp;
            readingCount++;
        }
    });

    document.getElementById("readingMetric").textContent = readingCount;
    document.getElementById("lastMessageMetric").textContent = new Date().toLocaleTimeString();
    while (tbody.rows.length > 300) tbody.deleteRow(0);
});

function clearMonitoring() {
    document.getElementById("monitorBody").replaceChildren();
    readingCount = 0;
    document.getElementById("readingMetric").textContent = "0";
    document.getElementById("lastMessageMetric").textContent = "—";
}

function updateMetrics() {
    const cards = [...document.querySelectorAll(".device-card")];
    document.getElementById("deviceMetric").textContent = cards.length;
    document.getElementById("registerMetric").textContent = cards.reduce((sum, c) => sum + c.querySelectorAll(".register-body tr").length, 0);
}

function loadExample() {
    clearDevices();
    [{Device_no:1, Device_Name:"ORP"}, {Device_no:2, Device_Name:"ORP2"}, {Device_no:3, Device_Name:"ORP3"}].forEach(d => addDevice({
        ...d, Slave_ID:"0x01", Function_ID:"0x03", Registers:[{ Register_Name:"ORP", Register_Address:"0x0000", Quantity:1, Data_Type:"uint16", Byte_Order:"ABCD", Offset:1, Slope:1, Decimal:1 }]
    }));
    generateJSON();
}

addDevice();
</script>
</body>
</html>
"""


@app.route("/")
def index():
  return render_template_string(HTML_TEMPLATE)


@app.route("/publish", methods=["POST"])
def publish_config():
  global mqtt_client, mqtt_connected
  if not mqtt_client or not mqtt_connected:
    return jsonify({"success": False, "error": "Backend MQTT client not connected."}), 500

  try:
    data = request.json
    payload_str = json.dumps(data)
    info = mqtt_client.publish(CONFIG_TOPIC, payload_str, qos=1)
    info.wait_for_publish()
    return jsonify({"success": True})
  except Exception as e:
    return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
  mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
  mqtt_thread.start()

  print("Starting Web Server at http://localhost:5000")
  socketio.run(app, host="0.0.0.0", port=5000, debug=True)