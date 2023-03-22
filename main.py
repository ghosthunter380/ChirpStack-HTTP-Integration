import json
import datetime
import mysql.connector
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from chirpstack_api import integration
from google.protobuf.json_format import Parse

# MySQL database configuration
db_config = {
    'user': 'chirpstack',
    'password': 'chirpstack',
    'host': 'localhost',
    'database': 'lorawan_cs'
}

class Handler(BaseHTTPRequestHandler):
    # True -  JSON marshaler
    # False - Protobuf marshaler (binary)
    json = True

    def do_POST(self):
        self.send_response(200)
        self.end_headers()
        query_args = parse_qs(urlparse(self.path).query)

        content_len = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_len)

        if query_args["event"][0] == "up":
            self.up(body)

        elif query_args["event"][0] == "join":
            self.join(body)

        else:
            print("handler for event %s is not implemented" % query_args["event"][0])

    def up(self, body):
        up = self.unmarshal(body, integration.UplinkEvent())
        dev_eui = up.device_info.dev_eui.hex()
        payload = up.data.hex()
        print("Uplink received from: %s with payload: %s" % (dev_eui, payload))
        
        # Decode the payload to get the device parameters
        try:
            payload_data = bytes.fromhex(payload).decode('utf-8')
            payload_json = json.loads(payload_data)
            
            # Filter device parameters such as coordinates, temperature, and humidity
            devName = payload_json['deviceInfo']['deviceName']
            gatewayID = payload_json['rxInfo']['0']['gatewayID']
            time = payload_json['rxInfo']['0']['time']
            timestamp = datetime.datetime.fromisoformat(time)
            formated_ts = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            temperature = payload_json['object']['temperature']
            lat = payload_json['object']['latitude']
            lon = payload_json['object']['longitude']
            alt = payload_json['object']['altitude']
            rssi = payload_json['rxInfo']['0']['rssi']
            snr = payload_json['rxInfo']['0']['snr']
            
            
            # Insert the device parameters into a MySQL database
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()
            query = "INSERT INTO lora_test (devName, time, temperature, latitude, longitude, altitude, gatewayID, rssi, snr) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (devName, formated_ts, temperature, lat, lon, alt, gatewayID, rssi, snr)
            cursor.execute(query, values)
            conn.commit()
            cursor.close()
            conn.close()
            
            print("Device parameters inserted into MySQL database")
            
        except Exception as e:
            print("Error decoding or filtering payload data:", e)

    def join(self, body):
        join = self.unmarshal(body, integration.JoinEvent())
        print("Device: %s joined with DevAddr: %s" % (join.device_info.dev_eui, join.dev_addr))

    def unmarshal(self, body, pl):
        if self.json:
            return Parse(body, pl)
        
        pl.ParseFromString(body)
        return pl

httpd = HTTPServer(('', 8090), Handler)
print("Listening on localhost:8090...")
httpd.serve_forever()
