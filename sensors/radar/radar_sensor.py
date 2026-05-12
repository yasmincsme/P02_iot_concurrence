"""
radar_sensor.py - Sensor de Radar Costeiro

Gera dados simulados de radar naval de forma autônoma:
  - Contagem de embarcações na área de cobertura
  - Velocidade média das embarcações
  - Direção predominante do tráfego (bearing 0-360°)
  - Anomalias: pico de tráfego, velocidade incomum
"""

import socket
import json
import time
import random
import logging
import os

BROKER_IP  = os.environ.get("BROKER_HOST", "localhost")
PORT       = int(os.environ.get("BROKER_PORT", "1883"))
SECTOR_ID  = os.environ.get("SECTOR_ID", "1")
CLIENT_ID  = f"radar_s{SECTOR_ID}"
TOPIC      = f"strait/sector/{SECTOR_ID}/sensors/radar"
INTERVAL   = float(os.environ.get("INTERVAL", "4"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [RADAR-S{SECTOR_ID}] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(CLIENT_ID)


def _enc_rem(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            break
    return bytes(out)


def build_connect(client_id):
    cid = client_id.encode()
    var = b"\x00\x04MQTT\x04\x02\x00\x3c" + bytes([len(cid) >> 8, len(cid) & 0xFF]) + cid
    return bytes([0x10]) + _enc_rem(len(var)) + var


def build_publish(topic, payload):
    tb  = topic.encode()
    var = bytes([len(tb) >> 8, len(tb) & 0xFF]) + tb + payload
    return bytes([0x30]) + _enc_rem(len(var)) + var


class RadarSensor:
    def __init__(self):
        self.vessel_count = random.randint(3, 12)
        self.bearing      = random.uniform(0, 360)
        self.avg_speed    = random.uniform(8, 18)   # knots

    def read(self):
        # Deriva suave do tráfego
        self.vessel_count = max(0, min(30, self.vessel_count + random.randint(-2, 2)))
        self.bearing      = (self.bearing + random.uniform(-5, 5)) % 360
        self.avg_speed    = max(2, min(35, self.avg_speed + random.uniform(-1.5, 1.5)))

        anomaly = False
        alert   = None

        # Congestionamento (tráfego acima do normal)
        if self.vessel_count > 22:
            anomaly = True
            alert   = "congestionamento_detectado"

        # Velocidade incomum (muito rápido ou muito devagar)
        if self.avg_speed > 28 or (self.vessel_count > 0 and self.avg_speed < 3):
            anomaly = True
            alert   = "velocidade_anomala"

        return {
            "id":           CLIENT_ID,
            "sector":       SECTOR_ID,
            "vessel_count": self.vessel_count,
            "bearing_deg":  round(self.bearing, 1),
            "avg_speed_kn": round(self.avg_speed, 1),
            "anomaly":      anomaly,
            "alert":        alert,
            "ts":           int(time.time()),
        }


def run():
    sensor = RadarSensor()
    log.info(f"Radar costeiro iniciado | tópico: {TOPIC}")

    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((BROKER_IP, PORT))
            sock.sendall(build_connect(CLIENT_ID))
            time.sleep(0.5)
            log.info(f"Conectado ao broker {BROKER_IP}:{PORT}")

            while True:
                data   = sensor.read()
                packet = build_publish(TOPIC, json.dumps(data).encode())
                sock.sendall(packet)
                if data["anomaly"]:
                    log.warning(f"ANOMALIA: {data['alert']} | embarcações={data['vessel_count']}")
                else:
                    log.info(
                        f"embarcações={data['vessel_count']} "
                        f"velocidade={data['avg_speed_kn']}kn "
                        f"bearing={data['bearing_deg']}°"
                    )
                time.sleep(INTERVAL)

        except Exception as e:
            log.warning(f"Erro de conexão: {e}, reconectando em 5s")
            try:
                sock.close()
            except Exception:
                pass
            time.sleep(5)


if __name__ == "__main__":
    run()
