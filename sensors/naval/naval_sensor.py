"""
naval_sensor.py - Sensor Naval de Monitoramento Fixo

Gera dados simulados de sensor naval de forma autônoma:
  - Nível acústico subaquático (dB)
  - Detecção de anomalia magnética
  - Contagem de contatos de superfície
  - Distância ao contato mais próximo (metros)
  - Anomalias: nível acústico alto, anomalia magnética detectada

Comunicação:
  - Handshake inicial: TCP CONNECT → CONNACK
  - Envio de dados:    UDP PUBLISH (fire-and-forget)
"""

import socket
import json
import time
import random
import logging
import os

BROKER_IP = os.environ.get("BROKER_HOST", "localhost")
PORT      = int(os.environ.get("BROKER_PORT", "1883"))
SECTOR_ID = os.environ.get("SECTOR_ID", "1")
CLIENT_ID = f"naval_s{SECTOR_ID}"
TOPIC     = f"strait/sector/{SECTOR_ID}/sensors/naval"
INTERVAL  = float(os.environ.get("INTERVAL", "5"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [NAVAL-S{SECTOR_ID}] %(message)s",
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


class NavalSensor:
    def __init__(self):
        self.acoustic_db      = random.uniform(35, 55)
        self.surface_contacts = random.randint(0, 6)
        self.nearest_m        = random.uniform(500, 5000)

    def read(self):
        self.acoustic_db      = max(20, min(120, self.acoustic_db + random.uniform(-3, 3)))
        self.surface_contacts = max(0, min(15, self.surface_contacts + random.randint(-1, 1)))
        self.nearest_m        = max(50, min(10000, self.nearest_m + random.uniform(-200, 200)))

        magnetic_anomaly = random.random() < 0.03

        anomaly = False
        alert   = None
        if self.acoustic_db > 90:
            anomaly = True
            alert   = "nivel_acustico_critico"
        elif magnetic_anomaly:
            anomaly = True
            alert   = "anomalia_magnetica_detectada"
        elif self.surface_contacts > 12:
            anomaly = True
            alert   = "multiplos_contatos_suspeitos"

        return {
            "id":                CLIENT_ID,
            "sector":            SECTOR_ID,
            "acoustic_db":       round(self.acoustic_db, 1),
            "surface_contacts":  self.surface_contacts,
            "nearest_contact_m": round(self.nearest_m, 0),
            "magnetic_anomaly":  magnetic_anomaly,
            "anomaly":           anomaly,
            "alert":             alert,
            "ts":                int(time.time()),
        }


def run():
    sensor   = NavalSensor()
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    log.info(f"Sensor naval iniciado | tópico: {TOPIC}")

    # ── Handshake TCP inicial ────────────────────────────────────────────────
    while True:
        try:
            tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp.settimeout(5)
            tcp.connect((BROKER_IP, PORT))
            tcp.sendall(build_connect(CLIENT_ID))
            ack = tcp.recv(4)
            tcp.close()
            if ack and len(ack) >= 4 and ack[3] == 0:
                log.info(f"Handshake TCP OK com {BROKER_IP}:{PORT} | publicando via UDP")
                break
            raise ConnectionError("CONNACK inválido")
        except Exception as e:
            log.warning(f"Broker indisponível: {e}, tentando em 5s")
            try:
                tcp.close()
            except Exception:
                pass
            time.sleep(5)

    # ── Loop de publicação UDP ────────────────────────────────────────────────
    while True:
        data   = sensor.read()
        packet = build_publish(TOPIC, json.dumps(data).encode())
        udp_sock.sendto(packet, (BROKER_IP, PORT))
        if data["anomaly"]:
            log.warning(f"ANOMALIA: {data['alert']}")
        else:
            log.info(
                f"acústico={data['acoustic_db']}dB "
                f"contatos={data['surface_contacts']} "
                f"mais_próx={data['nearest_contact_m']}m"
            )
        time.sleep(INTERVAL)


if __name__ == "__main__":
    run()
