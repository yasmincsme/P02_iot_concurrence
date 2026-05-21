import socket
import threading
import json
import time
import random
import logging
import os
import heapq

SECTOR_ID   = int(os.environ.get("SECTOR_ID", "1"))
LOCAL_BROKER = os.environ.get("LOCAL_BROKER", "localhost")
BROKER_PORT  = int(os.environ.get("BROKER_PORT", "1883"))
RA_PORT      = int(os.environ.get("RA_PORT", "5001"))

PEERS_ENV       = os.environ.get("PEERS", "")
DRONES_ENV      = os.environ.get("DRONES", "drone_1:localhost:1883")
ALL_BROKERS_ENV = os.environ.get("ALL_BROKERS", "localhost:1883")

REPLY_TIMEOUT    = float(os.environ.get("REPLY_TIMEOUT", "6.0"))
MISSION_MIN      = int(os.environ.get("MISSION_MIN", "20"))
MISSION_MAX      = int(os.environ.get("MISSION_MAX", "60"))
OCC_INTERVAL_MIN = int(os.environ.get("OCC_MIN", "25"))
OCC_INTERVAL_MAX = int(os.environ.get("OCC_MAX", "70"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [SETOR-{SECTOR_ID}] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(f"setor_{SECTOR_ID}")

OCCURRENCE_TYPES = {
    "bloqueio_de_rota":       4,
    "embarcacao_a_deriva":    4,
    "risco_ambiental":        4,
    "falha_de_sinalizacao":   3,
    "congestionamento":       3,
    "inspecao_urgente":       3,
    "objeto_nao_identificado":2,
    "inspecao_rotineira":     1,
}


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


def _dec_rem(sock):
    mult, val = 1, 0
    for _ in range(4):
        b = sock.recv(1)
        if not b:
            return None
        byte = b[0]
        val += (byte & 0x7F) * mult
        if not (byte & 0x80):
            return val
        mult <<= 7
    return None


def _read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c:
            return None
        buf += c
    return buf


def _topic_matches(pattern, topic):
    def m(p, t):
        if not p:
            return not t
        if p[0] == "#":
            return True
        if not t:
            return p == ["#"]
        if p[0] in ("+", t[0]):
            return m(p[1:], t[1:])
        return False
    return pattern == topic or m(pattern.split("/"), topic.split("/"))


class MQTTClient:
    def __init__(self, host, port, client_id):
        self.host      = host
        self.port      = port
        self.client_id = client_id
        self._sock     = None
        self._lock     = threading.Lock()
        self._cbs      = {}
        self._mid      = 0
        self._alive    = False

    def connect(self, retries=15, delay=3):
        for i in range(1, retries + 1):
            try:
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._sock.settimeout(5)
                self._sock.connect((self.host, self.port))
                self._sock.settimeout(None)
                cid = self.client_id.encode()
                var = (b"\x00\x04MQTT\x04\x02\x00\x3c"
                       + bytes([len(cid) >> 8, len(cid) & 0xFF]) + cid)
                self._sock.sendall(bytes([0x10]) + _enc_rem(len(var)) + var)
                ack = _read_exact(self._sock, 4)
                if not ack or ack[0] != 0x20 or ack[3] != 0:
                    raise ConnectionError("CONNACK inválido")
                self._alive = True
                threading.Thread(target=self._reader, daemon=True).start()
                log.info(f"MQTT conectado {self.host}:{self.port} (id={self.client_id})")
                return
            except Exception as e:
                log.warning(f"MQTT conexão {i}/{retries} → {self.host}:{self.port}: {e}")
                time.sleep(delay)
        raise ConnectionError(f"Falha ao conectar ao broker {self.host}:{self.port}")

    def publish(self, topic, payload, qos=0, retain=False):
        if isinstance(payload, str):
            payload = payload.encode()
        tb  = topic.encode()
        var = bytes([len(tb) >> 8, len(tb) & 0xFF]) + tb + payload
        f   = (qos << 1) | (1 if retain else 0)
        pkt = bytes([0x30 | f]) + _enc_rem(len(var)) + var
        with self._lock:
            try:
                self._sock.sendall(pkt)
            except Exception as e:
                log.warning(f"Publish falhou {topic}: {e}")

    def subscribe(self, topic, callback):
        if topic not in self._cbs:
            self._cbs[topic] = []
        self._cbs[topic].append(callback)
        self._mid = (self._mid % 65535) + 1
        mid = self._mid
        tb  = topic.encode()
        var = bytes([mid >> 8, mid & 0xFF, len(tb) >> 8, len(tb) & 0xFF]) + tb + b"\x00"
        pkt = bytes([0x82]) + _enc_rem(len(var)) + var
        with self._lock:
            try:
                self._sock.sendall(pkt)
            except Exception as e:
                log.warning(f"Subscribe falhou {topic}: {e}")

    def _reader(self):
        while self._alive:
            try:
                hdr = self._sock.recv(1)
                if not hdr:
                    break
                ptype = (hdr[0] >> 4) & 0x0F
                flags = hdr[0] & 0x0F
                rem   = _dec_rem(self._sock)
                if rem is None:
                    break
                data  = _read_exact(self._sock, rem) if rem else b""
                if data is None:
                    break
                if ptype == 3:
                    qos  = (flags >> 1) & 0x03
                    tlen = (data[0] << 8) | data[1]
                    top  = data[2:2 + tlen].decode()
                    off  = 2 + tlen
                    if qos > 0:
                        mid = (data[off] << 8) | data[off + 1]
                        off += 2
                        with self._lock:
                            self._sock.sendall(bytes([0x40, 0x02, mid >> 8, mid & 0xFF]))
                    msg = data[off:]
                    for pat, cbs in self._cbs.items():
                        if _topic_matches(pat, top):
                            for cb in cbs:
                                try:
                                    cb(top, msg)
                                except Exception as e:
                                    log.warning(f"Callback erro: {e}")
                elif ptype == 12:
                    with self._lock:
                        self._sock.sendall(bytes([0xD0, 0x00]))
            except Exception as e:
                if self._alive:
                    log.warning(f"Leitura MQTT: {e}")
                break


class LamportClock:
    def __init__(self):
        self._t    = 0
        self._lock = threading.Lock()

    def tick(self):
        with self._lock:
            self._t += 1
            return self._t

    def update(self, received: int):
        with self._lock:
            self._t = max(self._t, received) + 1
            return self._t

    @property
    def value(self):
        with self._lock:
            return self._t


class RicartAgrawala:

    def __init__(self, sector_id: int, peer_count: int,
                 clock: LamportClock, send_fn):
        self.sector_id  = sector_id
        self.peer_count = peer_count
        self.clock      = clock
        self.send_fn    = send_fn

        self._lock       = threading.Lock()
        self._requesting = {}
        self._deferred   = {}
        self._replies    = {}
        self._events     = {}

    def request(self, drone_id: str, criticality: int,
                occurrence_id: str, timeout: float = None) -> bool:
        ts = self.clock.tick()

        with self._lock:
            self._requesting[drone_id] = {"ts": ts, "crit": criticality, "occ": occurrence_id}
            self._replies[drone_id]    = set()
            ev = threading.Event()
            self._events[drone_id]     = ev

        self.send_fn({
            "type":          "REQUEST",
            "drone_id":      drone_id,
            "sector_id":     self.sector_id,
            "timestamp":     ts,
            "criticality":   criticality,
            "occurrence_id": occurrence_id,
        })
        log.info(f"RA REQUEST {drone_id} ts={ts} crit={criticality} occ={occurrence_id}")

        if self.peer_count == 0:
            return True

        deadline = time.time() + (timeout or REPLY_TIMEOUT)
        while True:
            with self._lock:
                n = len(self._replies.get(drone_id, set()))
            if n >= self.peer_count:
                break
            if time.time() > deadline:
                with self._lock:
                    n = len(self._replies.get(drone_id, set()))
                log.warning(
                    f"RA TIMEOUT {drone_id}: {n}/{self.peer_count} replies, "
                    "assumindo peers falhos como OK"
                )
                break
            time.sleep(0.05)

        log.info(f"RA ADQUIRIDO {drone_id}")
        return True

    def handle_request(self, msg: dict):
        sender   = msg["sector_id"]
        drone_id = msg["drone_id"]
        req_ts   = msg["timestamp"]
        req_crit = msg["criticality"]

        self.clock.update(req_ts)

        with self._lock:
            our = self._requesting.get(drone_id)
            defer = False

            if our:
                our_ts, our_crit = our["ts"], our["crit"]

                if our_crit > req_crit:
                    defer = True
                elif our_crit == req_crit and our_ts < req_ts:
                    defer = True
                elif our_crit == req_crit and our_ts == req_ts and self.sector_id < sender:
                    defer = True

            if defer:
                if drone_id not in self._deferred:
                    self._deferred[drone_id] = []
                if sender not in self._deferred[drone_id]:
                    self._deferred[drone_id].append(sender)
                log.debug(f"RA DEFER reply para setor {sender} ({drone_id})")
            else:
                self._send_reply(drone_id, sender)

    def handle_reply(self, msg: dict):
        drone_id    = msg["drone_id"]
        from_sector = msg["from_sector"]
        to_sector   = msg.get("to_sector")

        if to_sector is not None and to_sector != self.sector_id:
            return

        with self._lock:
            if drone_id in self._replies:
                self._replies[drone_id].add(from_sector)
                if len(self._replies[drone_id]) >= self.peer_count:
                    if drone_id in self._events:
                        self._events[drone_id].set()

        log.debug(f"RA REPLY de setor {from_sector} para {drone_id}")

    def release(self, drone_id: str):
        with self._lock:
            self._requesting.pop(drone_id, None)
            self._replies.pop(drone_id, None)
            self._events.pop(drone_id, None)
            deferred = self._deferred.pop(drone_id, [])

        for sector in deferred:
            self._send_reply(drone_id, sector)

        self.send_fn({
            "type":      "RELEASE",
            "drone_id":  drone_id,
            "sector_id": self.sector_id,
        })
        log.info(f"RA RELEASE {drone_id} ({len(deferred)} replies adiados enviados)")

    def _send_reply(self, drone_id: str, target_sector: int):
        self.send_fn({
            "type":        "REPLY",
            "drone_id":    drone_id,
            "from_sector": self.sector_id,
            "to_sector":   target_sector,
        })


class SectorManager:

    def __init__(self):
        self.sector_id = SECTOR_ID

        self.peers = []
        for p in PEERS_ENV.split(","):
            p = p.strip()
            if p:
                h, prt = p.rsplit(":", 1)
                self.peers.append((h, int(prt)))

        self.drone_map = {}
        for entry in DRONES_ENV.split(","):
            entry = entry.strip()
            if entry:
                parts = entry.split(":")
                self.drone_map[parts[0]] = (parts[1], int(parts[2]))

        self.all_brokers = []
        for b in ALL_BROKERS_ENV.split(","):
            b = b.strip()
            if b:
                h, prt = b.rsplit(":", 1)
                self.all_brokers.append((h, int(prt)))

        self.drone_status = {d: "offline" for d in self.drone_map}
        self.drone_lock   = threading.Lock()

        self.occ_queue   = []
        self.occ_counter = 0
        self.occ_lock    = threading.Lock()

        self.missions      = {}
        self.missions_lock = threading.Lock()

        self.clock = LamportClock()

        self.ra = RicartAgrawala(
            sector_id  = self.sector_id,
            peer_count = len(self.peers),
            clock      = self.clock,
            send_fn    = self._broadcast_ra,
        )

        self.ra_conns = {}
        self.ra_lock  = threading.Lock()

        self.local_mqtt = MQTTClient(
            LOCAL_BROKER, BROKER_PORT,
            f"setor_{SECTOR_ID}_local"
        )
        self.broker_mqtts = {}
        for (h, prt) in self.all_brokers:
            cid = f"setor_{SECTOR_ID}_{h}_{prt}"
            self.broker_mqtts[(h, prt)] = MQTTClient(h, prt, cid)

        self._running = False

    def start(self):
        self._running = True

        threading.Thread(target=self._ra_server, daemon=True).start()
        time.sleep(0.3)

        self.local_mqtt.connect()
        for key, client in self.broker_mqtts.items():
            try:
                client.connect()
            except Exception as e:
                log.warning(f"Broker {key} indisponível: {e}")

        for client in self.broker_mqtts.values():
            client.subscribe("strait/drones/+/status", self._on_drone_status)

        self.local_mqtt.subscribe(
            f"strait/sector/{self.sector_id}/sensors/+",
            self._on_sensor_data
        )

        time.sleep(2)
        threading.Thread(target=self._connect_ra_peers, daemon=True).start()

        threading.Thread(target=self._occurrence_generator, daemon=True).start()

        threading.Thread(target=self._occurrence_dispatcher, daemon=True).start()

        log.info(
            f"Setor {self.sector_id} iniciado | "
            f"Drones: {list(self.drone_map.keys())} | "
            f"Peers RA: {self.peers}"
        )

        while self._running:
            time.sleep(1)

    def _ra_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", RA_PORT))
        srv.listen(20)
        log.info(f"RA server escutando na porta {RA_PORT}")
        while self._running:
            try:
                conn, addr = srv.accept()
                threading.Thread(
                    target=self._handle_ra_conn,
                    args=(conn, addr),
                    daemon=True
                ).start()
            except Exception as e:
                log.warning(f"RA server erro: {e}")

    def _handle_ra_conn(self, conn, addr):
        buf = b""
        while self._running:
            try:
                data = conn.recv(4096)
                if not data:
                    break
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    try:
                        msg = json.loads(line.decode())
                        self._process_ra_msg(msg)
                    except Exception as e:
                        log.warning(f"Mensagem RA inválida: {e}")
            except Exception as e:
                log.warning(f"RA conn {addr}: {e}")
                break
        try:
            conn.close()
        except Exception:
            pass

    def _connect_ra_peers(self):
        for (host, port) in self.peers:
            for attempt in range(12):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    sock.connect((host, port))
                    sock.settimeout(None)
                    with self.ra_lock:
                        self.ra_conns[(host, port)] = sock
                    log.info(f"RA conectado ao peer {host}:{port}")
                    break
                except Exception as e:
                    log.warning(f"RA peer {host}:{port} tentativa {attempt + 1}/12: {e}")
                    time.sleep(3)

    def _broadcast_ra(self, msg: dict):
        data = (json.dumps(msg) + "\n").encode()
        with self.ra_lock:
            conns = list(self.ra_conns.values())
        for conn in conns:
            try:
                conn.sendall(data)
            except Exception as e:
                log.warning(f"RA broadcast falhou: {e}")

    def _process_ra_msg(self, msg: dict):
        mtype = msg.get("type")
        if mtype == "REQUEST":
            self.ra.handle_request(msg)
        elif mtype == "REPLY":
            self.ra.handle_reply(msg)
        elif mtype == "RELEASE":
            pass

    def _on_drone_status(self, topic: str, payload: bytes):
        try:
            parts    = topic.split("/")
            drone_id = parts[2]
            data     = json.loads(payload)
            status   = data.get("status", "available")
            with self.drone_lock:
                if drone_id in self.drone_status:
                    prev = self.drone_status[drone_id]
                    self.drone_status[drone_id] = status
                    if prev != status:
                        log.info(f"Drone {drone_id}: {prev} → {status}")
        except Exception as e:
            log.warning(f"Erro ao processar status de drone: {e}")

    def _on_sensor_data(self, topic: str, payload: bytes):
        try:
            data        = json.loads(payload)
            sensor_type = topic.split("/")[-1]
            self._check_sensor_anomaly(sensor_type, data)
        except Exception:
            pass

    def _check_sensor_anomaly(self, sensor_type: str, data: dict):
        thresholds = {"radar": 0.04, "buoy": 0.03}
        chance = thresholds.get(sensor_type, 0.02)

        if random.random() < chance:
            occ_candidates = {
                "radar": ["objeto_nao_identificado", "bloqueio_de_rota", "congestionamento"],
                "buoy":  ["risco_ambiental", "falha_de_sinalizacao"],
            }
            occ_type = random.choice(occ_candidates.get(sensor_type, ["inspecao_urgente"]))
            self._enqueue_occurrence(occ_type, f"anomalia em sensor {sensor_type}")

    def _occurrence_generator(self):
        time.sleep(random.uniform(8, 20))
        while self._running:
            occ_type = random.choice(list(OCCURRENCE_TYPES.keys()))
            self._enqueue_occurrence(
                occ_type,
                f"detectado pelo monitoramento do setor {self.sector_id}"
            )
            wait = random.uniform(OCC_INTERVAL_MIN, OCC_INTERVAL_MAX)
            log.info(f"Próxima ocorrência em ~{wait:.0f}s")
            time.sleep(wait)

    def _enqueue_occurrence(self, occ_type: str, reason: str):
        criticality = OCCURRENCE_TYPES.get(occ_type, 1)
        ts          = self.clock.tick()

        with self.occ_lock:
            self.occ_counter += 1
            occ_id = f"occ_s{self.sector_id}_{self.occ_counter:04d}"
            occ = {
                "id":          occ_id,
                "type":        occ_type,
                "criticality": criticality,
                "sector_id":   self.sector_id,
                "timestamp":   ts,
                "reason":      reason,
            }
            heapq.heappush(
                self.occ_queue,
                (-criticality, ts, self.sector_id, self.occ_counter, occ)
            )

        log.info(
            f"OCORRÊNCIA enfileirada: {occ_id} "
            f"tipo={occ_type} crit={criticality}"
        )
        self.local_mqtt.publish(
            f"strait/sector/{self.sector_id}/occurrence",
            json.dumps(occ)
        )

    def _occurrence_dispatcher(self):
        while self._running:
            occ = None
            with self.occ_lock:
                if self.occ_queue:
                    _, _, _, _, occ = heapq.heappop(self.occ_queue)

            if occ:
                threading.Thread(
                    target=self._handle_occurrence,
                    args=(occ,),
                    daemon=True
                ).start()
            else:
                time.sleep(0.5)

    def _handle_occurrence(self, occ: dict):
        occ_id = occ["id"]
        crit   = occ["criticality"]
        log.info(f"Tratando {occ_id} (tipo={occ['type']}, crit={crit})")

        max_attempts = 15
        drone_id     = None

        for attempt in range(1, max_attempts + 1):
            candidate = self._pick_available_drone()
            if not candidate:
                log.info(f"{occ_id}: nenhum drone disponível (tentativa {attempt}), aguardando...")
                time.sleep(5)
                continue

            drone_id = candidate
            log.info(f"{occ_id}: tentando adquirir {drone_id} via Ricart-Agrawala (tentativa {attempt})")

            with self.drone_lock:
                if self.drone_status.get(drone_id) != "available":
                    drone_id = None
                    continue
                self.drone_status[drone_id] = "requesting"

            self.ra.request(drone_id, crit, occ_id)

            with self.drone_lock:
                current = self.drone_status.get(drone_id)
                if current in ("available", "requesting"):
                    self.drone_status[drone_id] = "busy"
                    break
                else:
                    log.warning(
                        f"{occ_id}: {drone_id} ficou {current} durante RA, liberando e tentando outro"
                    )
                    self.ra.release(drone_id)
                    drone_id = None

        if not drone_id:
            log.error(f"{occ_id}: FALHA ao adquirir drone após {max_attempts} tentativas")
            occ_retry = dict(occ)
            occ_retry["criticality"] = max(1, occ["criticality"] - 1)
            time.sleep(10)
            self._enqueue_occurrence(occ["type"], f"re-enfileirado: {occ['reason']}")
            return

        self._dispatch_drone(drone_id, occ)

    def _pick_available_drone(self):
        with self.drone_lock:
            available = [d for d, s in self.drone_status.items() if s == "available"]
        return random.choice(available) if available else None

    def _dispatch_drone(self, drone_id: str, occ: dict):
        occ_id = occ["id"]

        with self.missions_lock:
            self.missions[drone_id] = occ_id

        dispatch_msg = {
            "drone_id":        drone_id,
            "sector_id":       self.sector_id,
            "occurrence_id":   occ_id,
            "occurrence_type": occ["type"],
            "criticality":     occ["criticality"],
            "timestamp":       time.time(),
        }

        log.info(f"DESPACHANDO {drone_id} → {occ_id} (tipo={occ['type']})")

        broker_addr = self.drone_map.get(drone_id)
        if broker_addr and broker_addr in self.broker_mqtts:
            self.broker_mqtts[broker_addr].publish(
                f"strait/drones/{drone_id}/dispatch",
                json.dumps(dispatch_msg)
            )
        else:
            self.local_mqtt.publish(
                f"strait/drones/{drone_id}/dispatch",
                json.dumps(dispatch_msg)
            )

        mission_duration = random.uniform(MISSION_MIN, MISSION_MAX)
        log.info(f"Missão {occ_id}: duração estimada {mission_duration:.0f}s")

        elapsed        = 0
        check_interval = 5
        reallocated    = False

        while elapsed < mission_duration:
            time.sleep(check_interval)
            elapsed += check_interval

            with self.drone_lock:
                status = self.drone_status.get(drone_id, "offline")

            if status == "offline":
                log.warning(f"{occ_id}: drone {drone_id} FALHOU em missão, realocando")
                with self.missions_lock:
                    self.missions.pop(drone_id, None)
                self.ra.release(drone_id)
                self._enqueue_occurrence(occ["type"], f"realocação após falha de {drone_id}")
                reallocated = True
                return

        if not reallocated:
            log.info(f"Missão {occ_id} concluída, liberando {drone_id}")

            with self.drone_lock:
                if self.drone_status.get(drone_id) == "busy":
                    self.drone_status[drone_id] = "available"

            with self.missions_lock:
                self.missions.pop(drone_id, None)

            self.ra.release(drone_id)

            recall_msg = {
                "drone_id":  drone_id,
                "command":   "recall",
                "sector_id": self.sector_id,
            }
            if broker_addr and broker_addr in self.broker_mqtts:
                self.broker_mqtts[broker_addr].publish(
                    f"strait/drones/{drone_id}/recall",
                    json.dumps(recall_msg)
                )


def main():
    manager = SectorManager()
    manager.start()


if __name__ == "__main__":
    main()
