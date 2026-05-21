<div align="center">

# Sistema de Monitoramento do Estreito Marítimo

#### Projeto da disciplina TEC 502 - Concorrência e Conectividade

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-306998?logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![MQTT](https://img.shields.io/badge/MQTT-660066?logoColor=white)](https://mqtt.org/)

</div>

> Este projeto implementa um sistema distribuído de monitoramento de um estreito marítimo. Quatro setores independentes operam com seus próprios brokers MQTT, gerenciadores de setor, drones e sensores. O algoritmo de **Ricart-Agrawala** garante exclusão mútua distribuída na alocação de drones compartilhados entre setores. A comunicação dos sensores com os brokers é feita via **UDP** (fire-and-forget), enquanto drones e gerenciadores utilizam **TCP**. Todo o protocolo MQTT foi implementado do zero, sem bibliotecas externas. Desenvolvido para a disciplina de Concorrência e Conectividade na Universidade Estadual de Feira de Santana (UEFS).

---

## Sumário

- [Introdução](#introdução)
- [Tecnologias e Ferramentas Utilizadas](#tecnologias-e-ferramentas-utilizadas)
- [Funcionalidades](#funcionalidades)
- [Arquitetura do Sistema](#arquitetura-do-sistema)
- [Componentes](#componentes)
  - [Broker MQTT](#broker-mqtt)
  - [Gerenciador de Setor](#gerenciador-de-setor)
  - [Drone](#drone)
  - [Sensores](#sensores)
  - [Monitor TUI](#monitor-tui)
- [Algoritmo de Exclusão Mútua](#algoritmo-de-exclusão-mútua-ricart-agrawala)
- [Protocolo de Comunicação](#protocolo-de-comunicação)
- [Estrutura do Repositório](#estrutura-do-repositório)
- [Como Utilizar](#como-utilizar)
- [Equipe](#equipe)
- [Referências](#referências)

---

## Introdução

O sistema simula o monitoramento de um estreito marítimo dividido em quatro setores geográficos. Cada setor opera de forma autônoma: possui um broker MQTT dedicado, um gerenciador responsável por detectar ocorrências e despachar drones, e dois sensores (radar e boia) que publicam leituras continuamente.

Os drones são um **recurso compartilhado** entre os quatro setores. Qualquer gerenciador pode despachar qualquer drone do sistema para atender uma ocorrência — desde que o drone esteja disponível e nenhum outro gerenciador esteja acessando o mesmo drone simultaneamente. Para garantir essa exclusão mútua de forma distribuída, o algoritmo de **Ricart-Agrawala** é executado diretamente entre os gerenciadores via TCP, sem intermediários.

O sistema não possui ponto único de falha: se um setor inteiro cair, os demais continuam operando normalmente.

---

## Tecnologias e Ferramentas Utilizadas

- **Python 3.9:** Linguagem principal de todos os componentes.
- **Socket (TCP/UDP):** Módulo nativo para toda comunicação de rede — sem bibliotecas MQTT externas.
- **Threading:** Módulo nativo para operações concorrentes dentro de cada componente.
- **Curses:** Módulo nativo para a interface TUI do monitor.
- **JSON:** Formato de serialização para todas as mensagens trocadas no sistema.
- **Docker / Docker Compose:** Isolamento e orquestração dos 24 containers do sistema.

---

## Funcionalidades

- **Broker MQTT customizado:** Implementação própria do protocolo MQTT com suporte a TCP e UDP simultâneos na mesma porta. Suporta QoS 0 e 1, wildcards (`+`, `#`) e retained messages.
- **Exclusão mútua distribuída:** Algoritmo de Ricart-Agrawala com prioridade por criticidade, timestamp de Lamport e sector_id como desempate.
- **Sensores via UDP:** Radares e boias publicam dados via UDP (fire-and-forget), com handshake TCP inicial para verificar disponibilidade do broker.
- **Drones com retained messages:** Cada drone publica seu status com `retain=True`, garantindo que gerenciadores recém-conectados recebam o estado atual imediatamente.
- **Tolerância a falhas:** Peers RA que não respondem em 6 segundos são tratados como falhos (reply implícito). Drones que falham em missão são detectados e a ocorrência é reenfileirada.
- **Monitor TUI:** Interface curses em tempo real mostrando status dos 4 setores, 8 drones, leituras dos 8 sensores e últimos eventos do sistema.
- **Sem ponto único de falha:** Cada setor tem broker independente. A queda de um setor não afeta os demais.

---

## Arquitetura do Sistema

```
         Ricart-Agrawala TCP :5001 (exclusão mútua)
    ┌────────┬────────┬────────┐
    │        │        │        │
  [SM1]───[SM2]───[SM3]───[SM4]     Gerenciadores de Setor
    │        │        │        │
  [B1]     [B2]     [B3]     [B4]   Brokers MQTT (TCP + UDP :1883)
 :1883    :1884    :1885    :1886
    │        │        │        │
  ┌─┴─┐   ┌─┴─┐   ┌─┴─┐   ┌─┴─┐
  Da  Db  Dc  Dd  De  Df  Dg  Dh   Drones (TCP MQTT)
  R1  Bu1 R2  Bu2 R3  Bu3 R4  Bu4  Sensores (UDP MQTT)

              [Monitor TUI]
           conecta aos 4 brokers
```

| Componente | Protocolo | Observação |
|---|---|---|
| Drone → Broker | TCP MQTT | Status publicado com `retain=True` |
| Sensor → Broker | UDP MQTT | Fire-and-forget, sem overhead de conexão |
| Gerenciador → Broker local | TCP MQTT | Publica ocorrências e despachos |
| Gerenciador → Todos os brokers | TCP MQTT | Monitora status de todos os 8 drones |
| Gerenciador ↔ Gerenciador | TCP direto :5001 | RA bypassa os brokers |
| Monitor → Todos os brokers | TCP MQTT | Leitura de todos os tópicos |

**Distribuição de drones por setor:**

| Setor | Broker | Drones | Sensores |
|---|---|---|---|
| S1 | broker_1 :1883 | drone_a, drone_b | radar_1, buoy_1 |
| S2 | broker_2 :1884 | drone_c, drone_d | radar_2, buoy_2 |
| S3 | broker_3 :1885 | drone_e, drone_f | radar_3, buoy_3 |
| S4 | broker_4 :1886 | drone_g, drone_h | radar_4, buoy_4 |

---

## Componentes

### Broker MQTT

Implementado em `broker/iot_broker.py`. Escuta **TCP e UDP na mesma porta** (1883). Conexões TCP tratam clientes persistentes (drones, gerenciadores, monitor); datagramas UDP recebem publicações de sensores e roteiam para os assinantes normalmente.

Suporta: `CONNECT/CONNACK`, `PUBLISH QoS 0/1`, `SUBSCRIBE/SUBACK`, `PINGREQ/PINGRESP`, retained messages, wildcards `+` e `#`.

### Gerenciador de Setor

Implementado em `sector_manager/sector_manager.py`. Responsável por:

1. **Detectar ocorrências** — geradas periodicamente ou disparadas por anomalias nos sensores.
2. **Enfileirar com prioridade** — heap por criticidade DESC → timestamp Lamport ASC → sector_id ASC.
3. **Executar Ricart-Agrawala** — negocia acesso exclusivo ao drone escolhido com os 3 outros gerenciadores.
4. **Despachar o drone** — publica no broker onde o drone está registrado.
5. **Gerenciar a missão** — monitora o drone; se falhar, reenfileira a ocorrência.

Conecta-se a **todos os 4 brokers** para monitorar o status de qualquer drone do sistema.

### Drone

Implementado em `drone/drone_agent.py`. Conecta ao broker do seu setor de origem via TCP MQTT. Publica o próprio status com `retain=True` nos estados: `available`, `busy`, `offline`. Aguarda comandos de despacho (`/dispatch`) e retorno (`/recall`).

### Sensores

Implementados em `sensors/radar/radar_sensor.py` e `sensors/buoy/buoy_sensor.py`. Cada sensor:

1. Realiza **handshake TCP** inicial para confirmar que o broker está disponível.
2. Publica leituras via **UDP** no loop principal (fire-and-forget).

**Radar:** mede contagem de embarcações, velocidade média (kn) e bearing (°). Detecta congestionamento e velocidade anômala.

**Bóia:** mede altura de ondas (m), corrente (kn), visibilidade (nmi) e temperatura da água (°C). Detecta condições adversas.

### Monitor TUI

Implementado em `monitor/monitor.py`. Interface curses com atualização a cada 400ms. Exibe:

- Status dos 4 setores (online/offline)
- Estado dos 8 drones com setor de origem
- Últimos 7 eventos (ocorrências e despachos)
- Última leitura de cada um dos 8 sensores

Uma thread independente por broker garante que a falha de conexão a um broker não congele a interface.

---

## Algoritmo de Exclusão Mútua: Ricart-Agrawala

O algoritmo garante que dois gerenciadores nunca despachem o mesmo drone simultaneamente.

**Funcionamento:**

1. O gerenciador que deseja usar um drone envia `REQUEST` broadcast para os outros 3, com `(timestamp_Lamport, criticidade, sector_id)`.
2. Cada peer responde imediatamente com `REPLY`, a menos que também esteja solicitando o **mesmo drone** com prioridade maior.
3. **Prioridade maior** = criticidade mais alta → se igual, timestamp menor → se igual, sector_id menor.
4. Quando recebe `REPLY` de todos os peers, o gerenciador adquire o recurso.
5. Ao finalizar a missão, envia `RELEASE` e entrega os `REPLY` adiados.

**Tolerância a falhas:** peers que não respondem em 6 segundos são contabilizados como se tivessem respondido — o sistema não trava na ausência de um setor.

**Relógio de Lamport:** incrementado a cada evento local; atualizado para `max(local, recebido) + 1` ao receber mensagens.

---

## Protocolo de Comunicação

O protocolo MQTT foi implementado do zero usando apenas sockets Python. Os pacotes seguem a especificação MQTT 3.1.1:

```
Byte fixo: [tipo (4 bits) | flags (4 bits)]
Remaining Length: codificação variável (1-4 bytes, bit 7 = continuação)
Payload: tópico (2 bytes comprimento + string) + dados
```

TCP e UDP coexistem na **mesma porta 1883**: são protocolos distintos no nível do SO, o broker abre um socket de cada tipo separadamente.

**Tópicos utilizados:**

| Tópico | Publicador | Assinantes |
|---|---|---|
| `strait/drones/{id}/status` | Drone | Todos os gerenciadores, Monitor |
| `strait/drones/{id}/dispatch` | Gerenciador | Drone alvo |
| `strait/drones/{id}/recall` | Gerenciador | Drone alvo |
| `strait/sector/{n}/occurrence` | Gerenciador | Monitor |
| `strait/sector/{n}/sensors/radar` | Sensor radar | Gerenciador do setor, Monitor |
| `strait/sector/{n}/sensors/buoy` | Sensor boia | Gerenciador do setor, Monitor |

---

## Estrutura do Repositório

```
P02_iot_concurrence/
├── broker/
│   ├── iot_broker.py        # Broker MQTT customizado (TCP + UDP)
│   └── Dockerfile
├── sector_manager/
│   ├── sector_manager.py    # Gerenciador + Ricart-Agrawala + Lamport
│   └── Dockerfile
├── drone/
│   ├── drone_agent.py       # Agente de drone
│   └── Dockerfile
├── sensors/
│   ├── radar/
│   │   ├── radar_sensor.py  # Sensor de radar (handshake TCP + UDP)
│   │   └── Dockerfile
│   └── buoy/
│       ├── buoy_sensor.py   # Sensor de boia (handshake TCP + UDP)
│       └── Dockerfile
├── monitor/
│   ├── monitor.py           # Monitor TUI (curses)
│   └── Dockerfile
└── docker-compose.yml       # Orquestração dos 24 containers
```

---

## Como Utilizar

**Pré-requisitos:** Docker e Docker Compose instalados.

```bash
git clone https://github.com/ymeira/P02_iot_concurrence.git
cd P02_iot_concurrence
docker compose up -d
docker compose run --rm -it monitor   # abre o TUI
```

Logs e encerramento:

```bash
docker logs -f setor_1_manager   # ou drone_a, radar_setor_2, etc.
docker compose down
```

---

### Deploy distribuído em máquinas físicas separadas

Cada componente roda em uma máquina diferente com `docker run`, usando os IPs reais da rede. As imagens estão disponíveis no Docker Hub em `yasmincsme/iot-estreito-*`.

Ordem de inicialização recomendada:

1. Brokers
2. Gerentes — aguardam os brokers estarem prontos
3. Drones e Sensores — em paralelo
4. Monitor — por último

---

## Equipe

- Yasmin Cordeiro Meira



## Referências

> - [1] Python Software Foundation. "socket — Low-level networking interface." Python 3 documentation. https://docs.python.org/3/library/socket.html
> - [2] Python Software Foundation. "threading — Thread-based parallelism." Python 3 documentation. https://docs.python.org/3/library/threading.html
> - [3] Python Software Foundation. "curses — Terminal handling for character-cell displays." Python 3 documentation. https://docs.python.org/3/library/curses.html
> - [4] OASIS Standard. "MQTT Version 3.1.1." OASIS, 2014. https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html
> - [5] Ricart, G.; Agrawala, A. K. "An optimal algorithm for mutual exclusion in computer networks." *Communications of the ACM*, v. 24, n. 1, p. 9–17, 1981.
> - [6] Lamport, L. "Time, clocks, and the ordering of events in a distributed system." *Communications of the ACM*, v. 21, n. 7, p. 558–565, 1978.
