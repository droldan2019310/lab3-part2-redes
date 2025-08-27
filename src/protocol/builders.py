# ── Redis connection ─────────────────────────────────────────────
REDIS_HOST=lab3.redesuvg.cloud
REDIS_PORT=6379
REDIS_PWD=UVGRedis2025

# ── Identidad y topología ───────────────────────────────────────
SECTION=sec10
TOPO=topo1         # identificador corto de la topología (para formar el canal)
NODE=A             # cambia esto por el id del nodo que estés levantando

# ── Rutas a configs ─────────────────────────────────────────────
NAMES_PATH=./configs/names.json
TOPO_PATH=./configs/topo.json

# ── Protocolo/Timers por defecto ────────────────────────────────
PROTO=lsr
HELLO_INTERVAL_SEC=5
INFO_INTERVAL_SEC=12
TTL_DEFAULT=5

# ── Logging ────────────────────────────────────────────────────
LOG_LEVEL=INFO
