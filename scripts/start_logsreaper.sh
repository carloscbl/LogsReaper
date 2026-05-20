#!/usr/bin/env bash
# start_logsreaper.sh — levanta (o reemplaza) el container `logs-reaper`
# en background, atado a la red de docker, escuchando logs de `sm-*-1`
# y exponiendo el dashboard de stats en LOGS_REAPER_PORT (default 9110).
#
# Idempotente: si ya existe un container con ese nombre lo recrea limpio.
#
# Uso:
#   ./start_logsreaper.sh                       # services=all, duration sin límite
#   ./start_logsreaper.sh --duration 1800       # 30 minutos
#   ./start_logsreaper.sh --services accounts,gateway-isp
#   ./start_logsreaper.sh --port 9105
#
# Variables de entorno opcionales:
#   LOGS_REAPER_IMAGE  (default logs-reaper:dev)
#   LOGS_REAPER_PORT   (default 9110)
#   LOGS_REAPER_NAME   (default logs-reaper)
#   LOGS_REAPER_OUT    (default ./out_ci)

set -euo pipefail
cd "$(dirname "$0")/.."  # .
REAPER_DIR="$(pwd)"
REPO_ROOT="$(cd ../.. && pwd)"

IMAGE="${LOGS_REAPER_IMAGE:-logs-reaper:dev}"
NAME="${LOGS_REAPER_NAME:-logs-reaper}"
PORT="${LOGS_REAPER_PORT:-9110}"
OUT_DIR="${LOGS_REAPER_OUT:-$REAPER_DIR/out_ci}"
BASELINES_DIR="$REAPER_DIR/baselines"

SERVICES="all"
DURATION=""
KEEP_ALIVE=1  # default: dashboard sigue vivo aunque acabe la captura

while [[ $# -gt 0 ]]; do
    case "$1" in
        --services)   SERVICES="$2"; shift 2 ;;
        --duration)   DURATION="$2"; shift 2 ;;
        --port)       PORT="$2"; shift 2 ;;
        --name)       NAME="$2"; shift 2 ;;
        --image)      IMAGE="$2"; shift 2 ;;
        --no-keep-alive) KEEP_ALIVE=0; shift ;;
        -h|--help)
            sed -n '2,20p' "$0"
            exit 0
            ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

# Stop & remove any previous instance with the same name.
if docker ps -a --format '{{.Names}}' | grep -qx "$NAME"; then
    echo "[start_logsreaper] removing existing container $NAME..."
    docker rm -f "$NAME" >/dev/null
fi

# Ensure the image exists locally; build if missing.
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "[start_logsreaper] image $IMAGE not found locally — building..."
    docker build -t "$IMAGE" -f "$REAPER_DIR/Dockerfile" "$REAPER_DIR"
fi

mkdir -p "$OUT_DIR"
mkdir -p "$BASELINES_DIR"

# Build the command we hand to the container.
# Modo eterno por defecto: sin --duration, collect corre hasta SIGTERM.
# El usuario puede acotarlo con `--duration N` si quiere.
DURATION_ARG="${DURATION:-}"
# Capturar histórico al arrancar: los servicios que ya estaban corriendo
# (kafka, identity, mongodb...) tienen sus logs de boot/idle disponibles
# para que se genere baseline aunque estén callados ahora mismo.
INITIAL_TAIL="${INITIAL_TAIL:-2000}"

KEEP_ALIVE_FLAG=""
if [[ "$KEEP_ALIVE" != "1" ]]; then
    KEEP_ALIVE_FLAG="--no-keep-alive"
fi

# --- Desktop notifications (libnotify / DBus) -------------------------------
# Para que `notify-send` dentro del contenedor llegue al popup del usuario,
# montamos el bus de sesión y hacemos correr al contenedor con UID/GID del
# host (la auth de dbus-daemon usa SO_PEERCRED — sin match de UID, rechaza).
# Si XDG_RUNTIME_DIR no está o no hay bus accesible (CI/headless), el flag
# LOGSREAPER_NOTIFY queda en stderr-fallback automáticamente.
HOST_UID="$(id -u)"
HOST_GID="$(id -g)"
XDG_RT="${XDG_RUNTIME_DIR:-/run/user/${HOST_UID}}"
DBUS_BUS_PATH="${XDG_RT}/bus"
DBUS_ARGS=()
USER_ARGS=()
if [[ -S "$DBUS_BUS_PATH" ]]; then
    DBUS_ARGS+=(
        -v "${DBUS_BUS_PATH}:${DBUS_BUS_PATH}"
        -e "XDG_RUNTIME_DIR=${XDG_RT}"
        -e "DBUS_SESSION_BUS_ADDRESS=unix:path=${DBUS_BUS_PATH}"
        -e "LOGSREAPER_NOTIFY=1"
        -e "LOGSREAPER_OUT_HOST_PATH=${OUT_DIR}"
        -e "LOGSREAPER_DASHBOARD_URL=http://localhost:${PORT}/"
    )
    # Match de UID/GID y entrada al grupo docker para poder leer el socket.
    # HOME=/tmp porque al no ser root el contenedor no tiene /root y streamlit
    # quiere un dir escribible para su config.
    USER_ARGS+=(--user "${HOST_UID}:${HOST_GID}" -e "HOME=/tmp")
    if DOCKER_GID="$(getent group docker | cut -d: -f3)" && [[ -n "$DOCKER_GID" ]]; then
        USER_ARGS+=(--group-add "$DOCKER_GID")
    fi
    echo "[start_logsreaper] desktop notifications enabled (dbus=${DBUS_BUS_PATH})"
else
    echo "[start_logsreaper] no DBus session bus at ${DBUS_BUS_PATH} — notifications will fall back to stderr"
    # Sin DBus tampoco forzamos --user (rompería volúmenes ya creados como root).
    DBUS_ARGS+=(-e "LOGSREAPER_NOTIFY=0" -e "LOGSREAPER_OUT_HOST_PATH=${OUT_DIR}")
fi

DURATION_FLAG=()
if [[ -n "$DURATION_ARG" ]]; then
    DURATION_FLAG=(--duration "$DURATION_ARG")
fi

echo "[start_logsreaper] launching $NAME (image=$IMAGE port=$PORT services=$SERVICES duration=${DURATION_ARG:-∞} keep_alive=${KEEP_ALIVE})"

# Sin --restart: cuando el operador haga `docker stop logs-reaper` el container
# desaparece limpio. El subcomando `live` orquesta `collect` (en bg) +
# Streamlit (en fg, puerto $PORT). Streamlit muestra TODAS las tabs
# históricas + la nueva tab "Live Ingest" leyendo stats_snapshot.json.
docker run -d \
    --name "$NAME" \
    --network sm_internalNetwork \
    -p "${PORT}:${PORT}" \
    -v /var/run/docker.sock:/var/run/docker.sock:ro \
    -v "$REPO_ROOT:/workspace:rw" \
    -v "$OUT_DIR:/work/out:rw" \
    -v "$BASELINES_DIR:/work/baselines:rw" \
    -e LOGS_REAPER_OUT=/work/out \
    -e LOGS_REAPER_BASELINES=/work/baselines \
    -e LOGS_REAPER_REGISTRY=/work/out/runs \
    "${DBUS_ARGS[@]}" \
    "${USER_ARGS[@]}" \
    --label "io.logs-reaper.role=live" \
    --stop-signal SIGTERM \
    --stop-timeout 15 \
    "$IMAGE" \
        live \
            --services "$SERVICES" \
            --out "/work/out" \
            "${DURATION_FLAG[@]}" \
            --streamlit-port "$PORT" \
            --streamlit-host 0.0.0.0 \
            --baselines-dir /work/baselines \
            --auto-index-interval 5 \
            --auto-index-min-green-runs 1 \
            --initial-tail "$INITIAL_TAIL" \
            $KEEP_ALIVE_FLAG >/dev/null

# Streamlit tarda ~5s en arrancar; le damos hasta 60s.
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    # Streamlit responde 200 a / cuando está listo.
    if curl -sf -m 2 "http://localhost:${PORT}/" -o /dev/null 2>&1; then
        echo "[start_logsreaper] ready — dashboard: http://localhost:${PORT}/"
        echo "[start_logsreaper] tab Live Ingest se actualiza cada 2s vía stats_snapshot.json"
        echo "[start_logsreaper] follow logs:  docker logs -f $NAME"
        exit 0
    fi
    sleep 1
done

echo "[start_logsreaper] WARNING: dashboard did not come up in 60s. Container logs:"
docker logs --tail 40 "$NAME" || true
exit 1
