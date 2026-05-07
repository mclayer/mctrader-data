# syntax=docker/dockerfile:1.7
# mctrader-data Pilot — Docker-first containerization (CFP-128 / ADR-033)
# 2-stage build: deps (uv install) → runner (slim + non-root)

#─── Stage 1: deps ───
FROM python:3.12-slim AS deps

# Install uv (pinned for reproducibility)
# hadolint ignore=DL3013
RUN pip install --no-cache-dir uv==0.5.11

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src/ ./src/

# uv pip install --system --no-cache (resolves git+https deps for mctrader-market / -bithumb)
# git is required for git+https sources, then purged in this stage (deps stage discarded later).
RUN apt-get update \
    && apt-get install --no-install-recommends -y git=1:* \
    && uv pip install --system --no-cache . \
    && apt-get purge -y git \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /root/.cache

#─── Stage 2: runner ───
FROM python:3.12-slim AS runner

# Non-root user (UID 1001)
RUN useradd --system --uid 1001 --no-create-home --shell /usr/sbin/nologin mctrader \
    && mkdir -p /var/lib/mctrader/data \
    && chown -R mctrader:mctrader /var/lib/mctrader

# Copy installed packages + entry script from deps stage
COPY --from=deps /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=deps /usr/local/bin/mctrader-data /usr/local/bin/mctrader-data

ENV MCTRADER_DATA_ROOT=/var/lib/mctrader/data \
    MCTRADER_HEALTH_PORT=8080 \
    PYTHONUNBUFFERED=1

USER mctrader
WORKDIR /var/lib/mctrader

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=60s \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/health').status==200 else 1)"]

ENTRYPOINT ["mctrader-data"]
CMD ["collect", "--top-n", "10", "--include", "transactions,orderbook", "--log-level", "INFO"]
