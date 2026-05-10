# syntax=docker/dockerfile:1.7
# mctrader-data Pilot — Docker-first containerization (CFP-128 / ADR-033)
# 2-stage build: deps (uv install) → runner (slim + non-root)

#─── Stage 1: deps ───
FROM python:3.12-slim AS deps

# Install uv (pinned for reproducibility)
# hadolint ignore=DL3013
RUN pip install --no-cache-dir uv==0.5.11

WORKDIR /build
# vendor/ contains pre-built wheels for private git+https deps (mctrader-market family)
COPY vendor/ ./vendor/
COPY pyproject.toml README.md ./
COPY src/ ./src/

# 1) Install PyPI deps of vendor packages (websockets etc.) — no git needed.
# 2) Install vendor wheels (mctrader-market family) without dep checking.
# 3) Strip git+https entries from pyproject.toml so uv resolves only PyPI deps.
# 4) Install mctrader-data + remaining PyPI deps via uv.
RUN uv pip install --system --no-cache "websockets>=12,<14" "pydantic>=2,<3" \
    && pip install --no-cache-dir --no-deps ./vendor/*.whl \
    && sed -i '/mctrader-market.*git+/d' pyproject.toml \
    && uv pip install --system --no-cache .

#─── Stage 2: runner ───
FROM python:3.12-slim AS runner

# Non-root user (UID 1001)
RUN useradd --system --uid 1001 --no-create-home --shell /usr/sbin/nologin mctrader \
    && mkdir -p /var/lib/mctrader/data \
    && chown -R mctrader:mctrader /var/lib/mctrader

# Copy installed packages from deps stage
COPY --from=deps /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

ENV MCTRADER_DATA_ROOT=/var/lib/mctrader/data \
    MCTRADER_HEALTH_PORT=8080 \
    PYTHONUNBUFFERED=1

USER mctrader
WORKDIR /var/lib/mctrader

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=60s \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/health').status==200 else 1)"]

ENTRYPOINT ["python", "-m", "mctrader_data.cli"]
CMD ["collect", "--top-n", "10", "--include", "transactions,orderbook", "--log-level", "INFO"]
