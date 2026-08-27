FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip wheel -r requirements.txt -w /wheels

FROM python:3.12-slim AS production

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN groupadd --gid 1000 deployer \
    && useradd --uid 1000 --gid deployer --shell /bin/bash --create-home deployer

WORKDIR /app

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r /build/requirements.txt \
    && rm -rf /wheels

COPY pyproject.toml README.md ./
COPY stock_trading ./stock_trading
COPY configs ./configs

RUN chown -R deployer:deployer /app
USER deployer

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

CMD ["python", "-m", "stock_trading"]
