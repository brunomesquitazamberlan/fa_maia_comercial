FROM python:3.13-slim

# Install Chrome and ChromeDriver dependencies
RUN apt-get update && apt-get install --no-install-recommends -y \
    wget \
    gnupg2 \
    curl \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv para o PATH correto
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy and install application
COPY . /app
WORKDIR /app


# Sincronizar dependências
RUN uv sync --frozen --no-cache

CMD ["/app/.venv/bin/uvicorn", "main:app", "--port", "8025", "--host", "0.0.0.0", "--loop", "uvloop", "--http", "httptools", "--limit-concurrency", "1000"]