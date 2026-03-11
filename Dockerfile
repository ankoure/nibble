FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

WORKDIR /app

# Install dependencies (cached layer separate from source)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-group dev

# Install the project itself
COPY nibble/ nibble/
COPY pyproject.toml .
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --frozen --no-group dev


FROM python:3.12-slim

RUN useradd --create-home --shell /bin/bash nibble
WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY nibble/ nibble/

ENV PATH="/app/.venv/bin:$PATH"
ENV NIBBLE_HOST=0.0.0.0
ENV NIBBLE_PORT=8080

USER nibble
EXPOSE 8080

CMD ["python", "-m", "nibble.server"]
