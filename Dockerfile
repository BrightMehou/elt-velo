FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_TOOL_BIN_DIR=/usr/local/bin

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT []

# Expose Streamlit (8501) et dbt docs (8080)
EXPOSE 8501 8080

ENTRYPOINT []

CMD bash -c "\
  python src/init_db.py && \
  dbt docs generate --project-dir src/elt --profiles-dir src/elt && \
  (streamlit run src/ui.py --server.port 8501 --server.address 0.0.0.0 &) && \
  (dbt docs serve --project-dir src/elt --profiles-dir src/elt --port 8080 --host 0.0.0.0)"


