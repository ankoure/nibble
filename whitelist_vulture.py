# vulture whitelist - names that appear unused but are intentionally kept
#
# Protocol / framework requirements
_.app  # asynccontextmanager lifespan receives the FastAPI app instance
_.request  # sse-starlette uses the Request for disconnect detection

# CLI entry points (registered in pyproject.toml [project.scripts])
_.print_openapi
