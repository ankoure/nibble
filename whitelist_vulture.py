# vulture whitelist - names that appear unused but are intentionally kept
#
# Protobuf generated files - imports register descriptor dependencies implicitly
gtfs__realtime__pb2  # noqa: F401 - required by nyct_subway_pb2 descriptor

# Protocol / framework requirements
_.app  # asynccontextmanager lifespan receives the FastAPI app instance
_.request  # sse-starlette uses the Request for disconnect detection

# CLI entry points (registered in pyproject.toml [project.scripts])
_.print_openapi
