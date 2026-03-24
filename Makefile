-include .env
export

.PHONY: fix-gtfs lint typecheck openapi generate-protos

generate-protos: ## Compile all .proto files in protos/ into nibble/protos/
	uv run python -m grpc_tools.protoc \
		-I protos \
		--python_out=nibble/protos \
		gtfs-realtime.proto \
		nyct/nyct-subway.proto
	@# Fix import in generated NYCT file: protoc emits a bare module name which
	@# won't resolve from a subpackage; rewrite to a fully-qualified package path.
	sed -i 's/^import gtfs_realtime_pb2/from nibble.protos import gtfs_realtime_pb2/' \
		nibble/protos/nyct/nyct_subway_pb2.py

openapi: ## Regenerate openapi.json from current server routes
	uv run nibble-openapi > openapi.json

lint: ## Run ruff and vulture
	uv run ruff check nibble/ tests/
	uv run vulture nibble/ whitelist_vulture.py --min-confidence 80

typecheck: ## Run mypy
	uv run mypy nibble/

fix-gtfs: ## Download and fix the GTFS static bundle, writing fixed.zip
	python3 -c "\
import httpx, os, sys; \
from nibble.gtfs.fixer import fix_gtfs_zip; \
url = os.environ.get('NIBBLE_GTFS_STATIC_URL') or sys.exit('NIBBLE_GTFS_STATIC_URL not set'); \
print(f'Downloading {url}'); \
content = httpx.get(url, follow_redirects=True, timeout=60).content; \
fixed = fix_gtfs_zip(content); \
open('fixed.zip', 'wb').write(fixed); \
print(f'Written fixed.zip ({len(fixed):,} bytes)')"
