-include .env
export

.PHONY: fix-gtfs lint typecheck

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
