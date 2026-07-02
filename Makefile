PYTHON ?= python3

.PHONY: help check

help:
	@echo "Cachy Router commands:"
	@echo "  make check  - run offline syntax/contract checks"

check:
	$(PYTHON) -B -c 'import ast, pathlib; [ast.parse(p.read_text(encoding="utf-8"), filename=str(p)) for p in pathlib.Path("scripts").glob("*.py")]'
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/validate_cache_router_contracts.py --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_offline_prototype.py --json
