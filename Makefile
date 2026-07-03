PYTHON ?= python3

.PHONY: help check check-clean-checkout check-scheduler-stress

help:
	@echo "Cachy Router commands:"
	@echo "  make check  - run offline syntax/contract checks"
	@echo "  make check-scheduler-stress  - run the 600-second offline scheduler stress gate"
	@echo "  make check-clean-checkout  - prove make check from a clean committed checkout"

check:
	$(PYTHON) -B -c 'import ast, pathlib; [ast.parse(p.read_text(encoding="utf-8"), filename=str(p)) for p in pathlib.Path("scripts").glob("*.py")]'
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/validate_cache_router_contracts.py --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/replay_cache_router_decisions.py --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_offline_prototype.py --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_setup_doctor.py --workers-file configs/cache-router/workers.example.json --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_setup_doctor_matrix_test.py
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_daemon_smoke_test.py
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_sidecar_smoke_test.py
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_transport.py --self-test
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_remote_stack_smoke_test.py
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_store_audit.py --self-test --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_performance_probe.py --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_correctness_probe.py --self-test --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_long_soak_probe.py --self-test --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_live_endpoint_matrix.py --self-test --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_suffix_benchmark_gate.py --self-test --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_agent_loop.py --self-test --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_public_hygiene_scan.py --self-test --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/validate_cache_router_setup_docs.py
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/validate_cache_router_endpoint_docs.py
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/validate_cache_router_claim_map.py --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/validate_cache_router_architecture_doc.py
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/validate_acceptance_metrics.py --json
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_release_gap_report.py --summary

check-clean-checkout:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/validate_clean_checkout_check.py

check-scheduler-stress:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/cache_router_scheduler_stress_probe.py --json
