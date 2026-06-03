.PHONY: check test clean structural-tests

check test:
	bash scripts/check.sh

clean:
	bash scripts/clean_artifacts.sh

structural-tests:
	bash scripts/audit_structural_tests.sh
