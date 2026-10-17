# Runs a target in every module (each subdirectory has its own go.mod and Makefile).
#
#   make test       all tests; the integration tests need Docker
#   make test-unit  tests that need no Docker
#   make lint       golangci-lint
#   make vuln       govulncheck
#   make check      tidy, vet, race tests, lint and vuln: what CI checks without Docker
MODULES := db cache kafka rabbitmq logger oauth2 i18n cryptoutil
# Modules with a Docker-free test-unit target.
UNIT_MODULES := db cache kafka rabbitmq oauth2

.PHONY: test test-unit lint vuln check

test:
	@for m in $(MODULES); do echo "==> $$m"; $(MAKE) --no-print-directory -C $$m test || exit 1; done

test-unit:
	@for m in $(UNIT_MODULES); do echo "==> $$m"; $(MAKE) --no-print-directory -C $$m test-unit || exit 1; done

lint:
	@for m in $(MODULES); do echo "==> $$m"; $(MAKE) --no-print-directory -C $$m lint || exit 1; done

vuln:
	@for m in $(MODULES); do echo "==> $$m"; $(MAKE) --no-print-directory -C $$m vuln || exit 1; done

check:
	@for m in $(MODULES); do echo "==> $$m"; $(MAKE) --no-print-directory -C $$m check || exit 1; done
