# Runs a target in every module (each subdirectory has its own go.mod and Makefile).
#
#   make test       all tests
#   make lint       golangci-lint
#   make vuln       govulncheck
#   make check      tidy, vet, race tests, lint and vuln: what CI checks
MODULES := db cache kafka rabbitmq logger oauth2 i18n cryptoutil

.PHONY: test lint vuln check

test:
	@for m in $(MODULES); do echo "==> $$m"; $(MAKE) --no-print-directory -C $$m test || exit 1; done

lint:
	@for m in $(MODULES); do echo "==> $$m"; $(MAKE) --no-print-directory -C $$m lint || exit 1; done

vuln:
	@for m in $(MODULES); do echo "==> $$m"; $(MAKE) --no-print-directory -C $$m vuln || exit 1; done

check:
	@for m in $(MODULES); do echo "==> $$m"; $(MAKE) --no-print-directory -C $$m check || exit 1; done
