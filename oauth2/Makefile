.PHONY: test test-unit

include ../Makefile.common

# Includes the integration tests, which use an in-process mock OIDC server (no Docker).
test:
	go test -tags integration -v -count=1 -timeout 300s ./...

# Unit tests only: the mock-OIDC integration tests are excluded by build tag.
test-unit:
	go test -count=1 -timeout 120s ./...
