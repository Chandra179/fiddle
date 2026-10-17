.PHONY: test test-unit

include ../Makefile.common

# Everything, including the integration tests (needs Docker: testcontainers).
test:
	go test -tags integration -v -count=1 -timeout 300s ./...

# Unit tests only: no Docker, the integration tests are excluded by build tag.
test-unit:
	go test -count=1 -timeout 120s ./...
