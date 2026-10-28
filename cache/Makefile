.PHONY: test test-unit

include ../Makefile.common

# All tests are unit tests on miniredis: no Docker needed.
test test-unit:
	go test -count=1 -timeout 120s ./...
