.PHONY: test

include ../Makefile.common

# All tests are unit tests on miniredis: no Docker needed.
test:
	go test -count=1 -timeout 120s ./...
