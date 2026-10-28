.PHONY: test

include ../Makefile.common

# Unit tests only: no Docker needed.
test:
	go test -count=1 -timeout 120s ./...
