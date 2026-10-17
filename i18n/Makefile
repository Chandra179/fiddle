.PHONY: test

include ../Makefile.common

# i18n tests are file-based (no Docker).
test:
	go test -tags integration -v -count=1 -timeout 300s ./...
