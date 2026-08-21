VERSION=`git describe --tags`
BUILD=`date +%FT%T%z`
HASH=`git rev-parse --short HEAD`


LDFLAGS=-ldflags "-w -s -X main.version=${VERSION} -X main.build=${BUILD} -X main.gitCommit=${HASH}"

all: test build

build:
	go build -o vte2 ${LDFLAGS} ./cmd/vte
	go build -o ltgen ${LDFLAGS} ./cmd/ltgen
	go build -o ptextract ${LDFLAGS} ./cmd/ptextract

install:
	go install -o vte2 ${LDFLAGS} ./cmd/vte
	go install -o ltgen ${LDFLAGS} ./cmd/ltgen
	go install -o ptextract ${LDFLAGS} ./cmd/ptextract

clean:
	rm klogproc

test:
	go test ./...

.PHONY: clean install test
