# export GOPATH=$(readlink -f ../../../..)

all: build
build: rroxy 
.PHONY: rroxy test format

test:
	go test -v ./...

format:
	goimports -w=true *.go

rroxy:
	go build -v -ldflags "-X main.Rev=`git rev-parse --short HEAD`"
