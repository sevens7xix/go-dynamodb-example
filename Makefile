.DEFAULT_GOAL := run

fmt: 
	go fmt ./...
.PHONY:fmt

vet: fmt
	go vet ./...
.PHONY:vet

test: vet
	go test ./... -coverprofile cover.out
.PHONY:test

cover: test
	go tool cover --html=cover.out
.PHONY:cover

run: vet
	go run ./...
.PHONY:run