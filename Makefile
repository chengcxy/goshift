linux:fmt
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./cmd/goshift ./cmd/main.go
mac:fmt
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ./cmd/goshift ./cmd/main.go
fmt:
	gofmt -l -w cmd
	gofmt -l -w configor
	gofmt -l -w logger
	gofmt -l -w plugin
	gofmt -l -w scheduler
	gofmt -l -w utils
	gofmt -l -w utils
