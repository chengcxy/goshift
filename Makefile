version := $(shell /bin/date "+%Y-%m-%d %H:%M")
linux:fmt
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -ldflags="-X 'main.BuildTime=$(version)'" -o ./cmd/goshift ./cmd/main.go
mac:fmt
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -ldflags="-X 'main.BuildTime=$(version)'" -o ./cmd/goshift ./cmd/main.go
	upx -9 --force-macos ./cmd/goshift
fmt:
	gofmt -l -w cmd
	gofmt -l -w configor
	gofmt -l -w logger
	gofmt -l -w plugin
	gofmt -l -w scheduler
	gofmt -l -w utils
	gofmt -l -w utils

