BINARY_NAME=hello-world
.DEFAULT_GOAL := run

build:
	gox -osarch="darwin/amd64 linux/amd64 windows/amd64" -output="target/{{.Dir}}_{{.OS}}_{{.Arch}}" ./cmd/bitcaskctl

run: build
	./target/bitcaskctl_windows_amd64.exe


lint:
	golangci-lint run --enable-all

clean:
	go clean
	rm .\target\bitcaskctl_windows_amd64.exe
	rm .\target\bitcaskctl_darwin_amd64.exe
	rm .\target\bitcaskctl_linux_amd64.exe