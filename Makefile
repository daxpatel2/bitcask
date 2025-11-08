BINARY_NAME=hello-world
.DEFAULT_GOAL := run
#
#build:
#	gox -osarch="darwin/amd64 linux/amd64 windows/amd64" -output="target/{{.Dir}}_{{.OS}}_{{.Arch}}" ./cmd/bitcaskctl
#
run:
	# 10 seconds, 8 workers, 70/25/5 read/write/del mix, 256B values

    # fixed operation count per worker instead of duration
    go run ./cmd/bitcaskctl -ops 20000 -concurrency 4

    # run compaction every 5s to see the effect on throughput
    go run ./cmd/bitcaskctl -duration 30s -compactEvery 5s

    # with profiles
    go run ./cmd/bitcaskctl -duration 15s -cpuprofile cpu.pprof -memprofile mem.pprof


lint:
	golangci-lint fmt
	golangci-lint run

clean:
	go clean
	#rm .\target\bitcaskctl_windows_amd64.exe
	rm .\target\bitcaskctl_darwin_amd64.exe
	#rm .\target\bitcaskctl_linux_amd64.exe