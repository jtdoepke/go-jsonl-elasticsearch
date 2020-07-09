cli:
	go build -mod vendor -o bin/dump cmd/dump/main.go
	go build -mod vendor -o bin/restore cmd/restore/main.go
