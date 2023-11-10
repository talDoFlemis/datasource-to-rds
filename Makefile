SOURCEFILES=$(wildcard internal/*.go)

lambda: $(SOURCEFILES) cmd/lambda/main.go
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bootstrap -tags lambda.norpc ./cmd/lambda/main.go

zip: lambda
	zip -j bootstrap.zip bootstrap

local: $(SOURCEFILES) cmd/local/main.go
	GOOS=linux CGO_ENABLED=0 go build -o main ./cmd/local/main.go
