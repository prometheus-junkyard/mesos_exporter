all: get-deps build

build:
	@mkdir -p bin/
	@go build -o bin/mesos_exporter ./...

get-deps:
	@go get -d -v ./...

test:
	@go test -v ./...

format:
	@go fmt ./...

clean:
	@rm -rf bin/

docker: get-deps build
	@docker build -t mesos_exporter .
