prep:
	if test -d pkg; then rm -rf pkg; fi

self:   prep
	if test -d src/github.com/whosonfirst/go-whosonfirst-csvdb; then rm -rf src/github.com/whosonfirst/go-whosonfirst-csvdb; fi
	mkdir -p src/github.com/whosonfirst/go-whosonfirst-csvdb
	cp csvdb.go src/github.com/whosonfirst/go-whosonfirst-csvdb/

deps:   
	go get -u "github.com/whosonfirst/go-whosonfirst-csv"

fmt:
	go fmt *.go
	go fmt cmd/*.go

bin: 	self
	go build -o bin/wof-csvdb-index cmd/wof-csvdb-index.go
