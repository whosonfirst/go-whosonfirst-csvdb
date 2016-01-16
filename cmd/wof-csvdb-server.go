package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-csvdb"
	"github.com/whosonfirst/go-whosonfirst-log"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {

	var cols = flag.String("columns", "", "Comma-separated list of columns to index")
	var host = flag.String("host", "localhost", "The hostname to listen for requests on")
	var port = flag.Int("port", 8228, "The port number to listen for requests on")
	var cors = flag.Bool("cors", false, "Enable CORS headers")
	var loglevel = flag.String("loglevel", "info", "Log level for reporting")

	flag.Parse()
	args := flag.Args()

	to_index := make([]string, 0)

	for _, c := range strings.Split(*cols, ",") {
		to_index = append(to_index, c)
	}

	l_writer := io.MultiWriter(os.Stdout)

	logger := log.NewWOFLogger("[wof-csvdb-index] ")
	logger.AddLogger(l_writer, *loglevel)

	db, err := csvdb.NewCSVDB(logger)

	if err != nil {
		panic(err)
	}

	for _, path := range args {

		t1 := time.Now()

		err := db.IndexCSVFile(path, to_index)

		if err != nil {
			panic(err)
		}

		t2 := time.Since(t1)
		fmt.Printf("time to index %s: %v\n", path, t2)
	}

	handler := func(rsp http.ResponseWriter, req *http.Request) {

		query := req.URL.Query()

		k := query.Get("k")
		v := query.Get("v")

		if k == "" {
			http.Error(rsp, "Missing k parameter", http.StatusBadRequest)
			return
		}

		if v == "" {
			http.Error(rsp, "Missing v parameter", http.StatusBadRequest)
			return
		}

		rows, err := db.Where(k, v)

		if err != nil {
			http.Error(rsp, err.Error(), http.StatusInternalServerError)
			return
		}

		results := make([]map[string]string, 0)

		for _, row := range rows {
			results = append(results, row.AsMap())
		}

		js, err := json.Marshal(results)

		if err != nil {
			http.Error(rsp, err.Error(), http.StatusInternalServerError)
			return
		}

		// maybe this although it seems like it adds functionality for a lot of
		// features this server does not need - https://github.com/rs/cors
		// (20151022/thisisaaronland)

		if *cors {
			rsp.Header().Set("Access-Control-Allow-Origin", "*")
		}

		rsp.Header().Set("Content-Type", "application/json")
		rsp.Write(js)
	}

	endpoint := fmt.Sprintf("%s:%d", *host, *port)

	fmt.Printf("wof-csvdb-server running at %s\n", endpoint)

	http.HandleFunc("/", handler)
	http.ListenAndServe(endpoint, nil)
}
