package main

import (
	"bufio"
	"flag"
	"fmt"
	csvdb "github.com/whosonfirst/go-whosonfirst-csvdb"
	"os"
	"strings"
	"time"
)

func main() {

	var cols = flag.String("columns", "", "Comma-separated list of columns to index")

	flag.Parse()
	args := flag.Args()

	path := args[0]

	to_index := make([]string, 0)

	for _, c := range strings.Split(*cols, ",") {
		to_index = append(to_index, c)
	}

	t1 := time.Now()

	db, err := csvdb.NewCSVDB(path, to_index)

	if err != nil {
		panic(err)
	}

	t2 := time.Since(t1)

	fmt.Printf("indexes: %d keys: %d rows: %d time to index: %v\n", db.Indexes(), db.Rows(), db.Keys(), t2)

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("query <col>=<id>")
	fmt.Printf("> ")

	for scanner.Scan() {

		input := scanner.Text()
		fmt.Println(input)

		query := strings.Split(input, "=")

		if len(query) != 2 {
			fmt.Println("Invalid query")
			continue
		}

		k := query[0]
		v := query[1]

		fmt.Printf("search for %s=%s\n", k, v)

		rows, _ := db.Where(k, v)

		fmt.Printf("where %s=%s %d\n", k, v, len(rows))

		for i, row := range rows {

			fmt.Printf("looping over result #%d\n", i+1)

			for k, v = range row.AsMap() {
				r, _ := db.Where(k, v)
				fmt.Printf("where %s=%s %d\n", k, v, len(r))
			}

		}

		fmt.Println("")
		fmt.Println("query <col>=<id>")
		fmt.Printf("> ")
	}
}
