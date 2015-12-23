package main

import (
	csvdb "github.com/whosonfirst/go-whosonfirst-csvdb"
	"flag"
	"fmt"
	"time"
)

func main () {

     flag.Parse()
     args := flag.Args()

     path := args[0]

     to_index := make([]string, 0)
     to_index = append(to_index, "wof:id")
     to_index = append(to_index, "gp:id")
     to_index = append(to_index, "gn:id")

     t1 := time.Now()

     db, err := csvdb.NewCSVDB(path, to_index)

     if err != nil {
     	panic(err)
     }

     t2 := time.Since(t1)

     fmt.Printf("indexes: %d keys: %d rows: %d time to index: %v", db.Indexes(), db.Rows(), db.Keys(), t2)

     // db.Where
     
}
