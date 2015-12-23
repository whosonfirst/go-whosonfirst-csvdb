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

     fmt.Printf("indexes: %d keys: %d rows: %d time to index: %v\n", db.Indexes(), db.Rows(), db.Keys(), t2)

     rows, _ := db.Where("gp:id", "3534") 

     fmt.Printf("where gp:id= 3534 %d\n", len(rows))

     for i, row := range rows {

     	 fmt.Printf("looping over result #%d\n", i+1)

	     for k, v := range row.AsMap() {
	     	 r, _ := db.Where(k, v)
	     	 fmt.Printf("where %s=%s %d\n", k, v, len(r))
	     }

     }

}
