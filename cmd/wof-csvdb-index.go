package main

import (
	csvdb "github.com/whosonfirst/go-whosonfirst-csvdb"
	"flag"
	"fmt"
)

func main () {

     flag.Parse()
     args := flag.Args()

     path := args[0]

     to_index := make([]string, 0)
     to_index = append(to_index, "wof:id")

     db, err := csvdb.NewCSVDB(path, to_index)

     if err != nil {
     	panic(err)
     }

     fmt.Printf("%v\n", db)
}
