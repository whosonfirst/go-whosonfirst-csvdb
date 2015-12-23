package csvdb

import (
	csv "github.com/whosonfirst/go-whosonfirst-csv"
	"fmt"
	"io"
)

type CSVDB struct {
     db map[string]*CSVDBIndex
}

type CSVDBIndex struct {
     index map[string][]*CSVDBRow
}

type CSVDBRow struct {
     row map[string]string
}

func NewCSVDB (csv_file string, to_index []string) (*CSVDB, error) {

     db := make(map[string]*CSVDBIndex)

     reader, err := csv.NewDictReader(csv_file)

     if err != nil {
     	return nil, err
     }

     for {
	row, err := reader.Read()

	if err == io.EOF {
		break
	}

	if err != nil {
	        // fmt.Printf("%v\n", err)
		continue
	}

	for _, k := range to_index {

	    value, ok := row[k]	  

	    if !ok {
	       continue
	    }

	    if value == "" {
	       continue
	    }

	    idx, ok := db[k]

	    if !ok {
	       idx = NewCSVDBIndex()
	       db[k] = idx
	    }

	    // fmt.Printf("add %s=%s (%v)\n", k, value, row)

	    idx.Add(value, row)
	}

     }

     return &CSVDB{db}, nil
}

func NewCSVDBIndex() *CSVDBIndex {
     idx := make(map[string][]*CSVDBRow)
     return &CSVDBIndex{idx}
}

func NewCSVDBRow(row map[string]string) *CSVDBRow {
     return &CSVDBRow{row}
}

/* CSVDB methods */

func (d *CSVDB) Indexes() int {

     count := 0

     for _ = range d.db {
     	 count += 1
     }

     return count
}

func (d *CSVDB) Keys() int {

     count := 0

     for i, _ := range d.db {
     	 count += d.db[i].Keys()
     }

     return count
}

func (d *CSVDB) Rows() int {

     count := 0

     for i, _ := range d.db {
     	 count += d.db[i].Rows()
     }

     return count
}

/* CSVDBIndex methods */

func (i *CSVDBIndex) Add(key string, row map[string]string) bool {

     possible, ok := i.index[key]

     if !ok {
     	possible = make([]*CSVDBRow, 0)
     }

     dbrow := NewCSVDBRow(row)
     possible = append(possible, dbrow)

     i.index[key] = possible

     return true
}

func (i *CSVDBIndex) Keys() int {

     count := 0

     for _ = range i.index {
     	 count += 1
     }

     return count
}

func (i *CSVDBIndex) Rows() int {

     count := 0

     for _, rows := range i.index {
     	 count += len(rows)
     }

     return count
}

/* CSVDBRow methods */