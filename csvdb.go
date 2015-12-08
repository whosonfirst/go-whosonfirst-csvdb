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

type CSVDBRow struct {
     row map[string]string
}

func NewCSVDB (csv_file string, index []string) (*CSVDB, error) {

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

	for _, k := range index {

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
