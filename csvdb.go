package csvdb

import (
	"errors"
	_ "fmt"
	csv "github.com/whosonfirst/go-whosonfirst-csv"
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

func NewCSVDB(csv_file string, to_index []string) (*CSVDB, error) {

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

			pruned := make(map[string]string)

			for k, v := range row {

				if v == "" {
					continue
				}

				pruned[k] = v
			}

			idx, ok := db[k]

			if !ok {
				idx = NewCSVDBIndex()
				db[k] = idx
			}
			
			/* 
			   TO DO: ONLY STORE pruned ONCE AND THEN STORE A POINTER
			   TO IT FROM INDIVIDUAL INDEXES (20151222/thisisaaronland)
			*/

			idx.Add(value, pruned)
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

func (d *CSVDB) Where(key string, id string) ([]*CSVDBRow, error) {

	rows := make([]*CSVDBRow, 0)

	idx, ok := d.db[key]

	if !ok {
		return rows, errors.New("Unknown index")
	}

	rows, ok = idx.index[id] // PLEASE MAKE ME A FUNCTION OR SOMETHING

	if !ok {
		return rows, errors.New("Unknown ID")
	}

	return rows, nil
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

func (r *CSVDBRow) AsMap() map[string]string {
	return r.row
}
