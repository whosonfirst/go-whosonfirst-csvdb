package csvdb

import (
	"errors"
	_ "fmt"
	csv "github.com/whosonfirst/go-whosonfirst-csv"
	"io"
)

type CSVDB struct {

	// So yeah these two names should probably be flipped...
	// 
	// 'map' is something like:
	// map['gp:id'] = { '3534': [25] }
	// map['gn:id'] = { '999': [25] }
	//
	// 'lookup' is something like:
	// lookup[25] = {'gp:id':'3534', 'wof:id':'1234', 'gn:id':'999' }

	db     map[string]*CSVDBIndex	 // This is possibly/probably overkill...
	lookup []*CSVDBRow
}

type CSVDBIndex struct {
	index map[string][]int
}

type CSVDBRow struct {
	row map[string]string
}

func NewCSVDB() *CSVDB {

	db := make(map[string]*CSVDBIndex)
	lookup := make([]*CSVDBRow, 0)

	return &CSVDB{db, lookup}
}

func NewCSVDBIndex() *CSVDBIndex {
	idx := make(map[string][]int)
	return &CSVDBIndex{idx}
}

func NewCSVDBRow(row map[string]string) *CSVDBRow {
	return &CSVDBRow{row}
}

/* CSVDB methods */

func (d *CSVDB) IndexCSVFile(csv_file string, to_index []string) error {

	reader, err := csv.NewDictReader(csv_file)

	if err != nil {
		return err
	}

	offset := 0

	for {

		offset += 1

		row, err := reader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			continue
		}

		pruned := make(map[string]string)

		for k, v := range row {

			if v == "" {
				continue
			}

			pruned[k] = v
		}

		pruned_idx := -1

		for _, k := range to_index {

			value, ok := row[k]

			if !ok {
				continue
			}

			if value == "" {
				continue
			}

			idx, ok := d.db[k]

			if !ok {
				idx = NewCSVDBIndex()
				d.db[k] = idx
			}

			if pruned_idx == -1 {
				dbrow := NewCSVDBRow(pruned)
				d.lookup = append(d.lookup, dbrow)
				pruned_idx = len(d.lookup) - 1
			}

			idx.Add(value, pruned_idx)
		}

	}

	return nil
}

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

	return len(d.lookup)

	/*
		count := 0

		for i, _ := range d.db {
			count += d.db[i].Rows()
		}

		return count
	*/
}

func (d *CSVDB) Where(key string, id string) ([]*CSVDBRow, error) {

	rows := make([]*CSVDBRow, 0)

	idx, ok := d.db[key]

	if !ok {
		return rows, errors.New("Unknown index")
	}

	offsets, ok := idx.index[id] // PLEASE MAKE ME A FUNCTION OR SOMETHING

	if !ok {
		return rows, errors.New("Unknown ID")
	}

	for _, idx := range offsets {
		row := d.lookup[idx]
		rows = append(rows, row)
	}

	return rows, nil
}

/* CSVDBIndex methods */

func (i *CSVDBIndex) Add(key string, lookup_idx int) bool {

	possible, ok := i.index[key]

	if !ok {
		possible = make([]int, 0)
	}

	possible = append(possible, lookup_idx)
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
