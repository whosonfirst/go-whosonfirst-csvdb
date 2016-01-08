package csvdb

import (
	"errors"
	_ "fmt"
	"github.com/go-fsnotify/fsnotify"
	"github.com/whosonfirst/go-whosonfirst-csv"
	"io"
	"log"
	"path"
	"path/filepath"
	"time"
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

	db      map[string]*CSVDBIndex // This is possibly/probably overkill...
	columns map[string][]string
	files   map[string]time.Time
	lookup  []*CSVDBRow
	watcher *fsnotify.Watcher
}

type CSVDBIndex struct {
	index map[string][]int
}

type CSVDBRow struct {
	row map[string]string
}

func NewCSVDB() (*CSVDB, error) {

	db := make(map[string]*CSVDBIndex)
	files := make(map[string]time.Time)
	columns := make(map[string][]string)

	lookup := make([]*CSVDBRow, 0)

	watcher, err := fsnotify.NewWatcher()

	if err != nil {
		return nil, err
	}

	// defer watcher.Close()

	csvdb := CSVDB{
		db:      db,
		files:   files,
		columns: columns,
		lookup:  lookup,
		watcher: watcher,
	}

	go csvdb.monitor()

	return &csvdb, nil
}

func NewCSVDBIndex() *CSVDBIndex {
	idx := make(map[string][]int)
	return &CSVDBIndex{idx}
}

func NewCSVDBRow(row map[string]string) *CSVDBRow {
	return &CSVDBRow{row}
}

/* CSVDB methods */

func (d *CSVDB) monitor() {

	for {
		select {
		case event := <-d.watcher.Events:

			log.Printf("event (%s): %s\n", event.Name, event)

			f, _ := filepath.Abs(event.Name)
			_, relevant := d.files[f]

			if relevant && event.Op&fsnotify.Write == fsnotify.Write {
				d.reIndexCSVFile(f)
			}

		case err := <-d.watcher.Errors:
			log.Println("error:", err)
		}
	}

}

func (d *CSVDB) reIndexCSVFile(csv_file string) error {

	to_index, ok := d.columns[csv_file]

	if !ok {
		return errors.New("failed to locate columns")
	}

	log.Printf("REINDEX %s %s\n", csv_file, to_index)

	/*
		Build an index for this file - that means ripping the guts
		out of `IndexCSVFile`. Basically we need to track where the
		rows for a given file#k:v pair live (their offsets) and adjust
		that list accordingly...

		Swap indexes - this means changing the way stuff is stored
	*/

	return nil
}

// THIS IS NOT BEING USED YET...

func (d *CSVDB) index(csv_file string, to_index []string) (map[string]*CSVDBIndex, []*CSVDBRow, error) {

	reader, err := csv.NewDictReader(csv_file)

	if err != nil {
		return nil, nil, err
	}

	db := make(map[string]*CSVDBIndex)
	lookup := make([]*CSVDBRow, 0)

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

		/*
			Take row and truncate it down to something where all
			the keys have values. This is what we will store and
			so this assumption about a pruned record is probably
			incorrect. It will do for now but we might want / really
			should make it optional...
		*/

		pruned := make(map[string]string)

		for k, v := range row {

			if v == "" {
				continue
			}

			pruned[k] = v
		}

		pruned_idx := -1

		/*
			Loop through the list of keys we want to index. If we have a
			value (for that key) we want to see whether we have already
			created a row for it in `d.lookup` which is just a big list
			of (pruned) rows. Rather than storing the (pruned) row multiple
			times for each key we're indexing we store it once and associate
			its offset (in `d.lookup`) with the key.
		*/

		for _, k := range to_index {

			value, ok := pruned[k]

			if !ok {
				continue
			}

			if value == "" {
				continue
			}

			idx, ok := db[k]

			if !ok {
				idx = NewCSVDBIndex()
				d.db[k] = idx
			}

			if pruned_idx == -1 {
				dbrow := NewCSVDBRow(pruned)
				lookup = append(lookup, dbrow)
				pruned_idx = len(lookup) - 1
			}

			idx.Add(value, pruned_idx)
		}

	}

	return db, lookup, nil
}

func (d *CSVDB) IndexCSVFile(csv_file string, to_index []string) error {

	var abs_path string

	if path.IsAbs(csv_file) {
		abs_path = csv_file
	} else {
		abs_path, _ = filepath.Abs(csv_file)
	}

	_, exists := d.files[abs_path]

	if exists {
		return errors.New("This file has already been indexed")
	}

	root := path.Dir(abs_path)
	err := d.watcher.Add(root)

	if err != nil {
		return err
	}

	reader, err := csv.NewDictReader(abs_path)

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

		/*
			Take row and truncate it down to something where all
			the keys have values. This is what we will store and
			so this assumption about a pruned record is probably
			incorrect. It will do for now but we might want / really
			should make it optional...
		*/

		pruned := make(map[string]string)

		for k, v := range row {

			if v == "" {
				continue
			}

			pruned[k] = v
		}

		pruned_idx := -1

		/*
			Loop through the list of keys we want to index. If we have a
			value (for that key) we want to see whether we have already
			created a row for it in `d.lookup` which is just a big list
			of (pruned) rows. Rather than storing the (pruned) row multiple
			times for each key we're indexing we store it once and associate
			its offset (in `d.lookup`) with the key.
		*/

		for _, k := range to_index {

			value, ok := pruned[k]

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

	d.columns[abs_path] = to_index
	d.files[abs_path] = time.Now()

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
