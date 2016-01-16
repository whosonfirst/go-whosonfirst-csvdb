package csvdb

import (
	"errors"
	_ "fmt"
	"github.com/go-fsnotify/fsnotify"
	"github.com/whosonfirst/go-whosonfirst-csv"
	"github.com/whosonfirst/go-whosonfirst-log"
	"io"
	"path"
	"path/filepath"
	"sync"
	"time"
)

/* CSVDBIndex */

type CSVDBIndex struct {
	index map[string][]int
}

func NewCSVDBIndex() *CSVDBIndex {
	idx := make(map[string][]int)
	return &CSVDBIndex{idx}
}

/* CSVDBStore */

type CSVDBStore struct {
	store map[string]*CSVDBIndex
}

func NewCSVDBStore() *CSVDBStore {
	store := make(map[string]*CSVDBIndex)
	return &CSVDBStore{store}
}

/* CSVDBLookupTable */

func NewCSVDBLookupTable() *CSVDBLookupTable {
	table := make([]*CSVDBRow, 0)
	return &CSVDBLookupTable{table}
}

type CSVDBLookupTable struct {
	table []*CSVDBRow
}

/* CSVDBRow */

type CSVDBRow struct {
	row map[string]string
}

func NewCSVDBRow(row map[string]string) *CSVDBRow {
	return &CSVDBRow{row}
}

func (r *CSVDBRow) AsMap() map[string]string {
	return r.row
}

/* CSVDB */

type CSVDB struct {
	files   []string
	columns map[int][]string
	lookups map[int]*CSVDBLookupTable
	pairs   map[string]map[string][][]int // Ugh... really?

	logger  *log.WOFLogger
	watcher *fsnotify.Watcher
	reload  bool
}

func NewCSVDB(logger *log.WOFLogger) (*CSVDB, error) {

	files := make([]string, 0)
	columns := make(map[int][]string)

	watcher, err := fsnotify.NewWatcher()

	if err != nil {
		return nil, err
	}

	lookups := make(map[int]*CSVDBLookupTable)

	/*
	 This type definition is insane - please to make into
	 discrete types, at least with useful sem-descriptive
	 names (20160113/thisisaaronland)
	*/

	pairs := make(map[string]map[string][][]int)

	db := CSVDB{
		files:   files,
		columns: columns,
		lookups: lookups,
		pairs:   pairs,
		watcher: watcher,
		reload:  false,
		logger:  logger,
	}

	go db.monitor()

	return &db, nil
}

func (d *CSVDB) IndexCSVFile(csv_file string, to_index []string) error {

	var abs_path string

	if path.IsAbs(csv_file) {
		abs_path = csv_file
	} else {
		abs_path, _ = filepath.Abs(csv_file)
	}

	for _, indexed := range d.files {

		if abs_path == indexed {
			return errors.New("This file has already been indexed")
		}
	}

	root := path.Dir(abs_path)
	d.logger.Debug("watch %s", root)

	/*
		Note – it is apparently possible to have a directory with "too many files" to watch.
		I haven't figured out whether this is dependent on the operating system. Basically it
		seems to be triggered around line 226 in go-fsnotify/fsnotify/kqueue.go when it's
		calling the register method (20160115/thisisaaronland)
	*/

	err := d.watcher.Add(root)

	if err != nil {
		return err
	}

	db, lookup, err := d.index_csvfile(csv_file, to_index)

	if err != nil {
		return err
	}

	d.apply_index(abs_path, to_index, db, lookup)
	return nil
}

func (d *CSVDB) Where(key string, value string) ([]*CSVDBRow, error) {

	for d.reload {
		d.logger.Info("Re-indexing data")
		time.Sleep(1 * time.Second)
	}

	results := make([]*CSVDBRow, 0)

	values, ok := d.pairs[key]

	if !ok {
		return results, errors.New("Unknown key")
	}

	pairs, ok := values[value]

	if !ok {
		return results, errors.New("Unknown value")
	}

	for _, pair := range pairs {

		idx := pair[0]
		offset := pair[1]

		lookup := d.lookups[idx]
		row := lookup.table[offset]

		results = append(results, row)
	}

	return results, nil
}

func (d *CSVDB) monitor() {

	for {
		select {
		case event := <-d.watcher.Events:

			d.logger.Debug("event, %s", event)

			f, _ := filepath.Abs(event.Name)
			relevant := false

			if event.Op&fsnotify.Write == fsnotify.Write {

				for _, indexed := range d.files {
					if f == indexed {
						relevant = true
						break
					}
				}
			}

			if relevant {
				d.reindex_csvfile(f)
			}

		case err := <-d.watcher.Errors:
			d.logger.Warning("watcher is sad, because %s", err)
		}
	}

}

func (d *CSVDB) index_csvfile(csv_file string, to_index []string) (*CSVDBStore, *CSVDBLookupTable, error) {

     	t1 := time.Now()

	reader, err := csv.NewDictReader(csv_file)

	if err != nil {
		return nil, nil, err
	}

	db := NewCSVDBStore()
	lookup := NewCSVDBLookupTable()

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

		mu := new(sync.Mutex)

		for _, k := range to_index {

			value, ok := pruned[k]

			if !ok {
				continue
			}

			if value == "" {
				continue
			}

			mu.Lock()

			idx, ok := db.store[k]

			if !ok {
				idx = NewCSVDBIndex()
				db.store[k] = idx
			}

			if pruned_idx == -1 {
				dbrow := NewCSVDBRow(pruned)
				lookup.table = append(lookup.table, dbrow)
				pruned_idx = len(lookup.table) - 1
			}

			_, ok = idx.index[value]

			if !ok {
				idx.index[value] = make([]int, 0)
			}

			d.logger.Debug("index %s -> %d (%s)", value, pruned_idx, csv_file)
			idx.index[value] = append(idx.index[value], pruned_idx)
			
			mu.Unlock()
		}

	}

	t2 := time.Since(t1)
	d.logger.Debug("time to index %s, %v", csv_file, t2)

	return db, lookup, nil
}

func (d *CSVDB) apply_index(csv_file string, to_index []string, db *CSVDBStore, lookup *CSVDBLookupTable) {

	d.files = append(d.files, csv_file)
	idx := len(d.files) - 1

	d.lookups[idx] = lookup
	d.columns[idx] = to_index

	// please for to be WaitGroup-ing here... maybe?

	for k, i := range db.store {

		_, ok := d.pairs[k]

		if !ok {
			d.pairs[k] = make(map[string][][]int)
		}

		/*
			offset is the position of the (k,v) pair for the
			value stored in d.lookups[idx]
		*/

		for v, offset := range i.index {

			for _, p := range offset {

				pos := make([]int, 0)
				pos = append(pos, idx)
				pos = append(pos, p)

				pointers, ok := d.pairs[k][v]

				if !ok {
					pointers = make([][]int, 0)
				}

				pointers = append(pointers, pos)
				d.pairs[k][v] = pointers
			}
		}
	}

}

func (d *CSVDB) reindex_csvfile(csv_file string) error {

     	d.logger.Info("re-indexing %s", csv_file)

	t1 := time.Now()

	d.reload = true

	defer func(d *CSVDB) {
		// send a message on a channel?
		d.reload = false
	}(d)

	var idx int
	new_files := make([]string, 0)

	for i, indexed := range d.files {

		if csv_file == indexed {
			idx = i
		} else {
			new_files = append(new_files, indexed)
		}
	}

	d.files = new_files

	to_index := d.columns[idx]

	delete(d.lookups, idx)

	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)

	/*
	 TO CONSIDER - re-implement 'refs' to store the list
	 of (k,v) pairs associated with idx - smells a bit like
	 yak-shaving but might be useful in a multi-file context
	 (20160110/thisisaaronland)
	*/

	for key, values := range d.pairs {

		for value, _ := range values {

			wg.Add(1)

			go func(d *CSVDB, k string, v string, idx int) {

				defer wg.Done()

				new_pairs := make([][]int, 0)

				for _, pair := range d.pairs[key][value] {

					if pair[0] != idx {
						new_pairs = append(new_pairs, pair)
					}

				}

				mu.Lock()

				if len(new_pairs) == 0 {
					delete(d.pairs[k], v)

					if len(d.pairs[k]) == 0 {
						delete(d.pairs, k)
					}

				} else {
					d.pairs[k][v] = new_pairs
				}

				mu.Unlock()

			}(d, key, value, idx)
		}
	}

	wg.Wait()

	db, lookup, err := d.index_csvfile(csv_file, to_index)

	if err != nil {
		return err
	}

	d.apply_index(csv_file, to_index, db, lookup)

	t2 := time.Since(t1)
	d.logger.Debug("time to re-index %s, %v", csv_file, t2)

	return nil
}
