# go-whosonfirst-csvdb

Experimental in-memory database for CSV files.

## Caveats

This is not sophisticated. It is not meant to be sophisticated. It is meant to be easy and fast. It might also be too soon for you to play with depending on how you feel about "things in flux".

## Usage

_Please write me_

## Utilities

### wof-csvdb-index

This is a little bit of a misnomer as it's mostly a testing tool right now. Oh well...

In this example we'll index three columns from the [wof-concordances-latest.csv]() file (specifically `wof:id` and `gp:id` and `gn:id`) and then perform a couple queries against the index. We'll also query for the key, value pairs in each response row (assuming that most of them will fail since they haven't been indexed).

```
$> ./bin/wof-csvdb-index -columns wof:id,gp:id,gn:id /usr/local/mapzen/whosonfirst-data/meta/wof-concordances-latest.csv 
indexes: 3 keys: 583635 rows: 573437 time to index: 6.633153941s
query <key>=<id>
> gp:id=3534
search for gp:id=3534
where gp:id=3534 1
looping over result #1
where tgn:id=7013051 0
where gn:id=6077243 1
where dbp:id=Montreal 0
where fb:id=en.montreal 0
where fct:id=03c06bce-8f76-11e1-848f-cfd5bf3ef515 0
where nyt:id=N59179828586486930801 0
where wof:id=101736545 1
where gp:id=3534 1
where wd:id=Q340 0

> gp:id=2518161
search for gp:id=2518161
where gp:id=2518161 1
looping over result #1
where qs:id=1195177 0
where wof:id=85856887 1
where gp:id=2518161 1
```

## See also

* https://github.com/whosonfirst/go-whosonfirst-csv
