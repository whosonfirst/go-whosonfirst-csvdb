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
> indexes: 3 keys: 583634 rows: 573437 time to index: 4.628079208s
> query <col>=<id>
> gp:id=3534
search for gp:id=3534
where gp:id=3534 1
looping over result #1
where tgn:id=7013051 0
where wd:id=Q340 0
where fct:id=03c06bce-8f76-11e1-848f-cfd5bf3ef515 0
where fb:id=en.montreal 0
where gn:id=6077243 1
where nyt:id=N59179828586486930801 0
where wof:id=101736545 1
where gp:id=3534 1
where dbp:id=Montreal 0

> query <col>=<id>
> gp:id=44418
search for gp:id=44418
where gp:id=44418 0

> query <col>=<id>
> gp:id=1155
search for gp:id=1155
where gp:id=1155 1
looping over result #1
where gp:id=1155 1
where wof:id=85784831 1
where qs:id=238261 0

query <col>=<id>
> 
```

### wof-csvdb-server

A small HTTP pony for querying a CSV file and getting the results back as JSON.

```
$> ./bin/wof-csvdb-server -columns wof:id,gp:id,gn:id /usr/local/mapzen/whosonfirst-data/meta/wof-concordances-latest.csv
time to index /usr/local/mapzen/whosonfirst-data/meta/wof-concordances-latest.csv: 2.667267366s
wof-csvdb-server running at localhost:8228
```

And then:

```
curl -s 'http://localhost:8228?k=gp:id&v=3534' | python -mjson.tool
[
    {
        "dbp:id": "Montreal",
        "fb:id": "en.montreal",
        "fct:id": "03c06bce-8f76-11e1-848f-cfd5bf3ef515",
        "gn:id": "6077243",
        "gp:id": "3534",
        "nyt:id": "N59179828586486930801",
        "tgn:id": "7013051",
        "wd:id": "Q340",
        "wof:id": "101736545"
    }
]
```

_Note that as of this writing the `wof-csvdb-server` does not offer any kind of introspection so you need to know what has been indexed before you issue a query._

## See also

* https://github.com/whosonfirst/go-whosonfirst-csv
