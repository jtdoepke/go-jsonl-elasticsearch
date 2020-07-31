# go-jsonl-elasticsearch

Go package for working with line-delimited JSON files in an Elasticsearch (7.x) context.

## Tools

To build binary versions of these tools run the `cli` Makefile target. For example:

```
$> make cli
go build -mod vendor -o bin/dump cmd/dump/main.go
go build -mod vendor -o bin/restore cmd/restore/main.go
```
  
### dump

Export an Elasticsearch index as line-separated JSON.

```
$> bin/dump -h
Usage of ./bin/dump:
  -elasticsearch-endpoint string
    	The name of the Elasticsearch host to query.
  -elasticsearch-index string
    	The name of the Elasticsearch index to dump.
  -null
    	Output to /dev/null.
  -stdout
    	Output to STDOUT. (default true)
```

For example:

```
$> bin/dump \
	-elasticsearch-endpoint http://localhost:9200 \
	-elasticsearch-index millsfield \
	| bzip2 -c > /usr/local/data/millsfield.bz2

2020/07/09 13:29:52 Wrote 1000 (55658) records
2020/07/09 13:29:53 Wrote 2000 (55658) records
...
2020/07/09 13:30:28 Wrote 53000 (55658) records
2020/07/09 13:30:29 Wrote 54000 (55658) records
2020/07/09 13:30:29 Wrote 55000 (55658) records
2020/07/09 13:30:29 Wrote 55658 (55658) records
```

### restore

Restore an Elasticsearch index from line-separated JSON (produced by the `dump` tool).

```
$> bin/restore -h
Usage of ./bin/restore:
  -elasticsearch-endpoint string
    	The name of the Elasticsearch host to query.
  -elasticsearch-index string
    	The name of the Elasticsearch index to dump.
  -is-bzip
    	Signal that the data is compressed using bzip2 encoding.
  -stdin
    	Read data from STDIN
  -validate-json
    	Ensure each record is valid JSON.
  -workers int
    	The number of concurrent processes to use when indexing data. (default 4)
```

For example:

```
$> ./bin/restore \
	-elasticsearch-endpoint http://localhost:9200 \
	-elasticsearch-index millsfield \
	-is-bzip \
	/usr/local/data/millsfield.bz2

{
  "NumAdded": 55658,
  "NumFlushed": 55658,
  "NumFailed": 0,
  "NumIndexed": 55658,
  "NumCreated": 0,
  "NumUpdated": 0,
  "NumDeleted": 0,
  "NumRequests": 31
}
```

## See also

* https://github.com/aaronland/go-jsonl
* https://github.com/elastic/go-elasticsearch