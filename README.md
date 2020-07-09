# go-jsonl-elasticsearch

Go package for working with line-delimited JSON files in an Elasticsearch context.

## Important

Work in progress. Documentation to follow.

## Tools

```
$> make cli
go build -mod vendor -o bin/dump cmd/dump/main.go
go build -mod vendor -o bin/restore cmd/restore/main.go
```
  
### dump

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

## See also

* https://github.com/aaronland/go-jsonl