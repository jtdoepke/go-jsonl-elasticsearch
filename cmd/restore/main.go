package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/aaronland/go-jsonl/walk"
	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/tidwall/pretty"

	"github.com/sfomuseum/go-jsonl-elasticsearch/model"
)

func main() {

	es_endpoint := flag.String("elasticsearch-endpoint", "", "The name of the Elasticsearch host to query.")
	es_index := flag.String("elasticsearch-index", "", "The name of the Elasticsearch index to dump.")

	workers := flag.Int("workers", runtime.NumCPU(), "The number of concurrent processes to use when indexing data.")
	validate_json := flag.Bool("validate-json", false, "Ensure each record is valid JSON.")
	is_bzip := flag.Bool("is-bzip", false, "Signal that the data is compressed using bzip2 encoding.")
	stdin := flag.Bool("stdin", false, "Read data from STDIN")

	flag.Parse()

	ctx := context.Background()

	retry := backoff.NewExponentialBackOff()

	es_cfg := elasticsearch.Config{
		Addresses: []string{
			*es_endpoint,
		},
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retry.Reset()
			}
			return retry.NextBackOff()
		},
		MaxRetries: 5,
	}

	es_client, err := elasticsearch.NewClient(es_cfg)

	if err != nil {
		log.Fatalf("Failed to create ES client, %v", err)
	}

	bi_cfg := esutil.BulkIndexerConfig{
		Index:         *es_index,
		Client:        es_client,
		NumWorkers:    *workers,
		FlushInterval: 30 * time.Second,
	}

	bi, err := esutil.NewBulkIndexer(bi_cfg)

	record_ch := make(chan *walk.WalkRecord)
	error_ch := make(chan *walk.WalkError)
	done_ch := make(chan bool)

	go func() {

		for {

			select {
			case <-ctx.Done():
				return
			case <-done_ch:
				return
			case err := <-error_ch:
				log.Println(err)
			case rec := <-record_ch:

				var doc *model.ESHit

				err := json.Unmarshal(rec.Body, &doc)

				if err != nil {
					log.Printf("Failed to unmarshal document, %v", err)
				} else {

					doc_id := doc.ID
					doc_body, _ := json.Marshal(doc.Source)

					// hash := sha256.Sum256(rec.Body)
					// doc_id := fmt.Sprintf("%x", hash[:])

					br := bytes.NewReader(doc_body)
					path := "n/a"

					bulk_item := esutil.BulkIndexerItem{
						Action:     "index",
						DocumentID: doc_id,
						Body:       br,

						OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
							// log.Printf("Indexed %s\n", path)
						},

						OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
							if err != nil {
								log.Printf("ERROR: Failed to index %s, %s", path, err)
							} else {
								log.Printf("ERROR: Failed to index %s, %s: %s", path, res.Error.Type, res.Error.Reason)
							}
						},
					}

					err = bi.Add(ctx, bulk_item)

					if err != nil {
						log.Printf("Failed to schedule %s, %v", path, err)
					}
				}

			}
		}
	}()

	walk_opts := &walk.WalkOptions{
		Workers:       *workers,
		RecordChannel: record_ch,
		ErrorChannel:  error_ch,
		ValidateJSON:  *validate_json,
		FormatJSON:    false,
		IsBzip:        *is_bzip,
	}

	uris := flag.Args()

	if *stdin {
		walk.WalkReader(ctx, walk_opts, os.Stdin)

	} else {

		for _, uri := range uris {

			fh, err := os.Open(uri)

			if err != nil {
				log.Fatal(err)
			}

			walk.WalkReader(ctx, walk_opts, fh)
		}
	}

	done_ch <- true

	err = bi.Close(ctx)

	if err != nil {
		log.Fatal(err)
	}

	stats := bi.Stats()

	enc_stats, err := json.Marshal(stats)

	if err != nil {
		log.Fatal(err)
	}

	enc_stats = pretty.Pretty(enc_stats)
	fmt.Println(string(enc_stats))
}
