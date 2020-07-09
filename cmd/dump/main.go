package main

import (
	_ "bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aaronland/go-jsonl-elasticsearch/model"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

func main() {

	es_endpoint := flag.String("elasticsearch-endpoint", "", "The name of the Elasticsearch host to query.")
	es_index := flag.String("elasticsearch-index", "", "The name of the Elasticsearch index to dump.")

	null := flag.Bool("null", false, "Output to /dev/null.")
	stdout := flag.Bool("stdout", true, "Output to STDOUT.")

	flag.Parse()

	writers := make([]io.Writer, 0)

	if *null {
		writers = append(writers, ioutil.Discard)
	}

	if *stdout {
		writers = append(writers, os.Stdout)
	}

	wr := io.MultiWriter(writers...)

	es_cfg := elasticsearch.Config{
		Addresses: []string{
			*es_endpoint,
		},
	}

	es_client, err := elasticsearch.NewClient(es_cfg)

	if err != nil {
		log.Fatalf("Failed to create ES client, %v", err)
	}

	query_all := `{ "query": { "match_all": {} } }`
	scroll_id := ""

	count := 0

	for {

		query := query_all

		var rsp *esapi.Response
		var err error

		if scroll_id != "" {

			query = fmt.Sprintf(`{ "scroll": "5m", "scroll_id": "%s" }`, scroll_id)

			rsp, err = es_client.Scroll(
				es_client.Scroll.WithBody(strings.NewReader(query)),
			)

		} else {

			rsp, err = es_client.Search(
				es_client.Search.WithContext(context.Background()),
				es_client.Search.WithIndex(*es_index),
				es_client.Search.WithBody(strings.NewReader(query)),
				es_client.Search.WithScroll(time.Duration(60000000000)),
				es_client.Search.WithSize(1000),
			)
		}

		if err != nil {
			log.Fatalf("Failed to query, %v", err)
		}

		defer rsp.Body.Close()

		var es_response *model.ESResponse

		dec := json.NewDecoder(rsp.Body)
		err = dec.Decode(&es_response)

		if err != nil {
			log.Fatalf("Failed to decode response, %v", err)
		}

		for _, rec := range es_response.Hits.Hits {

			enc_rec, err := json.Marshal(rec)

			if err != nil {
				log.Fatal(err)
			}

			wr.Write(enc_rec)
			wr.Write([]byte("\n"))

			count += 1
		}

		log.Printf("Wrote %d (%d) records\n", count, es_response.Hits.Total.Value)

		if count >= es_response.Hits.Total.Value {
			break
		}

		scroll_id = es_response.ScrollID

		if scroll_id == "" {
			break
		}
	}
}
