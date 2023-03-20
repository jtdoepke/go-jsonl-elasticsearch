package main

import (
	_ "bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/sourcegraph/conc/pool"

	"github.com/sfomuseum/go-jsonl-elasticsearch/model"
)

// CLI flags
var (
	es_endpoint = flag.String("elasticsearch-endpoint", "", "The name of the Elasticsearch host to query.")
	es_index    = flag.String("elasticsearch-index", "", "The name of the Elasticsearch index to dump.")

	null   = flag.Bool("null", false, "Output to /dev/null.")
	stdout = flag.Bool("stdout", true, "Output to STDOUT.")
)

var es_client *elasticsearch.Client

func main() {
	flag.Parse()

	var err error
	es_client, err = elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{*es_endpoint},
	})
	if err != nil {
		log.Fatalf("Failed to create ES client, %v", err)
	}

	ctx := context.Background()
	p := pool.New().WithContext(ctx).WithCancelOnError()
	c := make(chan *model.ESResponse, 1)
	p.Go(func(ctx context.Context) error {
		defer close(c)
		return readIndex(ctx, c)
	})
	p.Go(func(ctx context.Context) error {
		return writeDocuments(ctx, c)
	})
	if err := p.Wait(); err != nil {
		log.Fatal(err)
	}
}

func readIndex(ctx context.Context, c chan<- *model.ESResponse) error {
	count := 0
	total := 0
	query := `{ "query": { "match_all": {} } }`
	resp, err := es_client.Search(
		es_client.Search.WithContext(ctx),
		es_client.Search.WithIndex(*es_index),
		es_client.Search.WithBody(strings.NewReader(query)),
		es_client.Search.WithScroll(1*time.Minute),
		es_client.Search.WithSize(1000),
	)
	if err != nil {
		return err
	}
	for {
		v := GetResponse()
		err = json.NewDecoder(resp.Body).Decode(v)
		resp.Body.Close()
		if err != nil {
			return err
		}

		if v.Hits.Total.Value > total {
			total = v.Hits.Total.Value
		}
		count += len(v.Hits.Hits)
		log.Printf("Got %d (%d) records\n", count, total)
		if len(v.Hits.Hits) > 0 {
			c <- v
		}
		if count >= total {
			log.Printf("stopping because count is >= total docs")
			break
		}
		if v.ScrollID == "" {
			log.Printf("stopping because ScrollID is empty")
			break
		}

		resp, err = es_client.Scroll(
			es_client.Scroll.WithContext(ctx),
			es_client.Scroll.WithScrollID(v.ScrollID),
			es_client.Scroll.WithScroll(5*time.Minute),
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeDocuments(ctx context.Context, c <-chan *model.ESResponse) error {
	writers := make([]io.Writer, 0)
	if *null {
		writers = append(writers, io.Discard)
	}
	if *stdout {
		writers = append(writers, os.Stdout)
	}
	wr := io.MultiWriter(writers...)

outer:
	for {
		select {
		case <-ctx.Done():
			return nil
		case resp, ok := <-c:
			if !ok {
				break outer
			}
			for _, rec := range resp.Hits.Hits {
				enc_rec, err := json.Marshal(rec)
				if err != nil {
					return err
				}
				wr.Write(enc_rec)
				wr.Write([]byte("\n"))
			}
			PutResponse(resp)
		}
	}
	return nil
}

var (
	zeroResponse = &model.ESResponse{}
	responsePool = sync.Pool{
		New: func() interface{} {
			return new(model.ESResponse)
		},
	}
)

func GetResponse() *model.ESResponse {
	r := responsePool.Get().(*model.ESResponse)
	*r = *zeroResponse
	return r
}

func PutResponse(r *model.ESResponse) {
	responsePool.Put(r)
}
