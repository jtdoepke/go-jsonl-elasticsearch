package main

import (
	_ "bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"math"
	"os"
	"sync"
	"time"

	retry "github.com/avast/retry-go"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	json "github.com/goccy/go-json"
	"github.com/sourcegraph/conc/pool"

	"github.com/sfomuseum/go-jsonl-elasticsearch/model"
)

// CLI flags
var (
	es_endpoint = flag.String("elasticsearch-endpoint", "", "The name of the Elasticsearch host to query.")
	es_index    = flag.String("elasticsearch-index", "", "The name of the Elasticsearch index to dump.")
	size        = flag.Int("size", 1000, "ES request batch size")

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
	c := make(chan *model.ESResponse, 10)
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
	// resp, err := es_client.OpenPointInTime([]string{*es_index}, "1m", es_client.OpenPointInTime.WithContext(ctx))
	// if err != nil {
	// 	return err
	// }
	// pit := &model.ESPIT{}
	// if err = json.NewDecoder(resp.Body).Decode(pit); err != nil {
	// 	return err
	// }
	// defer func() {
	// 	es_client.ClosePointInTime(
	// 		es_client.ClosePointInTime.WithBody(esutil.NewJSONReader(pit)),
	// 	)
	// }()

	body := &model.ESQuery{
		Query: json.RawMessage(`{"match_all":{}}`),
		Sort: []json.RawMessage{
			json.RawMessage(`{"@timestamp": {"order": "asc"}}`),
		},
		// PointInTime: *pit,
	}

	resp, err := es_client.Count(
		es_client.Count.WithContext(ctx),
		es_client.Count.WithIndex(*es_index),
	)
	if err != nil {
		return err
	}
	countResp := &model.ESCountResponse{}
	if err = json.NewDecoder(resp.Body).Decode(countResp); err != nil {
		return err
	}
	total := countResp.Count

	count := 0
	for {
		r := GetResponse()
		reqSize := *size
		err := retry.Do(
			func() error {
				resp, err = es_client.Search(
					es_client.Search.WithContext(ctx),
					es_client.Search.WithBody(esutil.NewJSONReader(body)),
					es_client.Search.WithSize(reqSize),
					es_client.Search.WithTrackTotalHits(false),
					es_client.Search.WithIndex(*es_index),
					es_client.Search.WithSort("_doc"),
					es_client.Search.WithSource("true"),
					es_client.Search.WithTrackScores(false),
				)
				if err != nil {
					return err
				}
				b, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					return err
				}
				if err := json.Unmarshal(b, r); err != nil {
					return err
				}
				if len(r.Error) > 0 {
					return errors.New(string(r.Error))
				}
				return nil
			},
			retry.OnRetry(func(n uint, err error) {
				reqSize = (*size) / int(math.Pow(2, float64(n)))
				if reqSize < 1 {
					reqSize = 1
				}
				log.Printf("setting request size to %d", reqSize)
			}),
			retry.MaxDelay(5*time.Minute),
		)
		if err != nil {
			return err
		}

		count += len(r.Hits.Hits)
		log.Printf("Got %d (%d) records\n", count, total)
		if len(r.Hits.Hits) > 0 {
			body.SearchAfter = r.Hits.Hits[len(r.Hits.Hits)-1].Sort
			// body.PointInTime = r.PointInTime
			c <- r
		}
		if len(r.Hits.Hits) < *size {
			log.Printf("stopping because got less than requested hits")
			break
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
				rec.Sort = nil
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
