package main

import (
	_ "bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log"
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
	size        = flag.Int("size", 100, "ES request batch size")

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
	c := make(chan *model.ESSearchResponse, 10)
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

func readIndex(ctx context.Context, c chan<- *model.ESSearchResponse) error {
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

	scrollID := ""

	count := 0
	for {
		r := GetResponse()
		err := retry.Do(
			func() error {
				if scrollID == "" {
					resp, err = es_client.Search(
						es_client.Search.WithContext(ctx),
						es_client.Search.WithBody(esutil.NewJSONReader(body)),
						es_client.Search.WithSize(*size),
						es_client.Search.WithTrackScores(false),
						es_client.Search.WithIndex(*es_index),
						es_client.Search.WithSort("_doc"),
						es_client.Search.WithSource("true"),
						es_client.Search.WithScroll(1*time.Minute),
					)
				} else {
					resp, err = es_client.Scroll(
						es_client.Scroll.WithContext(ctx),
						es_client.Scroll.WithScrollID(scrollID),
						es_client.Scroll.WithScroll(1*time.Minute),
					)
				}
				if err != nil {
					return err
				}
				err := json.NewDecoder(resp.Body).Decode(r)
				resp.Body.Close()
				if err != nil {
					return err
				}
				if len(r.Error) > 0 {
					return errors.New(string(r.Error))
				}
				return nil
			},
			retry.OnRetry(func(n uint, err error) {
				// Wait for circuit breakers to untrip
			outer:
				for {
					log.Println("checking for tripped breakers...")
					resp, err := es_client.Nodes.Stats(
						es_client.Nodes.Stats.WithContext(ctx),
						es_client.Nodes.Stats.WithMetric("breaker"),
					)
					if err != nil {
						log.Fatal(err)
					}
					s := &model.ESNodeStatsResponse{}
					err = json.NewDecoder(resp.Body).Decode(s)
					resp.Body.Close()
					if err != nil {
						log.Fatal(err)
					}
					if s.Status.Failed > 0 {
						time.Sleep(10 * time.Second)
						continue
					}
					for _, n := range s.Nodes {
						for _, b := range n.Breakers {
							if b.EstimatedSizeInBytes >= b.LimitSizeInBytes {
								time.Sleep(10 * time.Second)
								continue outer
							}
						}
					}
					break
				}
			}),
			retry.MaxDelay(1*time.Minute),
			retry.MaxJitter(10*time.Second),
		)
		if err != nil {
			return err
		}

		if n := len(r.Hits.Hits); n > 0 {
			count += n
			log.Printf("Got %d (%d) records\n", count, total)
			scrollID = r.ScrollID
			c <- r
		} else {
			log.Printf("stopping because got zero hits")
			break
		}
		if count >= total {
			log.Printf("stopping because count is greater than or equal to total hits")
			break
		}
	}

	return nil
}

func writeDocuments(ctx context.Context, c <-chan *model.ESSearchResponse) error {
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
	zeroResponse = &model.ESSearchResponse{}
	responsePool = sync.Pool{
		New: func() interface{} {
			return new(model.ESSearchResponse)
		},
	}
)

func GetResponse() *model.ESSearchResponse {
	r := responsePool.Get().(*model.ESSearchResponse)
	*r = *zeroResponse
	return r
}

func PutResponse(r *model.ESSearchResponse) {
	responsePool.Put(r)
}
