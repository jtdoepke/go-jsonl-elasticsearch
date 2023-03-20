package main

import (
	_ "bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"sync"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
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
	resp, err := es_client.OpenPointInTime([]string{*es_index}, "1m", es_client.OpenPointInTime.WithContext(ctx))
	if err != nil {
		return err
	}
	pit := &model.ESPIT{}
	if err = json.NewDecoder(resp.Body).Decode(pit); err != nil {
		return err
	}
	defer func() {
		es_client.ClosePointInTime(
			es_client.ClosePointInTime.WithBody(esutil.NewJSONReader(pit)),
		)
	}()

	body := &model.ESQuery{
		Query: json.RawMessage(`{"match_all":{}}`),
		Sort: []json.RawMessage{
			json.RawMessage(`{"@timestamp": {"order": "asc"}}`),
		},
		PointInTime: *pit,
	}

	resp, err = es_client.Count(
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
	const size = 1000
	for {
		resp, err = es_client.Search(
			es_client.Search.WithContext(ctx),
			es_client.Search.WithBody(esutil.NewJSONReader(body)),
			es_client.Search.WithSize(size),
			es_client.Search.WithTrackTotalHits(false),
			es_client.Search.WithIndex(*es_index),
		)
		if err != nil {
			return err
		}

		r := GetResponse()
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, r); err != nil {
			return err
		}

		count += len(r.Hits.Hits)
		log.Printf("Got %d (%d) records\n", count, total)
		if len(r.Hits.Hits) > 0 {
			c <- r
		}
		if len(r.Hits.Hits) < size {
			log.Printf("stopping because got less than requested hits")
			log.Printf("%s", b)
			break
		}
		if len(r.SearchAfter) == 0 {
			log.Printf("stopping because SearchAfter is empty")
			log.Printf("%s", b)
			break
		}
		body.SearchAfter = r.SearchAfter
		body.PointInTime = r.PointInTime
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
