package model

import "encoding/json"

type ESResponse struct {
	ScrollID string         `json:"_scroll_id"`
	Hits     ESResponseHits `json:"hits"`
}

type ESTotal struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type ESResponseHits struct {
	Total ESTotal `json:"total"`
	Hits  []ESHit `json:"hits"`
}

type ESHit struct {
	ID     string          `json:"_id"`
	Index  string          `json:"_index"`
	Source json.RawMessage `json:"_source"`
}
