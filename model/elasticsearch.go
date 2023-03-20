package model

import "encoding/json"

type ESResponse struct {
	ScrollID    string            `json:"_scroll_id"`
	Hits        ESResponseHits    `json:"hits"`
	PointInTime ESPIT             `json:"pit,omitempty"`
	SearchAfter []json.RawMessage `json:"search_after,omitempty"`
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

type ESQuery struct {
	Query       json.RawMessage   `json:"query"`
	Sort        []json.RawMessage `json:"sort,omitempty"`
	SearchAfter []json.RawMessage `json:"search_after,omitempty"`
	PointInTime ESPIT             `json:"pit,omitempty"`
}

type ESPIT struct {
	ID        string `json:"id"`
	KeepAlive string `json:"keep_alive,omitempty"`
}

type ESCountResponse struct {
	Count int `json:"count"`
}
