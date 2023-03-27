package model

import "encoding/json"

type ESSearchResponse struct {
	Error       json.RawMessage   `json:"error,omniempty"`
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
	ID     string            `json:"_id"`
	Index  string            `json:"_index"`
	Source json.RawMessage   `json:"_source"`
	Sort   []json.RawMessage `json:"sort,omitempty"`
}

type ESQuery struct {
	Query       json.RawMessage   `json:"query"`
	Sort        []json.RawMessage `json:"sort,omitempty"`
	SearchAfter []json.RawMessage `json:"search_after,omitempty"`
	// PointInTime ESPIT             `json:"pit,omitempty"`
}

type ESPIT struct {
	ID        string `json:"id"`
	KeepAlive string `json:"keep_alive,omitempty"`
}

type ESCountResponse struct {
	Count int `json:"count"`
}

type ESNodeStatsResponse struct {
	Status struct {
		Failed     int `json:"failed"`
		Successful int `json:"successful"`
		Total      int `json:"total"`
	} `json:"_nodes"`
	Nodes map[string]struct {
		Attributes map[string]string `json:"attributes"`
		Breakers   map[string]struct {
			EstimatedSize          string  `json:"estimated_size"`
			EstimatedSize_in_bytes int64   `json:"estimated_size_in_bytes"`
			LimitSize              string  `json:"limit_size"`
			LimitSizeInBytes       int64   `json:"limit_size_in_bytes"`
			Overhead               float64 `json:"overhead"`
			Tripped                int     `json:"tripped"`
		} `json:"breakers"`
	} `json:"nodes"`
}
