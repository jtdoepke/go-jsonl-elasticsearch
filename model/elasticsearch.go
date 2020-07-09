package model

type ESResponse struct {
	ScrollID string         `json:"_scroll_id"`
	Hits     ESResponseHits `json:"hits"`
}

type ESResponseHits struct {
	Total int     `json:"total"`
	Hits  []ESHit `json:"hits"`
}

type ESHit struct {
	ID     string      `json:"_id"`
	Index  string      `json:"_index"`
	Source interface{} `json:"_source"`
}
