package domain

// QueueStats aggregates operational metrics and depth counts for a named queue.
type QueueStats struct {
	Queue     string `json:"queue"`
	Pending   int64  `json:"pending"`
	Scheduled int64  `json:"scheduled"`
	Running   int64  `json:"running"`
	Succeeded int64  `json:"succeeded"`
	Failed    int64  `json:"failed"`
	Dead      int64  `json:"dead"`
	Cancelled int64  `json:"cancelled"`
	Total     int64  `json:"total"`
}
