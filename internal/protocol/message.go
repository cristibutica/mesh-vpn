package protocol

type Message struct {
	Type  string   `json:"type"`
	Addrs []string `json:"addr,omitempty"`
	ID    string   `json:"id,omitempty"`
}
