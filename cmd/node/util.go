package main

import (
	"encoding/json"
	"time"
)

func marshalBody(v any) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	return json.RawMessage(raw), err
}

func nowMillis() int64 { return time.Now().UnixNano() / int64(time.Millisecond) }
