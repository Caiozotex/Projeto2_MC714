package transport

import "time"

func nowMillis() int64 { return time.Now().UnixNano() / int64(time.Millisecond) }
