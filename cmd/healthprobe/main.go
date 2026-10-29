package main

import (
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		os.Exit(2)
	}
	client := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	r, e := client.Get(os.Args[1])
	if e != nil {
		os.Exit(1)
	}
	_, e = io.Copy(io.Discard, io.LimitReader(r.Body, 32768))
	r.Body.Close()
	if e != nil || r.StatusCode != 200 {
		os.Exit(1)
	}
}
