package model

import (
	"crypto/rand"
	"fmt"
)

func NewID() string {
	var id [16]byte
	_, _ = rand.Read(id[:])
	id[6] = (id[6] & 15) | 64
	id[8] = (id[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
}
