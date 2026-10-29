package platform2_test

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"partner-hub/internal/mock/platform2"
	"strings"
	"testing"
)

const validPayload = `{"extUserId":"00009","stockCode":"000001","stockName":"平安银行","operateType":"BUY","tradeDateTime":"2026-10-22 09:30:00.123","marketNo":"SZ","bizType":1,"traceId":"trace"}`

func TestPlatform2Decode(t *testing.T) {
	r := &kgo.Record{Topic: "notifications", Partition: 2, Offset: 7, Key: []byte("00009"), Value: []byte(validPayload)}
	receipt, err := platform2.DecodeRecord(r)
	if err != nil || receipt.UserUID != "00009" || receipt.Topic != r.Topic || receipt.Partition != 2 || receipt.Offset != 7 || string(receipt.Payload) != validPayload || receipt.ReceiveID == "" {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	for _, body := range []string{
		strings.Replace(validPayload, `"bizType":1`, `"bizType":"1"`, 1),
		strings.Replace(validPayload, `"bizType":1`, `"bizType":6`, 1),
		strings.Replace(validPayload, `"traceId":"trace"`, `"traceId":"trace","traceId":"other"`, 1),
		strings.Replace(validPayload, `"traceId":"trace"`, `"traceId":null`, 1),
		strings.Replace(validPayload, `"traceId":"trace"`, `"traceId":"trace","msgId":"extra"`, 1),
		strings.Replace(validPayload, `.123`, `.12`, 1),
		strings.Replace(validPayload, `"stockCode"`, `"StockCode"`, 1),
	} {
		bad := *r
		bad.Value = []byte(body)
		if _, err = platform2.DecodeRecord(&bad); err == nil {
			t.Fatalf("invalid received payload accepted %s", body)
		}
	}
	bad := *r
	bad.Key = []byte("wrongUID")
	if _, err = platform2.DecodeRecord(&bad); err == nil {
		t.Fatal("key mismatch accepted")
	}
}
