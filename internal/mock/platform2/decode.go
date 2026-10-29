package platform2

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
	"partner-hub/internal/model"
	"strings"
	"time"
	_ "time/tzdata"
	"unicode"
	"unicode/utf8"
)

// Validate independently of the broker adapter; an invalid wire message cannot
// appear in the customer's actual-receipt center.
func DecodeRecord(r *kgo.Record) (Receipt, error) {
	bad := errors.New("invalid platform2 notification")
	if r == nil || len(r.Value) > 8192 || !utf8.Valid(r.Value) || r.Offset < 0 || r.Partition < 0 || r.Topic == "" {
		return Receipt{}, bad
	}
	decoder := json.NewDecoder(bytes.NewReader(r.Value))
	open, err := decoder.Token()
	if err != nil || open != json.Delim('{') {
		return Receipt{}, bad
	}
	fields := map[string]json.RawMessage{}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return Receipt{}, bad
		}
		name, ok := key.(string)
		if !ok || fields[name] != nil {
			return Receipt{}, bad
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return Receipt{}, bad
		}
		fields[name] = value
	}
	if close, err := decoder.Token(); err != nil || close != json.Delim('}') {
		return Receipt{}, bad
	}
	if _, err = decoder.Token(); err != io.EOF || len(fields) != 8 {
		return Receipt{}, bad
	}
	values := map[string]string{}
	for name, limit := range map[string]int{"extUserId": 128, "stockCode": 64, "stockName": 255, "operateType": 4, "tradeDateTime": 23, "marketNo": 2, "traceId": 128} {
		var value string
		if json.Unmarshal(fields[name], &value) != nil || strings.TrimSpace(value) == "" || utf8.RuneCountInString(value) > limit || strings.ContainsFunc(value, unicode.IsControl) {
			return Receipt{}, bad
		}
		values[name] = value
	}
	var kind int
	if json.Unmarshal(fields["bizType"], &kind) != nil || kind < 1 || kind > 5 {
		return Receipt{}, bad
	}
	if (values["operateType"] != "BUY" && values["operateType"] != "SELL") || (values["marketNo"] != "SH" && values["marketNo"] != "SZ") || string(r.Key) != values["extUserId"] {
		return Receipt{}, bad
	}
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return Receipt{}, bad
	}
	const layout = "2006-01-02 15:04:05.000"
	parsed, err := time.ParseInLocation(layout, values["tradeDateTime"], location)
	if err != nil || parsed.Format(layout) != values["tradeDateTime"] {
		return Receipt{}, bad
	}
	return Receipt{ReceiveID: model.NewID(), Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, UserUID: values["extUserId"], TraceID: values["traceId"], Payload: append([]byte(nil), r.Value...)}, nil
}
