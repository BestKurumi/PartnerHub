package ingress

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
	"unicode"
	"unicode/utf8"

	"partner-hub/internal/model"
)

const maxBodyBytes = 64 * 1024

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

var exchangeLocation, exchangeLocationErr = time.LoadLocation("Asia/Shanghai")

// FieldError describes a rejected field without including sensitive values.
type FieldError struct {
	Field  string
	Reason string
}

func (e *FieldError) Error() string { return "invalid trade field " + e.Field + ": " + e.Reason }
func (e *FieldError) Unwrap() error { return ErrInvalidTrade }

func invalid(field, reason string) error { return &FieldError{Field: field, Reason: reason} }

var (
	ErrInvalidTrade = errors.New("invalid trade")
	ErrBodyTooLarge = errors.New("trade body too large")
)

// Decode validates one upstream trade without performing routing or delivery.
func Decode(r io.Reader) (model.TradeEvent, error) {
	raw, err := io.ReadAll(io.LimitReader(r, maxBodyBytes+1))
	if err != nil {
		return model.TradeEvent{}, fmt.Errorf("read trade: %w", err)
	}
	if len(raw) > maxBodyBytes {
		return model.TradeEvent{}, ErrBodyTooLarge
	}
	if !utf8.Valid(raw) {
		return model.TradeEvent{}, invalid("body", "invalid UTF-8")
	}

	fields, err := objectFields(raw)
	if err != nil {
		return model.TradeEvent{}, err
	}
	t, err := decodeFields(fields)
	if err != nil {
		return model.TradeEvent{}, err
	}
	t.RawPayload = raw
	return t, nil
}

// Read the keys explicitly: struct decoding is case-insensitive and normal
// map decoding silently discards duplicate keys, including escaped spellings.
func objectFields(raw []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	open, err := d.Token()
	if err != nil || open != json.Delim('{') {
		return nil, invalid("body", "expected one JSON object")
	}
	fields := make(map[string]json.RawMessage, 20)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, invalid("body", "malformed JSON")
		}
		name, ok := key.(string)
		if !ok {
			return nil, invalid("body", "expected field name")
		}
		if _, exists := fields[name]; exists {
			return nil, invalid("body", "duplicate field")
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, invalid("body", "malformed field value")
		}
		fields[name] = value
	}
	if close, err := d.Token(); err != nil || close != json.Delim('}') {
		return nil, invalid("body", "incomplete JSON object")
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, invalid("body", "trailing JSON or data")
	}
	return fields, nil
}

func decodeFields(fields map[string]json.RawMessage) (model.TradeEvent, error) {
	var t model.TradeEvent
	// All 20 fields are required by the Demo contract. Unknown keys, including
	// case variants of valid fields, must never be accepted as aliases.
	stringsToRead := []struct {
		name  string
		dst   *string
		limit int
	}{
		{"msgId", &t.MsgID, 36}, {"eventType", &t.EventType, 128},
		{"source", &t.Source, 128}, {"clientId", &t.ClientID, 128},
		{"accountId", &t.AccountID, 128}, {"orderId", &t.OrderID, 128},
		{"businessId", &t.BusinessID, 128}, {"traceId", &t.TraceID, 128},
		{"secCode", &t.SecCode, 64}, {"secName", &t.SecName, 255},
		{"side", &t.Side, 128}, {"orderType", &t.OrderType, 128},
		{"orderStatus", &t.OrderStatus, 128}, {"market", &t.Market, 128},
		{"tradeType", &t.TradeType, 128},
	}
	for _, f := range stringsToRead {
		value := fields[f.name]
		if len(value) == 0 || value[0] != '"' || json.Unmarshal(value, f.dst) != nil || strings.TrimSpace(*f.dst) == "" {
			return t, invalid(f.name, "required nonempty string")
		}
		if utf8.RuneCountInString(*f.dst) > f.limit || strings.ContainsFunc(*f.dst, unicode.IsControl) {
			return t, invalid(f.name, "invalid length or control character")
		}
	}
	if !uuidPattern.MatchString(t.MsgID) {
		return t, invalid("msgId", "expected UUID")
	}
	for _, f := range []struct {
		name, value string
		allowed     []string
	}{
		{"eventType", t.EventType, []string{"TRADE_EXECUTION"}},
		{"source", t.Source, []string{"counter_system"}},
		{"side", t.Side, []string{"BUY", "SELL"}},
		{"orderType", t.OrderType, []string{"LIMIT", "MARKET"}},
		{"orderStatus", t.OrderStatus, []string{"FILLED", "PARTIAL_FILLED"}},
		{"market", t.Market, []string{"SH", "SZ"}},
		{"tradeType", t.TradeType, []string{"NORMAL", "IPO", "ETF", "MARGIN", "BLOCK"}},
	} {
		found := false
		for _, allowed := range f.allowed {
			if f.value == allowed {
				found = true
				break
			}
		}
		if !found {
			return t, invalid(f.name, "unsupported value")
		}
	}
	value := fields["produceTime"]
	if len(value) == 0 || bytes.Equal(value, []byte("null")) || json.Unmarshal(value, &t.ProduceTime) != nil {
		return t, invalid("produceTime", "required integer milliseconds")
	}
	for _, f := range []struct {
		name string
		dst  *json.Number
	}{
		{"execQuantity", &t.ExecQuantity}, {"execPrice", &t.ExecPrice}, {"execAmount", &t.ExecAmount},
	} {
		value := fields[f.name]
		if len(value) == 0 || (value[0] != '-' && (value[0] < '0' || value[0] > '9')) || json.Unmarshal(value, f.dst) != nil || !validDecimal(f.dst.String()) {
			return t, invalid(f.name, "required decimal within precision 38 and scale 12")
		}
	}
	var exchangeTime string
	value = fields["execTime"]
	if len(value) == 0 || value[0] != '"' || json.Unmarshal(value, &exchangeTime) != nil {
		return t, invalid("execTime", "required exchange time")
	}
	const layout = "2006-01-02 15:04:05.000"
	// Embed IANA data so runtime images without system tzdata still parse the
	// exchange zone correctly, including historical daylight-saving dates.
	if exchangeLocationErr != nil {
		return t, fmt.Errorf("load exchange timezone: %w", exchangeLocationErr)
	}
	parsedTime, err := time.ParseInLocation(layout, exchangeTime, exchangeLocation)
	if err != nil || parsedTime.Format(layout) != exchangeTime {
		return t, invalid("execTime", "expected yyyy-MM-dd HH:mm:ss.SSS in Asia/Shanghai")
	}
	t.ExecTime = parsedTime
	if len(fields) != 20 {
		return t, invalid("body", "unknown field")
	}
	return t, nil
}

// Inspect the equivalent exact decimal without float conversion or expansion
// of an untrusted exponent. Insignificant zeroes do not consume precision.
func validDecimal(value string) bool {
	coefficient := strings.TrimPrefix(value, "-")
	exponent := int64(0)
	if i := strings.IndexAny(coefficient, "eE"); i >= 0 {
		var err error
		exponent, err = strconv.ParseInt(coefficient[i+1:], 10, 32)
		if err != nil {
			return false
		}
		coefficient = coefficient[:i]
	}
	fraction := 0
	if i := strings.IndexByte(coefficient, '.'); i >= 0 {
		fraction = len(coefficient) - i - 1
		coefficient = coefficient[:i] + coefficient[i+1:]
	}
	coefficient = strings.TrimLeft(coefficient, "0")
	if coefficient == "" {
		return true
	}
	scale := int64(fraction) - exponent
	for strings.HasSuffix(coefficient, "0") {
		coefficient = strings.TrimSuffix(coefficient, "0")
		scale--
	}
	precision := int64(len(coefficient))
	if scale < 0 {
		precision -= scale
	}
	return precision <= 38 && scale <= 12
}
