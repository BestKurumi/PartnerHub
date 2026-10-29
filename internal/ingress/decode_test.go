package ingress_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"partner-hub/internal/ingress"
)

func fixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../../tests/fixtures/trades/normal.json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func replaceField(t *testing.T, raw []byte, field, value string) []byte {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if value == "" {
		delete(fields, field)
	} else {
		fields[field] = json.RawMessage(value)
	}
	b, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Catches rounding through float64, stripping ID zeroes, changing the source
// bytes, dropping fields, or interpreting exchange time in the host timezone.
func TestDecodeTrade(t *testing.T) {
	for _, tradeType := range []string{"NORMAL", "IPO", "ETF", "MARGIN", "BLOCK"} {
		t.Run(tradeType, func(t *testing.T) {
			raw := replaceField(t, fixture(t), "tradeType", `"`+tradeType+`"`)
			trade, err := ingress.Decode(bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("legal trade rejected: %v", err)
			}
			if trade.MsgID != "8a236f08-1bf4-4bb3-80c2-7eb27fe7c63f" || trade.EventType != "TRADE_EXECUTION" || trade.Source != "counter_system" || trade.ProduceTime != 1792634400123 {
				t.Errorf("upstream identity changed: %+v", trade)
			}
			if trade.ClientID != "000123" || trade.AccountID != "000456" || trade.OrderID != "000789" || trade.BusinessID != "000012" || trade.TraceID != "trace-demo-001" {
				t.Error("identifier values or leading zeroes lost")
			}
			if trade.SecCode != "000001" || trade.SecName != "平安银行" || trade.Side != "BUY" || trade.OrderType != "LIMIT" || trade.OrderStatus != "FILLED" || trade.Market != "SZ" || trade.TradeType != tradeType {
				t.Error("trade fields changed")
			}
			if trade.ExecQuantity.String() != "12345678901234567890123456.123456789012" || trade.ExecPrice.String() != "0.123456789012" || trade.ExecAmount.String() != "-12345678901234567890123456789012345678" {
				t.Error("precise numeric literals changed")
			}
			if trade.ExecTime.Format("2006-01-02 15:04:05.000") != "2026-10-22 09:30:00.123" || !trade.ExecTime.Equal(time.Date(2026, 10, 22, 1, 30, 0, 123000000, time.UTC)) {
				t.Errorf("exchange time changed: %v", trade.ExecTime)
			}
			if !bytes.Equal(trade.RawPayload, raw) {
				t.Error("original payload was rewritten")
			}
		})
	}
}

// Catches omitted validation, case-insensitive keys, and last-key-wins parsing.
func TestDecodeRejectsMalformed(t *testing.T) {
	base := fixture(t)
	cases := map[string][]byte{
		"empty":             nil,
		"array":             []byte(`[]`),
		"null":              []byte(`null`),
		"truncated":         base[:len(base)/2],
		"extra object":      append(append([]byte{}, base...), []byte(` {}`)...),
		"extra scalar":      append(append([]byte{}, base...), []byte(` true`)...),
		"duplicate":         append([]byte(`{"clientId":"other",`), bytes.TrimSpace(base)[1:]...),
		"escaped duplicate": append([]byte(`{"client\u0049d":"other",`), bytes.TrimSpace(base)[1:]...),
		"unknown field":     replaceField(t, base, "extra", `1`),
		"wrong key case":    replaceField(t, base, "ClientId", `"other"`),
		"bad utf8":          bytes.Replace(base, []byte("平安银行"), []byte{0xff}, 1),
		"null byte in id":   replaceField(t, base, "clientId", `"ab\u0000cd"`),
	}
	for _, field := range []string{"msgId", "eventType", "produceTime", "source", "clientId", "accountId", "orderId", "businessId", "traceId", "secCode", "secName", "side", "orderType", "execQuantity", "execPrice", "execAmount", "execTime", "orderStatus", "market", "tradeType"} {
		cases[field+" missing"] = replaceField(t, base, field, "")
		cases[field+" null"] = replaceField(t, base, field, `null`)
		cases[field+" empty"] = replaceField(t, base, field, `""`)
	}
	for field, values := range map[string][]string{
		"msgId":     {`"not-a-uuid"`, `"8a236f081bf44bb380c27eb27fe7c63f"`},
		"eventType": {`"OTHER"`}, "source": {`"other"`},
		"side": {`"buy"`}, "orderType": {`"STOP"`},
		"orderStatus": {`"OPEN"`}, "market": {`"HK"`}, "tradeType": {`"OTHER"`},
		"clientId":     {`123`, `"   "`, `[]`},
		"produceTime":  {`"1792634400123"`, `1.5`, `9223372036854775808`},
		"execQuantity": {`"1.2"`, `true`, `{}`, `123456789012345678901234567890123456789`},
		"execPrice":    {`0.0000000000001`},
		"execTime":     {`"2026-02-30 09:30:00.123"`, `"2026-10-22 09:30:00"`, `"2026-10-22T09:30:00.123Z"`, `"2026-10-22 09:30:00.1234"`},
	} {
		for _, value := range values {
			cases[field+"="+value] = replaceField(t, base, field, value)
		}
	}
	for _, field := range []string{"clientId", "accountId", "orderId", "businessId", "traceId"} {
		cases[field+" too long"] = replaceField(t, base, field, `"`+strings.Repeat("a", 129)+`"`)
	}
	cases["security code too long"] = replaceField(t, base, "secCode", `"`+strings.Repeat("a", 65)+`"`)
	cases["security name too long"] = replaceField(t, base, "secName", `"`+strings.Repeat("中", 256)+`"`)
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			trade, err := ingress.Decode(bytes.NewReader(raw))
			if !errors.Is(err, ingress.ErrInvalidTrade) {
				t.Fatalf("want invalid-trade error, got %v (trade %s)", err, trade.MsgID)
			}
			if trade.MsgID != "" || trade.RawPayload != nil {
				t.Error("invalid request returned a usable partial trade")
			}
		})
	}
}

// Catches byte-based string limits and added positivity constraints.
func TestDecodeTradeBoundaries(t *testing.T) {
	base := fixture(t)
	for _, tc := range []struct{ field, value string }{
		{"clientId", `"` + strings.Repeat("中", 128) + `"`},
		{"secCode", `"` + strings.Repeat("a", 64) + `"`},
		{"secName", `"` + strings.Repeat("中", 255) + `"`},
		{"side", `"SELL"`}, {"orderType", `"MARKET"`}, {"orderStatus", `"PARTIAL_FILLED"`}, {"market", `"SH"`},
		{"execQuantity", `0`}, {"execPrice", `-0.123456789012`},
	} {
		t.Run(tc.field+tc.value[:min(len(tc.value), 15)], func(t *testing.T) {
			if _, err := ingress.Decode(bytes.NewReader(replaceField(t, base, tc.field, tc.value))); err != nil {
				t.Fatalf("legal boundary rejected: %v", err)
			}
		})
	}
}

// Catches decimal precision/scale checks that count exponent spelling rather
// than the exact represented decimal, or accept quoted numeric fields.
func TestDecodeDecimal(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"12345678901234567890123456789012345678", true},
		{"12345678901234567890123456.123456789012", true},
		{"0.000000000001", true}, {"0", true}, {"-0", true},
		{"-123.45", true}, {"1e-12", true}, {"1.2345e2", true},
		{"1e37", true}, {"0.0000000000000", true},
		{"1.2300000000000", true},
		{"123456789012345678901234567890123456789", false},
		{"0.1234567890123", false}, {"1e-13", false}, {"1e38", false},
		{"1e999999999999999999999", false}, {"1e-999999999999999999999", false},
		{`"1.2"`, false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			trade, err := ingress.Decode(bytes.NewReader(replaceField(t, fixture(t), "execPrice", tc.value)))
			if tc.valid {
				if err != nil {
					t.Fatalf("exact decimal rejected: %v", err)
				}
				if trade.ExecPrice.String() != tc.value {
					t.Fatalf("literal changed: %s", trade.ExecPrice)
				}
			} else if !errors.Is(err, ingress.ErrInvalidTrade) {
				t.Fatalf("out-of-range decimal accepted: %v", err)
			}
		})
	}
}

// Catches counting characters instead of bytes or reading unbounded input.
func TestDecodeBodyLimit(t *testing.T) {
	base := fixture(t)
	for _, tc := range []struct {
		size     int
		tooLarge bool
	}{{65536, false}, {65537, true}} {
		raw := append(bytes.Clone(base), bytes.Repeat([]byte(" "), tc.size-len(base))...)
		_, err := ingress.Decode(bytes.NewReader(raw))
		if tc.tooLarge {
			if !errors.Is(err, ingress.ErrBodyTooLarge) {
				t.Fatalf("oversized body: want body-too-large, got %v", err)
			}
		} else if err != nil {
			t.Fatalf("exactly 64KiB rejected: %v", err)
		}
	}
	r := &countingReader{}
	if _, err := ingress.Decode(r); !errors.Is(err, ingress.ErrBodyTooLarge) {
		t.Fatalf("endless body: want body-too-large, got %v", err)
	}
	if r.read > 65537 {
		t.Fatalf("read %d bytes before rejecting", r.read)
	}
}

type countingReader struct{ read int }

func (r *countingReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = ' '
	}
	r.read += len(p)
	return len(p), nil
}

func TestDecodeReadFailure(t *testing.T) {
	_, err := ingress.Decode(io.MultiReader(bytes.NewReader(fixture(t)), failingReader{}))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("read error lost or accepted partial body: %v", err)
	}
}

func TestDecodeExchangeTimezone(t *testing.T) {
	raw := replaceField(t, fixture(t), "execTime", `"1990-07-01 09:30:00.123"`)
	trade, err := ingress.Decode(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	// Shanghai used UTC+9 in summer 1990. A fixed-offset parser would be wrong.
	want := time.Date(1990, 7, 1, 0, 30, 0, 123000000, time.UTC)
	if !trade.ExecTime.Equal(want) {
		t.Fatalf("want %v, got %v", want, trade.ExecTime.UTC())
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
