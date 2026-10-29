package delivery

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const RetryDelivery = "DELIVERY"
const RetryRouting = "ROUTING"

var (
	ErrInvalidRetry  = errors.New("invalid retry command")
	ErrRetryNotFound = errors.New("retry target not found")
	ErrRetryConflict = errors.New("retry command conflict")
)

// RetryRequest identifies a manual command; it cannot carry trade or UID edits.
type RetryRequest struct {
	OperationID string `json:"operationId"`
	Kind        string `json:"-"`
	TargetID    string `json:"-"`
	Operator    string `json:"operator"`
	Reason      string `json:"reason"`
}

// RetryResult is the saved command result, not a live delivery confirmation.
type RetryResult struct {
	OperationID string `json:"operationId"`
	Kind        string `json:"kind"`
	TargetID    string `json:"targetId"`
	RetryCycle  int    `json:"retryCycle"`
	State       string `json:"state"`
}

type RetryRepository interface {
	ExecuteRetry(context.Context, RetryRequest) (RetryResult, error)
}

type RetryCommand struct{ repository RetryRepository }

func NewRetryCommand(repository RetryRepository) *RetryCommand {
	return &RetryCommand{repository: repository}
}

func retryUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return err == nil && len(b) == 16
}
func retryText(s string, limit int) bool {
	if !utf8.ValidString(s) || strings.TrimSpace(s) == "" || utf8.RuneCountInString(s) > limit {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
func ValidateRetryRequest(r RetryRequest) error {
	if !retryUUID(r.OperationID) || !retryUUID(r.TargetID) || (r.Kind != RetryDelivery && r.Kind != RetryRouting) || !retryText(r.Operator, 128) || !retryText(r.Reason, 512) {
		return ErrInvalidRetry
	}
	return nil
}
func (c *RetryCommand) Execute(ctx context.Context, r RetryRequest) (RetryResult, error) {
	if err := ctx.Err(); err != nil {
		return RetryResult{}, err
	}
	if err := ValidateRetryRequest(r); err != nil {
		return RetryResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.repository.ExecuteRetry(ctx, r)
}
