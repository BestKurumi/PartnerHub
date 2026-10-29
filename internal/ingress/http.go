package ingress

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
	"log/slog"
	"net/http"
	"partner-hub/internal/model"
	"time"
)

type Processor interface {
	Process(context.Context, model.TradeEvent) (model.TradeResponse, error)
}
type GateConfig struct {
	MaxInFlight int
	Rate        float64
	Burst       int
}

// HTTPObservation contains only server-generated IDs, successfully decoded
// correlation fields and fixed result classifications. It never carries body,
// credentials or raw errors. The optional observer cannot alter the response.
type HTTPObservation struct {
	Kind, RequestID, MsgID, TraceID, DeliveryID, Platform, Outcome, Status string
}

func NewHTTP(p Processor, config GateConfig) http.Handler {
	return NewHTTPWithObserver(p, config, nil)
}

func NewHTTPWithObserver(p Processor, config GateConfig, observer func(HTTPObservation)) http.Handler {
	if config.MaxInFlight <= 0 {
		config.MaxInFlight = 64
	}
	if config.Rate <= 0 {
		config.Rate = 200
	}
	if config.Burst <= 0 {
		config.Burst = 40
	}
	slots := make(chan struct{}, config.MaxInFlight)
	limiter := rate.NewLimiter(rate.Limit(config.Rate), config.Burst)
	engine := gin.New()
	engine.Use(gin.CustomRecoveryWithWriter(nil, func(c *gin.Context, _ any) {
		slog.Error("trade handler panic", "status", "INTERNAL_ERROR")
		c.AbortWithStatus(http.StatusInternalServerError)
	}))
	engine.POST("/api/v1/trades", func(c *gin.Context) {
		observation := HTTPObservation{RequestID: model.NewID()}
		c.Header("X-Request-ID", observation.RequestID)
		emit := func(o HTTPObservation) {
			if observer != nil && o.Kind != "" {
				// Observability must not turn a successful admission/rejection into
				// an ambiguous processing failure if an optional callback panics.
				func() { defer func() { _ = recover() }(); observer(o) }()
			}
		}
		defer func() {
			if recovered := recover(); recovered != nil {
				observation.Kind, observation.Status = "ingress_panic", "INTERNAL_ERROR"
				emit(observation)
				panic(recovered)
			}
		}()
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), receivedAtKey{}, time.Now().UTC()))
		reject := func(code int, kind string) {
			observation.Kind, observation.Status = kind, "REJECTED"
			emit(observation)
			if code == 429 {
				c.Header("Retry-After", "1")
			}
			c.JSON(code, gin.H{"status": "REJECTED", "acceptance": "REJECTED"})
		}
		if c.Request.ContentLength > maxBodyBytes {
			reject(413, "ingress_body_too_large")
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			reject(429, "ingress_in_flight_limited")
			return
		}
		if !limiter.Allow() {
			reject(429, "ingress_rate_limited")
			return
		}
		trade, err := Decode(c.Request.Body)
		if err != nil {
			if errors.Is(err, ErrBodyTooLarge) {
				reject(413, "ingress_body_too_large")
			} else {
				reject(400, "ingress_invalid")
			}
			return
		}
		observation.MsgID, observation.TraceID = trade.MsgID, trade.TraceID
		response, err := p.Process(c.Request.Context(), trade)
		if err != nil {
			observation.Kind, observation.Status = "ingress_unconfirmed", "UNCONFIRMED"
			emit(observation)
			response.Acceptance = "UNCONFIRMED"
			response.RecordStatus = "UNCONFIRMED"
			c.JSON(503, response)
			return
		}
		code := http.StatusOK
		if response.Status == "RECOVERY_PENDING" {
			code = http.StatusAccepted
		}
		if response.Status != "SUCCEEDED" && response.Status != "NO_ROUTE" {
			observation.Kind, observation.Status = "ingress_first_abnormal", response.Status
			emitted := false
			for _, result := range response.PlatformResults {
				if result.FirstOutcome == model.Success {
					continue
				}
				o := observation
				o.Platform, o.DeliveryID, o.Outcome = result.PlatformID, result.DeliveryID, string(result.FirstOutcome)
				emit(o)
				emitted = true
			}
			if !emitted {
				emit(observation)
			}
		}
		c.JSON(code, response)
	})
	return engine
}
