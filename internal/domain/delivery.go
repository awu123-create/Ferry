package domain

import (
	"time"

	"github.com/google/uuid"
)

// AttemptErrorKind 对单次投递尝试（Delivery Attempt）的出站 HTTP 结果进行错误分类。
type AttemptErrorKind string

const (
	AttemptErrorNone    AttemptErrorKind = ""
	AttemptErrorTimeout AttemptErrorKind = "TIMEOUT"
	AttemptErrorNetwork AttemptErrorKind = "NETWORK"
	AttemptErrorHTTP3xx AttemptErrorKind = "HTTP_3XX"
	AttemptErrorHTTP4xx AttemptErrorKind = "HTTP_4XX"
	AttemptErrorHTTP429 AttemptErrorKind = "HTTP_429"
	AttemptErrorHTTP5xx AttemptErrorKind = "HTTP_5XX"
)

// DeliveryAttempt 表示一次实际发出 HTTP 请求后生成的不可变、只追加投递尝试记录。
type DeliveryAttempt struct {
	ID              int64
	TaskID          uuid.UUID
	ReplayCount     int
	AttemptNo       int
	HTTPStatus      int
	DurationMs      int
	ErrorKind       AttemptErrorKind
	ErrorMessage    string
	ResponseSnippet string
	CreatedAt       time.Time
}

// DeliveryRequest 承载出站 HTTP 适配器发起单次投递尝试所需的全部数据。
type DeliveryRequest struct {
	TaskID        uuid.UUID
	EndpointURL   string
	SigningSecret string
	AttemptNo     int
	Timeout       time.Duration
	Headers       map[string]string
	RawPayload    []byte
	SentAt        time.Time
}

// DeliveryResult 汇总单次出站 HTTP 投递尝试的返回结果。
type DeliveryResult struct {
	HTTPStatus      int
	DurationMs      int
	ErrorKind       AttemptErrorKind
	ErrorMessage    string
	ResponseSnippet string
	RetryAfter      time.Duration
}

// ReadyDelivery 表示从就绪流拉取到的单条待投递任务句柄；处理完毕后调用 Ack 推进确认水位。
type ReadyDelivery struct {
	TaskID uuid.UUID
	Ack    func()
}
