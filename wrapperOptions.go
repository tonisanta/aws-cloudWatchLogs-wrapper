package wrappercloudwatchlogs

import (
	"fmt"
	"time"
)

const (
	MaxBatchSize         = 1_048_576
	MaxLogEventsPerBatch = 10_000
)

type WrapperOptions struct {
	logGroup      string
	logStream     string
	retentionDays RetentionPolicy
	logsPerBatch  uint
	batchSize     uint
	sendAfter     time.Duration
}

func NewWrapperOptions(
	logGroup string,
	logStream string,
	retentionDays RetentionPolicy,
	logsPerBatch uint,
	batchSize uint,
	sendAfter time.Duration) (*WrapperOptions, error) {

	options := WrapperOptions{
		logGroup:      logGroup,
		logStream:     logStream,
		retentionDays: retentionDays,
		logsPerBatch:  logsPerBatch,
		batchSize:     batchSize,
		sendAfter:     sendAfter,
	}

	err := options.validate()
	if err != nil {
		return nil, err
	}
	return &options, nil

}

var ErrTooManyLogsPerBatch = fmt.Errorf("to many logs per batch, maxValue: %d", MaxLogEventsPerBatch)
var ErrBatchSizeTooBig = fmt.Errorf("batch size too big, maxValue: %d", MaxBatchSize)
var ErrInvalidSendAfter = fmt.Errorf("AWS restrictions: a batch of log events in a single request cannot span more than 24 hours")

// TODO: retornar errors categoritazas
func (options WrapperOptions) validate() error {
	if options.logsPerBatch > MaxLogEventsPerBatch {
		return ErrTooManyLogsPerBatch
	}

	if options.batchSize > MaxBatchSize {
		return ErrBatchSizeTooBig
	}

	if options.sendAfter >= time.Hour*24 {
		return ErrInvalidSendAfter
	}
	return nil
}

type RetentionPolicy int32

const (
	OneDay              RetentionPolicy = 1
	ThreeDays           RetentionPolicy = 3
	FiveDays            RetentionPolicy = 5
	OneWeek             RetentionPolicy = 7
	TwoWeeks            RetentionPolicy = 14
	OneMonth            RetentionPolicy = 30
	TwoMonths           RetentionPolicy = 60
	ThreeMonths         RetentionPolicy = 90
	FourMonths          RetentionPolicy = 120
	FiveMonths          RetentionPolicy = 150
	SixMonths           RetentionPolicy = 180
	OneYear             RetentionPolicy = 365
	OneYearAndOneMonth  RetentionPolicy = 400
	OneYearAndSixMonths RetentionPolicy = 545
	TwoYears            RetentionPolicy = 731
	FiveYears           RetentionPolicy = 1827
	TenYears            RetentionPolicy = 3653
)
