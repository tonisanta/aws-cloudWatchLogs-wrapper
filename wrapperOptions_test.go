package wrappercloudwatchlogs

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewWrapperOptions(t *testing.T) {
	t.Run("should fail when batch size is over MaxBatchSize", func(t *testing.T) {
		_, err := NewWrapperOptions(
			"my-logGroup",
			"my-stream",
			OneMonth,
			10,
			MaxBatchSize+1,
			time.Minute)

		assert.ErrorIs(t, err, ErrBatchSizeTooBig)
	})

	t.Run("should fail when logs per batch is over MaxLogEventsPerBatch", func(t *testing.T) {
		_, err := NewWrapperOptions(
			"my-logGroup",
			"my-stream",
			OneMonth,
			MaxLogEventsPerBatch+1,
			1024,
			time.Minute)

		assert.ErrorIs(t, err, ErrTooManyLogsPerBatch)
	})

	t.Run("should fail when sendAfter is bigger than 24 hours", func(t *testing.T) {
		_, err := NewWrapperOptions(
			"my-logGroup",
			"my-stream",
			OneMonth,
			10,
			1024,
			time.Hour*25)

		assert.ErrorIs(t, err, ErrInvalidSendAfter)
	})
}
