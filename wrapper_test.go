package wrappercloudwatchlogs

import (
	"ClientCloudwatchLogs/mocks"
	"context"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/mock"
	"strings"
	"testing"
	"time"
)

const (
	logsPerBatch = 4
	batchSize    = 2048
)

func TestCreateClient(t *testing.T) {
	opts := WrapperOptions{
		logGroup:      "test-group",
		logStream:     "my-stream",
		batchSize:     batchSize,
		logsPerBatch:  logsPerBatch,
		sendAfter:     time.Second * 10,
		retentionDays: OneMonth,
	}

	ctx := context.Background()
	clientAwsMock := getClientMock(t)

	t.Run("after sending a full batch + 1 extra log, the internal buffer should have 1 item", func(t *testing.T) {
		wrapper, err := New(ctx, clientAwsMock, &opts)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		msg := "Hello!"
		logEvent := types.InputLogEvent{Message: &msg}

		for i := 0; i < logsPerBatch; i++ {
			wrapper.addLogEvent(ctx, logEvent)
		}

		if len(wrapper.bufferLogEvents) != 0 {
			t.Errorf("buffer should be empty")
		}

		// when the buffer is full, should send the data and clear the buffer
		wrapper.addLogEvent(ctx, logEvent)

		if len(wrapper.bufferLogEvents) != 1 {
			t.Errorf("buffer should have only one item")
		}
	})

	t.Run("should send the data before exceeding the batchSize", func(t *testing.T) {
		wrapper, err := New(ctx, clientAwsMock, &opts)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		longString := strings.Repeat("a", 1024/4)
		logEvent := types.InputLogEvent{Message: &longString}

		wrapper.addLogEvent(ctx, logEvent)
		wrapper.addLogEvent(ctx, logEvent)
		// following log doesn't fit in the batchSize, so previous ones are sent
		wrapper.addLogEvent(ctx, logEvent)

		if len(wrapper.bufferLogEvents) != 1 {
			t.Errorf("Buffer should have one element")
		}
	})
}

func getClientMock(t *testing.T) CreateOperationsCloudwatch {
	clientMock := mocks.NewCreateOperationsCloudwatch(t)

	clientMock.EXPECT().
		CreateLogGroup(mock.Anything, mock.AnythingOfType("*cloudwatchlogs.CreateLogGroupInput")).
		Return(&cloudwatchlogs.CreateLogGroupOutput{}, nil)

	clientMock.EXPECT().
		CreateLogStream(mock.Anything, mock.AnythingOfType("*cloudwatchlogs.CreateLogStreamInput")).
		Return(&cloudwatchlogs.CreateLogStreamOutput{}, nil)

	clientMock.EXPECT().
		PutLogEvents(mock.Anything, mock.AnythingOfType("*cloudwatchlogs.PutLogEventsInput")).
		Return(&cloudwatchlogs.PutLogEventsOutput{}, nil)

	clientMock.EXPECT().
		PutRetentionPolicy(mock.Anything, mock.AnythingOfType("*cloudwatchlogs.PutRetentionPolicyInput")).
		Return(&cloudwatchlogs.PutRetentionPolicyOutput{}, nil)

	return clientMock
}
