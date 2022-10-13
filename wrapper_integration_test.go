//go:build integration

package wrappercloudwatchlogs

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"testing"
	"time"
)

const (
	awsEndpoint = "http://localhost:4566"
	awsRegion   = "eu-west-1"
)

func TestWithAwsClient(t *testing.T) {
	customResolver := getCustomResolver()
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithEndpointResolverWithOptions(customResolver))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	now := time.Now()
	streamName := fmt.Sprintf("my-stream-%s", now.Format(time.RFC3339))
	opts := WrapperOptions{
		logGroup:      "test-group-1",
		logStream:     streamName,
		batchSize:     MaxBatchSize,
		logsPerBatch:  2,
		sendAfter:     time.Minute * 5,
		retentionDays: OneWeek,
	}

	clientAws := cloudwatchlogs.NewFromConfig(cfg)
	ctx := context.Background()
	wrapper, err := New(ctx, clientAws, &opts)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	numLogsToSend := 2
	for i := 0; i < numLogsToSend; i++ {
		wrapper.Log("Log!")
	}
	wrapper.Close()

	getLogsReq := cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  &opts.logGroup,
		LogStreamName: &opts.logStream,
	}
	getLogsResponse, err := clientAws.GetLogEvents(ctx, &getLogsReq)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	numLogsGot := len(getLogsResponse.Events)
	if numLogsGot != numLogsToSend {
		t.Errorf("number of logs doesn't match, got: %d logs", numLogsGot)
	}
}

func TestLogsAreSentAfterConfiguredTime(t *testing.T) {
	customResolver := getCustomResolver()
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithEndpointResolverWithOptions(customResolver))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	opts := WrapperOptions{
		logGroup:      "test-group-1",
		logStream:     "my-stream-2",
		batchSize:     MaxBatchSize,
		logsPerBatch:  MaxLogEventsPerBatch,
		sendAfter:     time.Second * 2,
		retentionDays: OneWeek,
	}

	clientAws := cloudwatchlogs.NewFromConfig(cfg)
	ctx := context.Background()
	wrapper, err := New(ctx, clientAws, &opts)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	numLogsToSend := 2
	for i := 0; i < numLogsToSend; i++ {
		wrapper.Log("Log!")
	}

	time.Sleep(time.Second * 4)

	getLogsReq := cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  &opts.logGroup,
		LogStreamName: &opts.logStream,
	}
	getLogsResponse, err := clientAws.GetLogEvents(ctx, &getLogsReq)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	numLogsGot := len(getLogsResponse.Events)
	if numLogsGot != numLogsToSend {
		t.Errorf("number of logs doesn't match, got: %d logs", numLogsGot)
	}
}

func getCustomResolver() aws.EndpointResolverWithOptionsFunc {
	return aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service == cloudwatchlogs.ServiceID && region == awsRegion {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           awsEndpoint,
				SigningRegion: awsRegion,
			}, nil
		}
		// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})
}
