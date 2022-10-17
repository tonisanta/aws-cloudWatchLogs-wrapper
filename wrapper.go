package wrappercloudwatchlogs

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/tonisanta/aws-cloudWatchLogs-wrapper/metrics"
	"log"
	"time"
)

const charSize = 4

type CloudwatchLogsWrapper struct {
	client            CreateOperationsCloudwatch
	options           *WrapperOptions
	lastSequenceToken *string
	currentBatchSize  uint
	bufferLogEvents   []types.InputLogEvent
	logEvents         chan types.InputLogEvent
	notifyClose       chan struct{}
	closeDone         chan struct{}
	tickerSendToAws   *time.Ticker
	metrics           *metrics.WrapperMetrics
}

type CreateOperationsCloudwatch interface {
	CreateLogGroup(ctx context.Context, params *cloudwatchlogs.CreateLogGroupInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error)
	CreateLogStream(ctx context.Context, params *cloudwatchlogs.CreateLogStreamInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error)
	PutLogEvents(ctx context.Context, params *cloudwatchlogs.PutLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error)
	PutRetentionPolicy(ctx context.Context, params *cloudwatchlogs.PutRetentionPolicyInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutRetentionPolicyOutput, error)
}

func New(ctx context.Context, client CreateOperationsCloudwatch, options *WrapperOptions) (*CloudwatchLogsWrapper, error) {
	clg := cloudwatchlogs.CreateLogGroupInput{LogGroupName: &options.logGroup}
	_, err := client.CreateLogGroup(ctx, &clg)
	if err != nil && !resourceAlreadyExists(err) {
		return nil, err
	}

	retPolicy := cloudwatchlogs.PutRetentionPolicyInput{
		LogGroupName:    &options.logGroup,
		RetentionInDays: (*int32)(&options.retentionDays),
	}
	_, err = client.PutRetentionPolicy(ctx, &retPolicy)
	if err != nil {
		return nil, err
	}

	cls := cloudwatchlogs.CreateLogStreamInput{LogGroupName: &options.logGroup, LogStreamName: &options.logStream}
	_, err = client.CreateLogStream(ctx, &cls)
	if err != nil {
		return nil, err
	}

	clwWrapper := &CloudwatchLogsWrapper{
		client:          client,
		options:         options,
		bufferLogEvents: make([]types.InputLogEvent, 0, options.logsPerBatch),
		logEvents:       make(chan types.InputLogEvent, options.logsPerBatch),
		notifyClose:     make(chan struct{}),
		closeDone:       make(chan struct{}),
		tickerSendToAws: time.NewTicker(options.sendAfter),
		metrics:         metrics.New(),
	}

	go clwWrapper.handleRequests(ctx)

	return clwWrapper, nil
}

func resourceAlreadyExists(err error) bool {
	var ae *types.ResourceAlreadyExistsException
	alreadyExists := errors.As(err, &ae)
	return alreadyExists
}

func (c *CloudwatchLogsWrapper) handleRequests(ctx context.Context) {
	finish := false
	for {
		select {
		case <-c.tickerSendToAws.C:
			c.sendToCloudwatch(ctx)
		case logEvent := <-c.logEvents:
			c.addLogEvent(ctx, logEvent)
		case <-c.notifyClose:
			finish = true
		}

		if finish && len(c.logEvents) == 0 {
			log.Println("finish requested and no pending messages")
			c.sendToCloudwatch(ctx)
			c.closeDone <- struct{}{}
			return
		}
	}
}

func (c *CloudwatchLogsWrapper) Log(logMessage string) {
	unixUTC := time.Now().UTC().UnixMilli()
	logEvent := types.InputLogEvent{
		Message:   &logMessage,
		Timestamp: &unixUTC,
	}
	c.logEvents <- logEvent
}

func (c *CloudwatchLogsWrapper) addLogEvent(ctx context.Context, logEvent types.InputLogEvent) {
	sizeLog := uint(len(*logEvent.Message) * charSize)
	if sizeLog > c.options.batchSize {
		log.Printf("log skipped as it exceeds batch size\n")
		return
	}

	overSizeLimit := c.currentBatchSize+sizeLog > c.options.batchSize
	availablePositions := c.getLenBuffer() < c.options.logsPerBatch
	fitsInCurrentBatch := !overSizeLimit && availablePositions

	if fitsInCurrentBatch {
		c.addLogToBuffer(logEvent, sizeLog)
	}

	batchIsFull := c.getLenBuffer() == c.options.logsPerBatch
	if overSizeLimit || batchIsFull {
		c.sendToCloudwatch(ctx)
	}

	if !fitsInCurrentBatch {
		c.addLogToBuffer(logEvent, sizeLog)
	}
}

func (c *CloudwatchLogsWrapper) getLenBuffer() uint {
	return uint(len(c.bufferLogEvents))
}

func (c *CloudwatchLogsWrapper) addLogToBuffer(logEvent types.InputLogEvent, sizeLog uint) {
	c.bufferLogEvents = append(c.bufferLogEvents, logEvent)
	c.metrics.LogsBuffered.Add(1)
	c.currentBatchSize += sizeLog
	c.metrics.BatchSize.Add(float64(sizeLog))
}

func (c *CloudwatchLogsWrapper) sendToCloudwatch(ctx context.Context) {
	if c.getLenBuffer() == 0 {
		log.Printf("no elements to send\n")
		return
	}

	log.Printf("sending to AWS\n")
	putLogsEvents := cloudwatchlogs.PutLogEventsInput{
		LogEvents:     c.bufferLogEvents,
		LogGroupName:  &c.options.logGroup,
		LogStreamName: &c.options.logStream,
		SequenceToken: c.lastSequenceToken,
	}

	start := time.Now()
	putLogEventsOutput, err := c.client.PutLogEvents(ctx, &putLogsEvents)
	c.metrics.AWSResponseTime.Observe(time.Since(start).Seconds())
	if err != nil {
		log.Printf("error while sending logs to cloudwatch: %s\n", err)
		return
	}

	if putLogEventsOutput.RejectedLogEventsInfo != nil {
		log.Printf("error whith some of the logs: %#v\n", putLogEventsOutput.RejectedLogEventsInfo)
	}

	c.currentBatchSize = 0
	c.bufferLogEvents = c.bufferLogEvents[:0] // clear the buffer
	c.lastSequenceToken = putLogEventsOutput.NextSequenceToken
	c.tickerSendToAws.Reset(c.options.sendAfter) // to avoid re-sending after a short period since last time
	c.metrics.LogsBuffered.Set(0)
	c.metrics.BatchSize.Set(0)
	log.Printf("logs uploaded succesfully\n")
}

func (c *CloudwatchLogsWrapper) Close() {
	c.notifyClose <- struct{}{}
	<-c.closeDone // waits until sending pending logs
}
