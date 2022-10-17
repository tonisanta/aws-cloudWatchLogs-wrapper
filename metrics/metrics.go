package metrics

import "github.com/prometheus/client_golang/prometheus"

type WrapperMetrics struct {
	LogsBuffered    prometheus.Gauge
	BatchSize       prometheus.Gauge
	AWSResponseTime prometheus.Histogram
}

const metricsNamespace = "aws_cloudWatchLogs_wrapper"

func New() *WrapperMetrics {
	logsBuffered := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "logs_buffered",
		Help:      "Number of logs in the internal buffer pending to be sent to aws",
	})
	prometheus.MustRegister(logsBuffered)

	batchSize := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "batch_size",
		Help:      "Size in bytes of current batch",
	})
	prometheus.MustRegister(batchSize)

	awsResponseTime := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Name:      "aws_response_time",
		Help:      "Elapsed time in milliseconds for put log events request to aws",
	})
	prometheus.MustRegister(awsResponseTime)

	return &WrapperMetrics{
		LogsBuffered:    logsBuffered,
		BatchSize:       batchSize,
		AWSResponseTime: awsResponseTime,
	}
}
