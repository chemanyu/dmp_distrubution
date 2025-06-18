package ginporm

import (
	"dmp_distribution/common"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "service"
const namespace2 = "business"
const namespace_export = "export"

var (
	serverc_labels   = []string{"status", "endpoint", "method"}
	send_http_labels = []string{"status", "endpoint", "method"}

	ServerPoolCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace2,
			Name:      "server_pool_up_send_status",
			Help:      "count ok or fail",
		},
		[]string{"status", "endpoint", "keys"},
	)

	ProductCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace2,
			Name:      "product_up_send_status",
			Help:      "count ok or fail",
		},
		[]string{"status", "endpoint", "unikey"},
	)

	ChannelCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace2,
			Name:      "chanenl_up_send_status",
			Help:      "count ok or fail",
		},
		[]string{"status", "endpoint", "unikey"},
	)
	uptime = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "uptime",
			Help:      "HTTP service uptime.",
		}, nil,
	)

	reqCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "http_request_count_total",
			Help:      "Total number of HTTP requests made.",
		}, serverc_labels,
	)

	reqDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request latencies in seconds.",
		}, serverc_labels,
	)

	reqSizeBytes = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Name:      "http_request_size_bytes",
			Help:      "HTTP request sizes in bytes.",
		}, serverc_labels,
	)

	respSizeBytes = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Name:      "http_response_size_bytes",
			Help:      "HTTP response sizes in bytes.",
		}, serverc_labels,
	)

	ExportReqDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace_export,
			Name:      "send_http_request_duration_seconds",
			Help:      "Send HTTP request latencies in seconds.",
		}, send_http_labels,
	)

	ExportReqSizeBytes = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: namespace_export,
			Name:      "send_http_request_size_bytes",
			Help:      "HTTP request sizes in bytes.",
		}, send_http_labels,
	)

	ExportRespSizeBytes = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: namespace_export,
			Name:      "send_http_response_size_bytes",
			Help:      "HTTP response sizes in bytes.",
		}, send_http_labels,
	)
)

// init registers the prometheus metrics
func init() {
	prometheus.MustRegister(uptime, reqCount, reqDuration, reqSizeBytes, respSizeBytes, ProductCounterVec, ChannelCounterVec, ExportReqSizeBytes, ExportRespSizeBytes, ExportReqDuration, ServerPoolCounterVec)
	go recordUptime()
}

// recordUptime increases service uptime per second.
func recordUptime() {
	for range time.Tick(time.Second) {
		uptime.WithLabelValues().Inc()
	}
}

// calcRequestSize returns the size of request object.
func calcRequestSize(r *http.Request) float64 {
	size := 0
	if r.URL != nil {
		size = len(r.URL.String())
	}

	size += len(r.Method)
	size += len(r.Proto)

	for name, values := range r.Header {
		size += len(name)
		for _, value := range values {
			size += len(value)
		}
	}
	size += len(r.Host)

	// r.Form and r.MultipartForm are assumed to be included in r.URL.
	if r.ContentLength != -1 {
		size += int(r.ContentLength)
	}
	return float64(size)
}

type RequestLabelMappingFn func(c *gin.Context) string

// PromOpts represents the Prometheus middleware Options.
// It is used for filtering labels by regex.
type PromOpts struct {
	ExcludeRegexStatus     string
	ExcludeRegexEndpoint   string
	ExcludeRegexMethod     string
	EndpointLabelMappingFn RequestLabelMappingFn
}

// NewDefaultOpts return the default ProOpts
func NewDefaultOpts() *PromOpts {
	return &PromOpts{
		EndpointLabelMappingFn: func(c *gin.Context) string {
			paths := strings.Split(c.Request.URL.Path, "/")
			if len(paths) > 0 {
				return "/" + paths[1]
			}
			//by default do nothing, return URL as is
			return c.Request.URL.Path
		},
	}
}

// checkLabel returns the match result of labels.
// Return true if regex-pattern compiles failed.
func (po *PromOpts) checkLabel(label, pattern string) bool {
	if pattern == "" {
		return true
	}

	matched, err := regexp.MatchString(pattern, label)
	if err != nil {
		return true
	}
	return !matched
}

// PromMiddleware returns a gin.HandlerFunc for exporting some Web metrics
func PromMiddleware(promOpts *PromOpts) gin.HandlerFunc {
	// make sure promOpts is not nil
	if promOpts == nil {
		promOpts = NewDefaultOpts()
	}

	// make sure EndpointLabelMappingFn is callable
	if promOpts.EndpointLabelMappingFn == nil {
		promOpts.EndpointLabelMappingFn = func(c *gin.Context) string {
			return c.Request.URL.Path
		}
	}

	// Pre-calculate static values
	endpoints := []string{"/clk", "/imp", "/track", "/metrics", "/ip", "/favicon.ico"}

	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		status := strconv.Itoa(c.Writer.Status())
		endpoint := promOpts.EndpointLabelMappingFn(c)
		method := c.Request.Method

		if !common.InArray(endpoint, endpoints) {
			return
		}

		lvs := []string{status, endpoint, method}

		isOk := promOpts.checkLabel(status, promOpts.ExcludeRegexStatus) &&
			promOpts.checkLabel(endpoint, promOpts.ExcludeRegexEndpoint) &&
			promOpts.checkLabel(method, promOpts.ExcludeRegexMethod)

		if !isOk {
			return
		}

		// no response content will return -1
		respSize := c.Writer.Size()
		if respSize < 0 {
			respSize = 0
		}

		updateMetrics(lvs, start, c.Request, respSize)
	}
}

// updateMetrics updates the metrics with the given label values
func updateMetrics(lvs []string, start time.Time, req *http.Request, respSize int) {
	reqCount.WithLabelValues(lvs...).Inc()
	reqDuration.WithLabelValues(lvs...).Observe(time.Since(start).Seconds())
	reqSizeBytes.WithLabelValues(lvs...).Observe(calcRequestSize(req))
	respSizeBytes.WithLabelValues(lvs...).Observe(float64(respSize))
}

// PromHandler wrappers the standard http.Handler to gin.HandlerFunc
func PromHandler(handler http.Handler) gin.HandlerFunc {
	return func(c *gin.Context) {
		handler.ServeHTTP(c.Writer, c.Request)
	}
}
