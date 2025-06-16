package core

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	ginprom "dmp_distribution/common/ginporm"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/valyala/fasthttp"

	jsoniter "github.com/json-iterator/go"
)

var ReqClient *http.Client
var fastClient *fasthttp.Client

func init() {
	fastClient = &fasthttp.Client{
		ReadTimeout:              600 * time.Millisecond,
		WriteTimeout:             3000 * time.Millisecond,
		NoDefaultUserAgentHeader: true,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true, // 设置为 true 来禁用证书验证
		},
	}

	if ReqClient == nil {
		// 发送请求
		tr := &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			MaxIdleConns:      100,              // Maximum number of idle connections to keep
			IdleConnTimeout:   90 * time.Second, // Idle connection timeout
			DisableKeepAlives: false,            // Enable keep-alive connections
		}

		ReqClient = &http.Client{Transport: tr, Timeout: 5 * time.Second}
	}
}

func HttpGet(urlpath string, resp_exec func(content []byte) error) error {
	start := time.Now()

	req, err := http.NewRequest("GET", urlpath, nil)
	if err != nil {
		return errors.New(err.Error() + "         [" + urlpath + "]")
	}

	resp, err := ReqClient.Do(req)

	if err != nil {
		return err
	}
	endpoint := CallerName()
	defer func() {
		status := fmt.Sprintf("%d", resp.StatusCode)

		method := req.Method
		lvs := []string{status, endpoint, method}
		respSize := int64(0)
		if resp.ContentLength > 0 {
			respSize = resp.ContentLength
		}

		sendReqHttpSize := sendRequestSize(req)
		ginprom.ExportReqSizeBytes.WithLabelValues(lvs...).Observe(sendReqHttpSize)
		ginprom.ExportReqDuration.WithLabelValues(lvs...).Observe(time.Since(start).Seconds())
		ginprom.ExportRespSizeBytes.WithLabelValues(lvs...).Observe(float64(respSize))
	}()
	defer resp.Body.Close()
	if resp_exec != nil {
		respContent, err := ReadAllOptimized(resp.Body)
		if err != nil {
			return err
		}
		err = resp_exec(respContent)
		if err != nil {
			return err
		}
	}
	return nil
}

func HttpPost(urlpath string, params interface{}, handlers map[string]string, response_call func(content []byte) error) error {
	//如果params 是字节类型HttpPost

	if urlpath == "" {
		return errors.New("urlpath is empty")
	}
	var body []byte
	b, ok := params.([]byte)
	if ok {
		body = b
	} else {
		body, _ = jsoniter.Marshal(params)
	}

	start := time.Now()

	req, err := http.NewRequest("POST", urlpath, bytes.NewBuffer(body))
	if err != nil {
		fmt.Println(err.Error())
		return errors.New(err.Error() + "         [" + urlpath + "]")
	}
	req.Header.Add("Content-Type", "application/json")

	if handlers != nil {
		for k, v := range handlers {
			req.Header.Add(k, v)
		}
	}

	resp, err := ReqClient.Do(req)

	if err != nil {
		fmt.Println(err.Error())
		return errors.New(err.Error() + "         [" + urlpath + "]")
	}
	endpoint := CallerName()

	defer func() {
		status := fmt.Sprintf("%d", resp.StatusCode)
		method := req.Method
		lvs := []string{status, endpoint, method}

		respSize := int64(0)
		if resp.ContentLength > 0 {
			respSize = resp.ContentLength
		}

		sendReqHttpSize := sendRequestSize(req)
		ginprom.ExportReqSizeBytes.WithLabelValues(lvs...).Observe(sendReqHttpSize)
		ginprom.ExportReqDuration.WithLabelValues(lvs...).Observe(time.Since(start).Seconds())
		ginprom.ExportRespSizeBytes.WithLabelValues(lvs...).Observe(float64(respSize))
	}()

	// if resp.StatusCode != http.StatusOK {
	// 	fmt.Println(resp.StatusCode)
	// 	return errors.New("http status err :[" + resp.Status + "]         [" + url + "]")
	// }
	defer resp.Body.Close()
	respContent, err := ReadAllOptimized(resp.Body)
	//回调处理验证状态
	if response_call != nil || resp.StatusCode != http.StatusOK {
		if err != nil {
			return err
		}
		//fmt.Println(url, string(body))
		err = response_call(respContent)
		if err != nil {
			return err
		}
	}
	return nil
}

// sendRequestSize returns the size of request object.
func sendRequestSize(r *http.Request) float64 {
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

func estimateRequestSize(req *fasthttp.Request) float64 {
	// 计算请求行的大小 (方法 + 路径 + HTTP版本 + CRLF)
	requestLineSize := len(req.Header.Method()) + len(req.URI().RequestURI()) + len(" HTTP/1.1\r\n")

	// 计算头部的大小
	headersSize := 0
	req.Header.VisitAll(func(key, value []byte) {
		headersSize += len(key) + len(": ") + len(value) + len("\r\n")
	})

	// 如果是POST或PUT请求，还需要加上body的大小
	bodySize := len(req.Body())

	// 最后不要忘记添加头部结束的CRLF
	headerEndSize := len("\r\n")

	// 总请求大小
	totalSize := requestLineSize + headersSize + headerEndSize + bodySize

	return float64(totalSize)
}

// 创建一个sync.Pool对象，用于缓存[]byte类型的切片
var bufferPool = sync.Pool{
	New: func() interface{} {
		// 根据需要可以调整初始化大小
		return make([]byte, 1024) // 32KB
	},
}

// ReadAllOptimized 是对 ioutil.ReadAll 的优化版本，
// 它使用 sync.Pool 来重用 []byte 切片。
func ReadAllOptimized(r io.Reader) ([]byte, error) {
	buf := bufferPool.Get().([]byte)
	defer bufferPool.Put(buf)

	var result bytes.Buffer
	for {
		n, err := r.Read(buf)
		if n > 0 {
			result.Write(buf[:n])
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return result.Bytes(), nil
}

// 发送请求(http_method = POST|GET)
func Fast_Http(body_string, sendurl, content_type, http_method string, response_call func(content []byte) error) error {
	start := time.Now()
	method := "POST"
	if http_method == "GET" {
		method = "GET"
	}

	if sendurl == "" || (body_string == "" && method == "POST") {
		//log_str := fmt.Sprintf("[ssp_global Fast_Http sendurl or body_string is empty]")
		//GMsLog.LogFile(log_str)
		return nil
	}
	//超时设置
	//var fastClient *fasthttp.Client

	// fastClient = &fasthttp.Client{
	// 	ReadTimeout:  time.Duration(readTimeout) * time.Millisecond,
	// 	WriteTimeout: time.Duration(writeTimeout) * time.Millisecond,
	// }

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req) // 用完需要释放资源
	req.SetRequestURI(sendurl)         //设置请求的url
	req.SetBody([]byte(body_string))   //存储转换好的数据
	req.Header.SetMethod(method)       //设置请求方法

	if content_type == "json" {
		req.Header.Set("Content-Type", "application/json; charset=UTF-8")
		req.Header.Set("Accept", "application/json; charset=UTF-8")
	} else if content_type == "protobuf" {
		req.Header.Set("Content-Type", "application/x-protobuf; charset=UTF-8")
	} else if content_type == "protobuf2" {
		req.Header.Set("Content-Type", "application/octet-stream; charset=UTF-8")
	} else {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	//req.Header.Set("Accept-Encoding", "gzip")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp) // 用完需要释放资源
	endpoint := CallerName()
	defer func() {
		status := fmt.Sprintf("%d", resp.StatusCode())
		method := method
		lvs := []string{status, endpoint, method}
		respSize := float64(len(resp.Header.Header()) + len(resp.Body()))
		sendReqHttpSize := float64(len(req.Header.Header()) + len(req.Body()))
		ginprom.ExportReqSizeBytes.WithLabelValues(lvs...).Observe(sendReqHttpSize)
		ginprom.ExportReqDuration.WithLabelValues(lvs...).Observe(time.Since(start).Seconds())
		ginprom.ExportRespSizeBytes.WithLabelValues(lvs...).Observe(respSize)
	}()

	//设置读超时时间
	if err := fastClient.Do(req, resp); err != nil {
		log_str := fmt.Sprintf("[ssp_global Fast_Http fastClient.Do err][err=%s][send_url=%s]", err.Error(), sendurl)
		return errors.New(log_str)
	}

	//应答处理
	if resp.StatusCode() == 200 {
		var body []byte
		if strings.ToLower(string(resp.Header.Peek("Content-Encoding"))) == "gzip" {
			body, _ = GzipDecode(resp.Body())
		} else {
			body = resp.Body()
		}
		return response_call(body)
	} else {
		return nil
	}
}

// gzip字符解压缩
func GzipDecode(in []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out, err
	}
	defer reader.Close()
	return ReadAllOptimized(reader)
}

func CallerName() string {
	pc, _, _, ok := runtime.Caller(2) // 传入1表示获取上一级调用者的信息
	if !ok {
		return "unknown"
	}

	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return "unknown"
	}

	return parseMethodCall(fn.Name())
}

// 解析方法调用的函数
func parseMethodCall(input string) string {
	// 找到最后一个 . 的位置
	lastDot := strings.LastIndex(input, ".")
	// 找到 * 的位置
	asteriskIndex := strings.Index(input, "*")
	// 提取类型名称
	var typeName string
	if asteriskIndex != -1 && asteriskIndex < lastDot {
		// 如果有 `*`，则提取符号后面的部分
		typeName = input[asteriskIndex+1 : lastDot]
	} else {
		// 否则直接提取 . 之前的部分
		typeName = input[:lastDot]
	}
	typeName = strings.ReplaceAll(typeName, ")", "")
	// 拼接结果
	return typeName + input[lastDot:]
}
