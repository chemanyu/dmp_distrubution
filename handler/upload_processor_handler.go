package handlers

import (
	"net/http"

	"dmp_distribution/service"

	"github.com/gin-gonic/gin"
)

// UploadProcessorHandler 上传处理器处理器
type UploadProcessorHandler struct {
	CommonHandler
	processor *service.UploadProcessorService
}

var UploadHandler = new(UploadProcessorHandler)

func init() {
	UploadHandler.postMapping("upload", StartProcessor)
}

// NewUploadProcessorHandler 创建新的上传处理器处理器
func NewUploadProcessorHandler() *UploadProcessorHandler {
	return &UploadProcessorHandler{
		processor: service.NewUploadProcessorService(),
	}
}

// StartProcessor 手动启动处理器
func StartProcessor(c *gin.Context) {
	processor := service.NewUploadProcessorService()
	processor.StartProcessor()
	if processor.IsRunning() {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Upload processor is already running",
		})
		return
	}

	// 异步启动处理器
	go processor.StartProcessor()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Upload processor started successfully",
	})
}
