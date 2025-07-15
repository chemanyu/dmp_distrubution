package handlers

import (
	"net/http"

	"dmp_distribution/service"

	"github.com/gin-gonic/gin"
)

// UploadProcessorHandler 上传处理器处理器
type UploadProcessorHandler struct {
	processor *service.UploadProcessorService
}

// NewUploadProcessorHandler 创建新的上传处理器处理器
func NewUploadProcessorHandler() *UploadProcessorHandler {
	return &UploadProcessorHandler{
		processor: service.NewUploadProcessorService(),
	}
}

// StartProcessor 手动启动处理器
func (h *UploadProcessorHandler) StartProcessor(c *gin.Context) {
	if h.processor.IsRunning() {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Upload processor is already running",
		})
		return
	}

	// 异步启动处理器
	go h.processor.StartProcessor()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Upload processor started successfully",
	})
}

// GetStatus 获取处理器状态
func (h *UploadProcessorHandler) GetStatus(c *gin.Context) {
	status := "stopped"
	if h.processor.IsRunning() {
		status = "running"
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"processor_status": status,
		},
	})
}
