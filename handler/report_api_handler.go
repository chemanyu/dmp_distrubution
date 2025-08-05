package handlers

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"dmp_distribution/module"
	"dmp_distribution/service"

	"github.com/gin-gonic/gin"
)

type ReportApiHandler struct {
	CommonHandler
}

var GetReportApiHandler = new(ReportApiHandler)
var distributionSvc *service.DistributionService

func init() {
	GetReportApiHandler.postMapping("report_api", getReportApi)
	GetReportApiHandler.getMapping("download", downloadFile)
}

func getReportApi(ctx *gin.Context) {
	// 创建分发服务实例
	distributionSvc = service.NewDistributionService(&module.Distribution{})
	// 启动任务调度器（会在后台持续运行）
	distributionSvc.StartTaskScheduler()

	log.Printf("[Cron] Distribution service started successfully")

	ctx.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "report_api retrieved successfully",
	})
}

// downloadFile 下载文件接口
func downloadFile(ctx *gin.Context) {
	// 获取taskId参数
	taskIdStr := ctx.Query("taskId")
	if taskIdStr == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "taskId parameter is required",
		})
		return
	}

	// 转换taskId为整数
	taskId, err := strconv.Atoi(taskIdStr)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "invalid taskId format",
		})
		return
	}

	// 从数据库获取任务信息
	distModel := &module.Distribution{}
	task, found, err := distModel.GetByID(taskId)

	if err != nil {
		log.Printf("Failed to query task %d: %v", taskId, err)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "failed to query task information",
		})
		return
	}

	if !found {
		ctx.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "task not found",
		})
		return
	}

	// 检查文件路径是否存在
	if task.Path == "" {
		ctx.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "file path not found for this task",
		})
		return
	}

	// 检查文件是否存在
	if _, err := os.Stat(task.Path); os.IsNotExist(err) {
		ctx.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "file does not exist",
		})
		return
	}

	// 获取文件名
	fileName := filepath.Base(task.Path)
	if fileName == "" || fileName == "." {
		fileName = "task_" + taskIdStr + ".txt"
	}

	// 设置响应头
	ctx.Header("Content-Description", "File Transfer")
	ctx.Header("Content-Transfer-Encoding", "binary")
	ctx.Header("Content-Disposition", "attachment; filename="+fileName)
	ctx.Header("Content-Type", "application/octet-stream")

	// 提供文件下载
	ctx.File(task.Path)

	log.Printf("File downloaded successfully: task=%d, path=%s", taskId, task.Path)
}
