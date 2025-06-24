package handlers

import (
	"log"
	"net/http"

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
