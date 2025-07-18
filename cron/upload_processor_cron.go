package cron

import (
	"log"
	"time"

	"dmp_distribution/service"

	"github.com/robfig/cron/v3"
)

var (
	uploadProcessorSvc *service.UploadProcessorService
)

// InitUploadCronJobs 初始化上传相关定时任务
func InitUploadCronJobs() {
	log.Printf("[Cron] Initializing upload cron jobs...")
	once.Do(func() {
		cronInstance = cron.New(cron.WithSeconds())
		setupUploadJobs()
		cronInstance.Start()
		log.Printf("[Cron] Upload cron jobs initialized and started")
	})
}

func setupUploadJobs() {
	// 初始化上传处理服务（只创建一次）
	if uploadProcessorSvc == nil {
		uploadProcessorSvc = service.NewUploadProcessorService()
	}

	// 添加上传任务，每5分钟检查一次服务状态（因为现在处理器会持续运行）
	_, err := cronInstance.AddFunc("0 */5 * * * *", func() {
		log.Printf("[Cron] Checking upload processor status at %v", time.Now().Format("2006-01-02 15:04:05"))

		// 检查是否已经在运行，如果没有则启动
		if !uploadProcessorSvc.IsRunning() {
			log.Printf("[Cron] Upload processor not running, starting...")
			uploadProcessorSvc.StartProcessor()
		} else {
			log.Printf("[Cron] Upload processor is running normally")
		}
	})

	if err != nil {
		log.Printf("[Cron] Failed to setup upload job: %v", err)
	}
}
