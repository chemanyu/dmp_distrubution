package cron

import (
	"log"
	"sync"
	"time"

	"dmp_distribution/module"
	"dmp_distribution/service"

	"github.com/robfig/cron/v3"
)

var (
	cronInstance    *cron.Cron
	once            sync.Once
	distributionSvc *service.DistributionService
)

// InitCronJobs 初始化并启动所有定时任务
func InitCronJobs() {
	log.Printf("[Cron] Initializing cron jobs...")
	once.Do(func() {
		cronInstance = cron.New(cron.WithSeconds())
		setupDistributionJobs()
		cronInstance.Start()
		log.Printf("[Cron] Cron jobs initialized and started")
	})
}

// setupDistributionJobs 配置所有与分发相关的定时任务
func setupDistributionJobs() {
	// 添加分发任务，每5分钟执行一次
	_, err := cronInstance.AddFunc("0 */3 * * * *", func() {
		//_, err := cronInstance.AddFunc("0 0 13 * * *", func() {
		log.Printf("[Cron] Starting distribution job at %v", time.Now().Format("2006-01-02 15:04:05"))

		// 创建分发服务实例
		if distributionSvc == nil || !distributionSvc.IsRunning {
			distributionSvc = service.NewDistributionService(&module.Distribution{})
		}
		// 启动任务调度器（会在后台持续运行）
		distributionSvc.StartTaskScheduler()

		log.Printf("[Cron] Distribution service started successfully")
	})

	if err != nil {
		log.Printf("[Cron] Failed to setup distribution job: %v", err)
	}
}

// StopCronJobs 停止所有正在运行的定时任务
func StopCronJobs() {
	if cronInstance != nil {
		// 先停止定时任务
		cronInstance.Stop()
		log.Printf("[Cron] Cron scheduler stopped")

		// 再停止分发服务
		if distributionSvc != nil {
			distributionSvc.Stop()
			log.Printf("[Cron] Distribution service stopped")
		}
	}
}
