package cron

import (
	"log"
	"sync"
	"time"

	"dmp_distribution/common/redis"

	"github.com/robfig/cron/v3"

	"dmp_distribution/module"
	"dmp_distribution/service"
)

var (
	cronInstance    *cron.Cron
	once            sync.Once
	distributionSvc *service.DistributionService
)

// InitCronJobs 初始化并启动所有定时任务
func InitCronJobs() {
	once.Do(func() {
		cronInstance = cron.New(cron.WithSeconds())
		setupDistributionJobs()
		cronInstance.Start()
	})
}

// setupDistributionJobs 配置所有与分发相关的定时任务
func setupDistributionJobs() {
	// 添加分发任务，每5分钟执行一次

	// 你可以根据需要调整 cron 表达式
	_, err := cronInstance.AddFunc("0 */5 * * * *", func() {
		log.Printf("[Cron] Starting distribution job at %v", time.Now().Format("2006-01-02 15:04:05"))

		// 如果服务已经在运行，先停止它
		if distributionSvc != nil {
			distributionSvc.Stop()
		}

		// 初始化 Redis 客户端
		rdb := redis.NewData(nil)

		// 初始化分发服务
		distributionSvc = service.NewDistributionService(&module.Distribution{}, rdb.RedisPool)

		// 启动任务调度器
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
		cronInstance.Stop()

		// 停止分发服务
		if distributionSvc != nil {
			distributionSvc.Stop()
		}

		log.Printf("[Cron] All cron jobs and services stopped")
	}
}
