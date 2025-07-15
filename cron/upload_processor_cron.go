package cron

import (
	"log"
	"time"

	"dmp_distribution/service"
)

// UploadProcessorCron 上传处理定时任务
type UploadProcessorCron struct {
	processor *service.UploadProcessorService
	ticker    *time.Ticker
	stopChan  chan struct{}
}

// NewUploadProcessorCron 创建新的上传处理定时任务
func NewUploadProcessorCron() *UploadProcessorCron {
	return &UploadProcessorCron{
		processor: service.NewUploadProcessorService(),
		stopChan:  make(chan struct{}),
	}
}

// Start 启动定时任务
func (c *UploadProcessorCron) Start() {
	// 每分钟检查一次待处理的记录
	c.ticker = time.NewTicker(1 * time.Minute)

	log.Printf("Upload processor cron started, checking every minute")

	// 立即执行一次
	go c.runProcessor()

	// 定时执行
	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.runProcessor()
			case <-c.stopChan:
				log.Printf("Upload processor cron stopped")
				return
			}
		}
	}()
}

// Stop 停止定时任务
func (c *UploadProcessorCron) Stop() {
	if c.ticker != nil {
		c.ticker.Stop()
	}
	close(c.stopChan)
}

// runProcessor 运行处理器
func (c *UploadProcessorCron) runProcessor() {
	// 如果处理器已经在运行，则跳过
	if c.processor.IsRunning() {
		log.Printf("Upload processor is already running, skipping this cycle")
		return
	}

	log.Printf("Starting upload processor cycle")
	c.processor.StartProcessor()
}
