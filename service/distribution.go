package service

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"dmp_distribution/module"
	"dmp_distribution/platform"

	"github.com/redis/go-redis/v9"
)

const (
	TaskQueueKey       = "dmp:distribution:task:queue"
	TaskStatusKey      = "dmp:distribution:task:status:%d"
	TaskProgressKey    = "dmp:distribution:task:progress:%d"
	RetryMaxTimes      = 3
	StreamBatchSize    = 1000
	MaxParallelWorkers = 10
	RedisBatchSize     = 500
	TaskWaitStatus     = 0
	TaskRunStatus      = 1
	TaskDoneStatus     = 2
	TaskFailStatus     = 3
)

// DistributionService 分发服务
type DistributionService struct {
	distModel      *module.Distribution
	rdb            *redis.ClusterClient
	ctx            context.Context
	cancel         context.CancelFunc
	taskChan       chan *module.Distribution
	workerSem      chan struct{}
	wg             sync.WaitGroup
	isRunning      bool
	progressTicker *time.Ticker
}

// NewDistributionService 创建新的分发服务
func NewDistributionService(model *module.Distribution, rdb *redis.ClusterClient) *DistributionService {
	ctx, cancel := context.WithCancel(context.Background())
	srv := &DistributionService{
		distModel:      model,
		rdb:            rdb,
		ctx:            ctx,
		cancel:         cancel,
		taskChan:       make(chan *module.Distribution, 100),
		workerSem:      make(chan struct{}, MaxParallelWorkers),
		isRunning:      true,
		progressTicker: time.NewTicker(10 * time.Second),
	}

	// 启动必要的后台任务
	go srv.progressPersister()
	go srv.taskProcessor()

	return srv
}

// taskProcessor 处理从任务通道接收到的任务
func (s *DistributionService) taskProcessor() {
	log.Printf("Task processor started")
	for {
		select {
		case <-s.ctx.Done():
			log.Printf("Task processor stopped: context cancelled")
			return
		case task, ok := <-s.taskChan:
			if !ok {
				log.Printf("Task processor stopped: channel closed")
				return
			}

			// 获取工作协程信号量
			s.workerSem <- struct{}{}

			s.wg.Add(1)
			go func(t *module.Distribution) {
				defer s.wg.Done()
				defer func() { <-s.workerSem }() // 释放工作协程信号量

				startTime := time.Now()
				log.Printf("Processing task %d started", t.ID)

				if err := s.processTask(t); err != nil {
					log.Printf("Task %d failed after %v: %v", t.ID, time.Since(startTime), err)
					s.finalizeTask(t, TaskFailStatus, err)
				} else {
					log.Printf("Task %d completed successfully in %v", t.ID, time.Since(startTime))
					s.finalizeTask(t, TaskDoneStatus, nil)
				}
			}(task)
		}
	}
}

// IsRunning 返回服务是否正在运行
func (s *DistributionService) IsRunning() bool {
	return s.isRunning
}

// Stop 停止服务
func (s *DistributionService) Stop() {
	if !s.isRunning {
		return
	}
	s.isRunning = false

	if s.cancel != nil {
		s.cancel()
	}

	close(s.taskChan)
	s.wg.Wait()

	if s.progressTicker != nil {
		s.progressTicker.Stop()
	}

	log.Printf("Distribution service stopped gracefully")
}

// StartTaskScheduler 启动任务调度器
func (s *DistributionService) StartTaskScheduler() {
	log.Printf("Starting task scheduler")
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			log.Printf("Task scheduler stopped: context cancelled")
			return
		case <-ticker.C:
			if !s.isRunning {
				log.Printf("Task scheduler stopped: service not running")
				return
			}

			// 每次查询最多100个待处理任务
			tasks, _, err := s.distModel.List(map[string]interface{}{
				"status": TaskWaitStatus,
			}, 1, 100)
			if err != nil {
				log.Printf("List tasks error: %v", err)
				continue
			}

			for _, task := range tasks {
				select {
				case <-s.ctx.Done():
					return
				case s.taskChan <- &task:
					log.Printf("Scheduled task %d for processing", task.ID)
				default:
					log.Printf("Task channel full, skipping task %d", task.ID)
				}
			}
		}
	}
}

// progressPersister 定期持久化进度
func (s *DistributionService) progressPersister() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.progressTicker.C:
			if !s.isRunning {
				return
			}
			// 这里可以实现进度持久化逻辑
		}
	}
}

// processTask 处理单个任务
func (s *DistributionService) processTask(task *module.Distribution) error {
	// 1. 初始化任务状态
	if err := s.initTaskStatus(task); err != nil {
		return err
	}

	// 2. 流式处理文件
	processor := func(batch []map[string]string) error {
		return s.processBatch(task, batch)
	}

	if err := s.streamProcessFile(task, processor); err != nil {
		s.finalizeTask(task, TaskFailStatus, err)
		return err
	}

	// 3. 最终状态更新
	return s.finalizeTask(task, TaskDoneStatus, nil)
}

// streamProcessFile 流式处理大文件
func (s *DistributionService) streamProcessFile(task *module.Distribution, processor func([]map[string]string) error) error {

	file, err := os.Open(task.Path)
	if err != nil {
		return fmt.Errorf("open file error: %w", err)
	}
	defer file.Close()

	var (
		scanner   = bufio.NewScanner(file)
		batch     = make([]map[string]string, 0, StreamBatchSize)
		lineCount int
	)

	for scanner.Scan() {
		if !s.isRunning {
			return fmt.Errorf("service stopped")
		}

		deviceInfo, valid := s.parseLine(scanner.Text(), task)
		if !valid {
			continue
		}

		batch = append(batch, deviceInfo)
		lineCount++

		// 批次处理
		if len(batch) >= StreamBatchSize {
			if err := processor(batch); err != nil {
				return err
			}
			batch = batch[:0] // 清空批次
			s.updateProgress(task.ID, lineCount)
		}
	}

	// 处理剩余数据
	if len(batch) > 0 {
		if err := processor(batch); err != nil {
			return err
		}
		s.updateProgress(task.ID, lineCount)
	}

	return scanner.Err()
}

// processBatch 处理单个批次
func (s *DistributionService) processBatch(task *module.Distribution, batch []map[string]string) error {
	platformServers, err := platform.Servers.Get(task.Platform)
	if err != nil {
		return fmt.Errorf("get platform servers error: %w", err)
	}
	defer platform.Servers.Put(task.Platform, platformServers)

	// 分批提交到Redis
	for i := 0; i < len(batch); i += RedisBatchSize {
		end := i + RedisBatchSize
		if end > len(batch) {
			end = len(batch)
		}

		retry := 0
		for retry <= RetryMaxTimes {
			if err := platformServers.Distribution(task, batch[i:end]); err == nil {
				break
			}
			retry++
			time.Sleep(time.Second * time.Duration(retry))
		}

		if retry > RetryMaxTimes {
			return fmt.Errorf("max retries exceeded for batch %d-%d", i, end)
		}
	}
	return nil
}

// ========== 辅助方法 ========== //

// parseLine 优化后的行解析
func (s *DistributionService) parseLine(line string, task *module.Distribution) (map[string]string, bool) {
	fields := strings.Split(line, ",")
	if len(fields) == 0 {
		return nil, false
	}

	deviceInfo := make(map[string]string)
	if task.Imei == 1 && len(fields) > 0 {
		if imei := s.cleanIMEI(fields[0]); imei != "" {
			deviceInfo["imei"] = imei
		}
	}
	// 其他字段处理...
	if task.Oaid == 1 && len(fields) > 1 {
		if oaid := s.cleanOAID(fields[1]); oaid != "" {
			deviceInfo["oaid"] = oaid
		}
	}
	if task.Idfa == 1 && len(fields) > 2 {
		if idfa := s.cleanIDFA(fields[2]); idfa != "" {
			deviceInfo["idfa"] = idfa
		}
	}
	if task.Caid == 1 && len(fields) > 3 {
		deviceInfo["caid"] = fields[3]
	}
	if task.Caid2 == 1 && len(fields) > 4 {
		deviceInfo["caid2"] = fields[4]
	}
	if task.UserID == 1 && len(fields) > 5 {
		deviceInfo["user_id"] = fields[5]
	}

	return deviceInfo, len(deviceInfo) > 0
}

// initTaskStatus 任务初始化
func (s *DistributionService) initTaskStatus(task *module.Distribution) error {
	if err := s.distModel.UpdateStatus(task.ID, TaskRunStatus); err != nil {
		return fmt.Errorf("update status error: %w", err)
	}
	s.rdb.Set(s.ctx, fmt.Sprintf(TaskProgressKey, task.ID), "0", 0)
	return nil
}

// finalizeTask 任务收尾
func (s *DistributionService) finalizeTask(task *module.Distribution, status int, err error) error {
	if status == TaskFailStatus {
		log.Printf("Task %d failed: %v", task.ID, err)
	}
	s.rdb.Del(s.ctx, fmt.Sprintf(TaskProgressKey, task.ID))
	return s.distModel.UpdateStatus(task.ID, status)
}

// updateProgress 更新进度
func (s *DistributionService) updateProgress(taskID int, processed int) {
	s.rdb.Set(s.ctx, fmt.Sprintf(TaskProgressKey, taskID), processed, 0)
}

func (s *DistributionService) cleanIMEI(imei string) string {
	// 去除空格和特殊字符
	imei = strings.TrimSpace(imei)
	imei = strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, imei)

	// 验证IMEI长度
	if len(imei) != 15 {
		return ""
	}

	return imei
}

// cleanOAID 清洗OAID数据
func (s *DistributionService) cleanOAID(oaid string) string {
	return strings.TrimSpace(oaid)
}

// cleanIDFA 清洗IDFA数据
func (s *DistributionService) cleanIDFA(idfa string) string {
	idfa = strings.TrimSpace(strings.ToUpper(idfa))
	if len(idfa) != 36 { // UUID格式
		return ""
	}
	return idfa
}
