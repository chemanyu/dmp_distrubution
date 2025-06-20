package service

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dmp_distribution/module"
	"dmp_distribution/platform"

	"github.com/redis/go-redis/v9"
)

const (
	// TaskQueueKey       = "dmp:distribution:task:queue"
	// TaskStatusKey      = "dmp:distribution:task:status:%d"
	// TaskProgressKey    = "dmp:distribution:task:progress:%d"
	RetryMaxTimes      = 3
	StreamBatchSize    = 1000
	MaxParallelWorkers = 10
	RedisBatchSize     = 500
	TaskWaitStatus     = 1
	TaskRunStatus      = 2
	TaskDoneStatus     = 3
	TaskFailStatus     = 4

	// 进度更新相关常量
	ProgressUpdateInterval = 60 * time.Second // 进度更新间隔
	MinProgressDiff        = 50000            // 最小更新差异
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

	// 进度追踪
	progressMap sync.Map // 用于存储每个任务的进度
	//progressMutex  sync.RWMutex      // 用于保护进度更新
	lastUpdateTime map[int]time.Time // 记录每个任务最后更新时间
}

// taskProgress 任务进度结构
type taskProgress struct {
	currentCount int64     // 当前处理行数
	lastDBCount  int64     // 上次写入数据库的行数
	lastUpdate   time.Time // 上次更新时间
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
		progressTicker: time.NewTicker(ProgressUpdateInterval),
		lastUpdateTime: make(map[int]time.Time),
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

// progressPersister 定期将内存中的进度持久化到数据库
func (s *DistributionService) progressPersister() {
	for {
		select {
		case <-s.ctx.Done():
			s.flushAllProgress() // 服务停止时，确保刷新所有进度
			return
		case <-s.progressTicker.C:
			if !s.isRunning {
				return
			}
			s.flushProgress()
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
		batchSize int64
	)

	// 初始化进度记录
	s.progressMap.Store(task.ID, &taskProgress{
		lastUpdate: time.Now(),
	})

	for scanner.Scan() {
		if !s.isRunning {
			return fmt.Errorf("service stopped")
		}

		deviceInfo, valid := s.parseLine(scanner.Text(), task)
		if !valid {
			continue
		}

		batch = append(batch, deviceInfo)
		batchSize++

		// 批次处理
		if len(batch) >= StreamBatchSize {
			if err := processor(batch); err != nil {
				return err
			}
			s.updateProgress(task.ID, batchSize)
			batch = batch[:0] // 清空批次
			batchSize = 0
		}
	}

	// 处理剩余数据
	if len(batch) > 0 {
		if err := processor(batch); err != nil {
			return err
		}
		s.updateProgress(task.ID, batchSize)
	}

	// 确保最终进度被写入数据库
	s.flushProgress()

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
			if err := platformServers.Distribution(s.rdb, task, batch[i:end]); err == nil {
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
	// 初始化行数为0
	if err := s.distModel.UpdateLineCount(task.ID, 0); err != nil {
		return fmt.Errorf("init line count error: %w", err)
	}
	return nil
}

// finalizeTask 任务收尾
func (s *DistributionService) finalizeTask(task *module.Distribution, status int, err error) error {
	if status == TaskFailStatus {
		log.Printf("Task %d failed: %v", task.ID, err)
	}
	return s.distModel.UpdateStatus(task.ID, status)
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

// flushProgress 将内存中的进度刷新到数据库
func (s *DistributionService) flushProgress() {
	now := time.Now()

	s.progressMap.Range(func(key, value interface{}) bool {
		taskID := key.(int)
		progress := value.(*taskProgress)

		// 检查是否需要更新
		currentCount := atomic.LoadInt64(&progress.currentCount)
		lastDBCount := atomic.LoadInt64(&progress.lastDBCount)

		// 只有当处理行数显著增加或距离上次更新时间较长时才更新
		if currentCount-lastDBCount >= MinProgressDiff ||
			now.Sub(progress.lastUpdate) >= ProgressUpdateInterval {

			if err := s.distModel.UpdateLineCount(taskID, currentCount); err != nil {
				log.Printf("Failed to update line count for task %d: %v", taskID, err)
			} else {
				atomic.StoreInt64(&progress.lastDBCount, currentCount)
				progress.lastUpdate = now
			}
		}

		return true
	})
}

// flushAllProgress 刷新所有进度到数据库
func (s *DistributionService) flushAllProgress() {
	s.progressMap.Range(func(key, value interface{}) bool {
		taskID := key.(int)
		progress := value.(*taskProgress)

		currentCount := atomic.LoadInt64(&progress.currentCount)
		if err := s.distModel.UpdateLineCount(taskID, currentCount); err != nil {
			log.Printf("Failed to flush final line count for task %d: %v", taskID, err)
		}

		return true
	})
}

// updateProgress 更新处理进度（内存中）
func (s *DistributionService) updateProgress(taskID int, delta int64) {
	// 获取或创建进度记录
	value, _ := s.progressMap.LoadOrStore(taskID, &taskProgress{
		lastUpdate: time.Now(),
	})
	progress := value.(*taskProgress)

	// 原子递增计数
	atomic.AddInt64(&progress.currentCount, delta)
}
