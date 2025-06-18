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
	TaskProgressKey    = "dmp:distribution:task:progress:%d" // 新增进度跟踪
	RetryMaxTimes      = 3
	StreamBatchSize    = 1000 // 流式处理批次大小
	MaxParallelWorkers = 10   // 最大并行工作协程
	RedisBatchSize     = 500  // Redis批量操作大小
	TaskWaitStatus     = 0
	TaskRunStatus      = 1
	TaskDoneStatus     = 2
	TaskFailStatus     = 3
)

type DistributionService struct {
	distModel      *module.Distribution
	rdb            *redis.ClusterClient
	ctx            context.Context
	cancel         context.CancelFunc
	taskChan       chan *module.Distribution
	workerSem      chan struct{} // 工作协程信号量
	wg             sync.WaitGroup
	isRunning      bool
	progressTicker *time.Ticker // 进度保存定时器
}

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
	go srv.progressPersister() // 启动进度持久化协程
	return srv
}

// processTask 改造后的任务处理方法
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

// progressPersister 定期持久化进度到DB
func (s *DistributionService) progressPersister() {
	for range s.progressTicker.C {
		if !s.isRunning {
			return
		}
		// 获取所有运行中任务并保存进度
	}
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
