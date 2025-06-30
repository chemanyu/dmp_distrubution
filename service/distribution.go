package service

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dmp_distribution/module"
	"dmp_distribution/platform"
)

const (
	// TaskQueueKey       = "dmp:distribution:task:queue"
	// TaskStatusKey      = "dmp:distribution:task:status:%d"
	// TaskProgressKey    = "dmp:distribution:task:progress:%d"
	RetryMaxTimes      = 3
	StreamBatchSize    = 1000
	MaxParallelWorkers = 10
	RedisBatchSize     = 500
	TaskNoStartStatus  = 0 // 任务未开始状态
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
	crowdRule      *module.CrowdRule
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
func NewDistributionService(model *module.Distribution) *DistributionService {
	ctx, cancel := context.WithCancel(context.Background())
	srv := &DistributionService{
		distModel:      model,
		crowdRule:      &module.CrowdRule{},
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
					log.Printf("Task %d completed successfully after %v", t.ID, time.Since(startTime))
					s.distModel.UpdateExecTime(t.ID, time.Now().Unix())
					s.Stop()
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
	// 如果需要手动判断服务是否在运行
	if !s.isRunning {
		log.Printf("Task scheduler stopped: service not running")
		return
	}

	// 每次查询最多100个待处理任务
	tasks, _, err := s.distModel.List(map[string]interface{}{
		"status": TaskNoStartStatus,
	}, 0, 0)

	log.Print("Checking for new tasks to process...", tasks)

	if err != nil {
		log.Printf("List tasks error: %v", err)
		return
	}

	for _, task := range tasks {
		select {
		case <-s.ctx.Done():
			log.Printf("Task scheduler stopped: context cancelled")
			return
		case s.taskChan <- &task:
			log.Printf("Scheduled task %d for processing", task.ID)
			s.distModel.UpdateStatus(task.ID, TaskWaitStatus)
		default:
			log.Printf("Task channel full, skipping task %d", task.ID)
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

// findRemoteFiles 在远程服务器上查找匹配的文件
func (s *DistributionService) findRemoteFiles(serverIP, remotePattern string) ([]string, error) {
	// 使用ssh执行ls命令来获取匹配的文件列表
	cmd := exec.Command("ssh", fmt.Sprintf("root@%s", serverIP), "ls -1", remotePattern)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list remote files: %w, cmd: %s", err, cmd.String())
	}

	// 解析输出得到文件列表
	files := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(files) == 0 || (len(files) == 1 && files[0] == "") {
		return nil, fmt.Errorf("no files found matching pattern: %s", remotePattern)
	}

	return files, nil
}

// downloadRemoteFile 下载远程文件到本地临时目录
func (s *DistributionService) downloadRemoteFile(remotePath string) ([]string, error) {
	// 解析远程文件路径
	remotePath = strings.TrimPrefix(remotePath, "file://")
	log.Printf("Downloading remote file from path: %s", remotePath)
	parts := strings.SplitN(remotePath, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid remote file path: %s", remotePath)
	}

	serverIP := parts[0]
	remoteFilePath := "/" + parts[1]

	// 创建本地临时目录
	tempDir := "./"
	downloadDir := filepath.Join(tempDir, "dmp_downloads")
	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create download directory: %w", err)
	}

	// 获取匹配的远程文件列表
	matchedFiles, err := s.findRemoteFiles(serverIP, remoteFilePath)
	if err != nil {
		return nil, err
	}

	log.Printf("Found %d matching files on remote server", len(matchedFiles))

	// 下载所有匹配的文件
	var localPaths []string
	timestamp := time.Now().Format("20060102150405")

	for idx, remoteFile := range matchedFiles {
		// 生成本地文件路径
		fileName := filepath.Base(remoteFile)
		localPath := filepath.Join(downloadDir, fmt.Sprintf("%s_%d_%s", timestamp, idx, fileName))

		// 使用scp下载文件
		cmd := exec.Command("scp", fmt.Sprintf("root@%s:%s", serverIP, remoteFile), localPath)
		cmd.Stderr = os.Stderr

		log.Printf("Downloading file %d/%d: %s -> %s", idx+1, len(matchedFiles), remoteFile, localPath)

		if err := cmd.Run(); err != nil {
			// 如果下载失败，清理已下载的文件
			for _, path := range localPaths {
				s.cleanupTempFile(path)
			}
			return nil, fmt.Errorf("failed to download file %s: %w", remoteFile, err)
		}

		localPaths = append(localPaths, localPath)
	}

	return localPaths, nil
}

// cleanupTempFile 清理临时文件
func (s *DistributionService) cleanupTempFile(path string) {
	if strings.Contains(path, "dmp_downloads") {
		if err := os.Remove(path); err != nil {
			log.Printf("Failed to cleanup temp file %s: %v", path, err)
		} else {
			log.Printf("Successfully cleaned up temp file: %s", path)
		}
	}
}

// streamProcessFile 流式处理大文件
func (s *DistributionService) streamProcessFile(task *module.Distribution, processor func([]map[string]string) error) error {
	var localPaths []string
	var err error

	// 检查是否是远程文件
	if strings.HasPrefix(task.Path, "file://") {
		// 下载远程文件
		localPaths, err = s.downloadRemoteFile(task.Path)
		if err != nil {
			log.Printf("Failed to download remote files %s: %v", task.Path, err)
			return fmt.Errorf("download files error: %w", err)
		}
		// 确保在处理完成后清理所有临时文件
		defer func() {
			for _, path := range localPaths {
				s.cleanupTempFile(path)
			}
		}()
		log.Printf("Successfully downloaded %d remote files", len(localPaths))
	} else {
		localPaths = []string{task.Path}
	}

	var totalProcessed int64
	// 处理所有文件
	for fileIdx, filePath := range localPaths {
		log.Printf("Processing file %d/%d: %s", fileIdx+1, len(localPaths), filePath)

		// 打开文件进行处理
		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("Failed to open file %s: %v", filePath, err)
			return fmt.Errorf("open file error: %w", err)
		}

		var (
			scanner   = bufio.NewScanner(file)
			batch     = make([]map[string]string, 0, StreamBatchSize)
			batchSize int64
		)

		// 增加扫描器的缓冲区大小
		buffer := make([]byte, 0, 1024*1024)
		scanner.Buffer(buffer, 1024*1024)

		// 使用匿名函数确保文件正确关闭
		processErr := func() error {
			defer file.Close()

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
						return fmt.Errorf("process batch error: %w", err)
					}
					s.updateProgress(task.ID, batchSize)
					totalProcessed += batchSize
					batch = batch[:0]
					batchSize = 0
				}
			}

			// 处理剩余数据
			if len(batch) > 0 {
				if err := processor(batch); err != nil {
					return fmt.Errorf("process final batch error: %w", err)
				}
				s.updateProgress(task.ID, batchSize)
				totalProcessed += batchSize
			}

			return scanner.Err()
		}()

		if processErr != nil {
			return fmt.Errorf("error processing file %s: %w", filePath, processErr)
		}
	}

	log.Printf("Total processed lines across all files: %d", totalProcessed)
	// 确保最终进度被写入数据库
	s.flushProgress()

	return nil
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

// parseLine 优化后的行解析
func (s *DistributionService) parseLine(line string, task *module.Distribution) (map[string]string, bool) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil, false
	}

	deviceInfo := make(map[string]string)
	if task.UserID == 1 {
		deviceInfo["user_id"] = fields[1]
	}
	if task.Oaid == 1 {
		if oaid := strings.TrimSpace(fields[2]); oaid != "" {
			deviceInfo["oaid"] = oaid
		}
	}
	if task.Caid == 1 {
		// 处理可能包含多个CAID的情况
		if caids := strings.Split(fields[3], ","); len(caids) > 0 {
			for i, caid := range caids {
				if caid = strings.TrimSpace(caid); caid != "" {
					deviceInfo[fmt.Sprintf("caid_%d", i+1)] = caid
				}
			}
		}
	}
	if task.Idfa == 1 {
		if idfa := strings.TrimSpace(fields[4]); idfa != "" {
			deviceInfo["idfa"] = idfa
		}
	}
	if task.Imei == 1 {
		if imei := strings.TrimSpace(fields[5]); imei != "" {
			deviceInfo["imei"] = imei
		}
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
