package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysqldb "dmp_distribution/common/mysql"
	"dmp_distribution/core"
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
	IsRunning      bool
	progressTicker *time.Ticker

	// 进度追踪
	progressMap sync.Map // 用于存储每个任务的进度
	//progressMutex  sync.RWMutex      // 用于保护进度更新
	lastUpdateTime map[int]time.Time // 记录每个任务最后更新时间

	// 文件路径追踪
	taskFilePaths sync.Map // 用于存储每个任务生成的文件路径 map[int]string
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
		IsRunning:      true,
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
				s.wg.Wait() // 等待所有正在执行的任务完成
				log.Printf("All tasks completed, stopping service")
				s.Stop()
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
				}
			}(task)
		}
	}
}

// Stop 停止服务
func (s *DistributionService) Stop() {
	if !s.IsRunning {
		return
	}
	s.IsRunning = false

	if s.cancel != nil {
		s.cancel()
	}

	if s.progressTicker != nil {
		s.progressTicker.Stop()
	}

	log.Printf("Distribution service stopped gracefully")
}

// StartTaskScheduler 启动任务调度器
func (s *DistributionService) StartTaskScheduler() {
	log.Printf("Starting task scheduler")
	// 如果需要手动判断服务是否在运行
	if !s.IsRunning {
		log.Printf("Task scheduler stopped: service not running")
		return
	}

	// 待处理任务
	tasks, _, err := s.distModel.List(map[string]interface{}{}, 0, 0)

	log.Print("Checking for new tasks to process...", tasks)

	if err != nil {
		log.Printf("List tasks error: %v", err)
		return
	}

	// 提交所有任务到任务通道
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

	// 所有任务提交完成后关闭任务通道
	// 这会触发 taskProcessor 中的逻辑来等待所有任务完成后停止服务
	// close(s.taskChan)
	log.Printf("All tasks submitted, task channel closed")
}

// progressPersister 定期将内存中的进度持久化到数据库
func (s *DistributionService) progressPersister() {
	for {
		select {
		case <-s.ctx.Done():
			s.flushAllProgress() // 服务停止时，确保刷新所有进度
			return
		case <-s.progressTicker.C:
			if !s.IsRunning {
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
	// processor := func(batch []map[string]string) error {
	// 	return s.processBatch(task, batch)
	// }

	// if err := s.streamProcessFile(task, processor); err != nil {
	// 	s.finalizeTask(task, TaskFailStatus, err)
	// 	return err
	// }

	// 处理策略ID分发
	if err := s.processByStrategyID(task, int64(task.StrategyID)); err != nil {
		s.finalizeTask(task, TaskFailStatus, err)
		return err
	}

	// 3. 最终状态更新
	return s.finalizeTask(task, TaskDoneStatus, nil)
}

// findRemoteFiles 在远程服务器上查找匹配的文件
/*func (s *DistributionService) findRemoteFiles(serverIP, remotePattern string) ([]string, error) {
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

// downloadRemoteFile scp下载远程文件到本地临时目录
func (s *DistributionService) downloadRemoteFile(remotePath string) ([]string, error) {
	// 解析远程文件路径
	remotePath = strings.TrimPrefix(remotePath, "file:///")
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

// streamProcessFile 流式处理大文件
func (s *DistributionService) streamProcessFile(task *module.Distribution, processor func([]map[string]string) error) error {
	var localPaths []string
	var err error

	// 检查是否是远程文件
	if strings.HasPrefix(task.Path, "file://") {
		// 下载远程文件 - scp 逻辑
		//localPaths, err = s.downloadRemoteFile(task.Path)
		// 下载远程文件 - api 接口下载
		localPaths, err = s.downloadApiFile(task.Path)
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

// downloadApiFile 通过API接口下载文件
func (s *DistributionService) downloadApiFile(remotePath string) ([]string, error) {
	// 解析远程文件路径
	remotePath = strings.TrimPrefix(remotePath, "file:///")
	log.Printf("Downloading files via API from path: %s", remotePath)
	parts := strings.SplitN(remotePath, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid remote file path: %s", remotePath)
	}

	serverIP := parts[0]
	remotePrefix := "/" + parts[1]

	// 获取文件列表
	listURL := fmt.Sprintf("http://%s:6090/file/v1/download?prefix=%s", serverIP, remotePrefix)
	resp, err := http.Get(listURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch file list: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch file list, status code: %d", resp.StatusCode)
	}

	var fileList struct {
		Count int `json:"count"`
		Files []struct {
			Filename string `json:"filename"`
			Path     string `json:"path"`
			Size     int    `json:"size"`
		} `json:"files"`
		Status string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&fileList); err != nil {
		return nil, fmt.Errorf("failed to decode file list response: %w", err)
	}

	if fileList.Status != "success" {
		return nil, fmt.Errorf("failed to fetch file list, status: %s", fileList.Status)
	}

	log.Printf("Found %d files to download", fileList.Count)

	// 下载文件到本地目录
	tempDir := "./"
	downloadDir := filepath.Join(tempDir, "dmp_downloads")
	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create download directory: %w", err)
	}

	var localPaths []string
	for _, file := range fileList.Files {
		downloadURL := fmt.Sprintf("http://%s:6090/file/v1/download/file?path=%s", serverIP, file.Path)
		localPath := filepath.Join(downloadDir, file.Filename)

		resp, err := http.Get(downloadURL)
		if err != nil {
			return nil, fmt.Errorf("failed to download file %s: %w", file.Path, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("failed to download file %s, status code: %d", file.Path, resp.StatusCode)
		}

		out, err := os.Create(localPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create local file %s: %w", localPath, err)
		}
		defer out.Close()

		if _, err := io.Copy(out, resp.Body); err != nil {
			return nil, fmt.Errorf("failed to save file %s: %w", localPath, err)
		}

		localPaths = append(localPaths, localPath)
		log.Printf("Successfully downloaded file: %s", localPath)
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

// parseLine 优化后的行解析
func (s *DistributionService) parseLine(line string, task *module.Distribution) (map[string]string, bool) {
	fields := strings.Split(line, "\t")
	if len(fields) == 0 {
		return nil, false
	}

	deviceInfo := make(map[string]string)
	if task.UserID == 1 {
		deviceInfo["user_id"] = fields[1]
	}
	if task.Oaid == 1 && len(fields) >= 2 {
		if oaid := strings.TrimSpace(fields[2]); oaid != "" {
			deviceInfo["oaid"] = oaid
		}
	}
	if task.Caid == 1 && len(fields) >= 3 {
		// 处理可能包含多个CAID的情况
		if caids := strings.Split(fields[3], ","); len(caids) > 0 {
			for i, caid := range caids {
				if caid = strings.TrimSpace(caid); caid != "" {
					deviceInfo[fmt.Sprintf("caid_%d", i+1)] = caid
				}
			}
		}
	}
	if task.Idfa == 1 && len(fields) >= 4 {
		if idfa := strings.TrimSpace(fields[4]); idfa != "" {
			deviceInfo["idfa"] = idfa
		}
	}
	if task.Imei == 1 && len(fields) >= 5 {
		if imei := strings.TrimSpace(fields[5]); imei != "" {
			deviceInfo["imei"] = imei
		}
	}
	return deviceInfo, len(deviceInfo) > 0
}
*/

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

// processByStrategyID 通过StrategyID从Doris获取最新user_set，查mapping表，整理为batch推送redis，并兼容进度/状态
func (s *DistributionService) processByStrategyID(task *module.Distribution, strategyID int64) error {
	dorisDB := mysqldb.Doris
	if dorisDB == nil {
		return fmt.Errorf("doris connection is not initialized")
	}

	// 查 hash_id 范围
	var minHashID, maxHashID int64
	rangeQuery := `SELECT MIN(hash_id), MAX(hash_id) FROM dmp_user_mapping_v4`
	if err := dorisDB.QueryRow(rangeQuery).Scan(&minHashID, &maxHashID); err != nil {
		return fmt.Errorf("query hash_id range error: %w", err)
	}
	log.Printf("hash_id range: [%d, %d]", minHashID, maxHashID)

	// 分区并发查，避免OFFSET
	const partitionSize = int64(50000000) // 每分区5000万，可根据资源调整
	const maxConcurrency = 8              // 最大并发数
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)
	var firstErr error
	var mu sync.Mutex

	for start := minHashID; start <= maxHashID; start += partitionSize {
		end := start + partitionSize
		if end > maxHashID+1 {
			end = maxHashID + 1
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(start, end int64) {
			defer wg.Done()
			defer func() { <-sem }()

			var selectFields []string
			var scanFields []interface{}
			var userID, oaid, caid, idfa, imei string
			if task.UserID == 1 {
				selectFields = append(selectFields, "user_id")
				scanFields = append(scanFields, &userID)
			}
			if task.Oaid == 1 {
				selectFields = append(selectFields, "oaid")
				scanFields = append(scanFields, &oaid)
			}
			if task.Caid == 1 {
				selectFields = append(selectFields, "caid")
				scanFields = append(scanFields, &caid)
			}
			if task.Idfa == 1 {
				selectFields = append(selectFields, "idfa")
				scanFields = append(scanFields, &idfa)
			}
			if task.Imei == 1 {
				selectFields = append(selectFields, "imei")
				scanFields = append(scanFields, &imei)
			}

			query := fmt.Sprintf(`SELECT %s FROM dmp_user_mapping_v4
				WHERE hash_id >= ? AND hash_id < ? AND bitmap_contains(
					(SELECT user_set FROM dmp_crowd_user_bitmap WHERE crowd_rule_id = ? ORDER BY event_date DESC LIMIT 1),
                	hash_id
				)`,
				strings.Join(selectFields, ", "))
			log.Printf("Executing query: %s with range [%d, %d) and strategyID %d", query, start, end, strategyID)
			rows, err := dorisDB.Query(query, start, end, strategyID)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("query mapping error in range [%d,%d): %w", start, end, err)
				}
				mu.Unlock()
				return
			}
			defer rows.Close()

			batch := make([]map[string]string, 0, StreamBatchSize)
			processed := 0
			for rows.Next() {
				if err := rows.Scan(scanFields...); err != nil {
					log.Printf("Scan error: %v", err)
					continue
				}
				item := make(map[string]string)
				if task.UserID == 1 && userID != "" {
					item["user_id"] = userID
				}
				if task.Oaid == 1 && oaid != "" {
					item["oaid"] = oaid
				}
				if task.Caid == 1 && caid != "" {
					caids := strings.Split(caid, ",")
					for k, c := range caids {
						c = strings.TrimSpace(c)
						if c != "" {
							item[fmt.Sprintf("caid_%d", k+1)] = c
						}
					}
				}
				if task.Idfa == 1 && idfa != "" {
					item["idfa"] = idfa
				}
				if task.Imei == 1 && imei != "" {
					item["imei"] = imei
				}
				if len(item) > 0 {
					batch = append(batch, item)
				}
				if len(batch) >= StreamBatchSize {
					// 保存batch数据到文件
					if filePath, err := s.saveBatchToFile(task, batch, selectFields); err != nil {
						log.Printf("Failed to save batch to file: %v", err)
					} else {
						// 记录文件路径
						s.taskFilePaths.Store(task.ID, filePath)
					}

					if err := s.processBatch(task, batch); err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("redis batch push error: %w", err)
						}
						mu.Unlock()
						return
					}
					processed += len(batch)
					batch = batch[:0]
					// 进度更新
					s.updateProgress(task.ID, int64(StreamBatchSize))
				}
			}
			if len(batch) > 0 {
				// 保存batch数据到文件
				if filePath, err := s.saveBatchToFile(task, batch, selectFields); err != nil {
					log.Printf("Failed to save batch to file: %v", err)
				} else {
					// 记录文件路径
					s.taskFilePaths.Store(task.ID, filePath)
				}

				if err := s.processBatch(task, batch); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("redis batch push error: %w", err)
					}
					mu.Unlock()
					return
				}
				processed += len(batch)
				// 进度更新
				s.updateProgress(task.ID, int64(len(batch)))
			}
			log.Printf("Processed %d records for hash_id range [%d, %d)", processed, start, end)
		}(start, end)
	}
	wg.Wait()

	// 分区全部完成后，刷新进度
	s.flushProgress()

	if firstErr != nil {
		s.finalizeTask(task, TaskFailStatus, firstErr)
		return firstErr
	}

	s.finalizeTask(task, TaskDoneStatus, nil)
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

// finalizeTask 任务收尾
func (s *DistributionService) finalizeTask(task *module.Distribution, status int, err error) error {
	if status == TaskFailStatus {
		log.Printf("Task %d failed: %v", task.ID, err)
	} else if status == TaskDoneStatus {
		// 任务成功完成时，保存文件路径到数据库
		if filePath, exists := s.taskFilePaths.Load(task.ID); exists {
			if path, ok := filePath.(string); ok {
				if updateErr := s.distModel.UpdatePath(task.ID, path); updateErr != nil {
					log.Printf("Failed to update file path for task %d: %v", task.ID, updateErr)
				} else {
					log.Printf("Successfully updated file path for task %d: %s", task.ID, path)
				}
				// 清理内存中的文件路径记录
				s.taskFilePaths.Delete(task.ID)
			}
		}
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

// saveBatchToFile 将batch数据按selectFields顺序保存到文件，返回文件路径
func (s *DistributionService) saveBatchToFile(task *module.Distribution, batch []map[string]string, selectFields []string) (string, error) {
	// 获取保存地址
	baseAddress := core.GetConfig().OUTPUT_DIR
	if baseAddress == "" {
		baseAddress = "./output" // 默认输出目录
	}

	// 创建输出目录
	if err := os.MkdirAll(baseAddress, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	// 生成文件名，只包含任务ID，不包含时间戳，确保同一任务写入同一文件
	fileName := fmt.Sprintf("task_%d.csv", task.ID)
	filePath := filepath.Join(baseAddress, fileName)

	// 打开文件进行追加写入
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to open file for writing: %w", err)
	}
	defer file.Close()

	// 将batch数据写入文件，按照selectFields的顺序
	for _, item := range batch {
		var values []string

		// 按照selectFields的顺序输出字段
		for _, field := range selectFields {
			if field == "caid" {
				// 处理多个caid字段的特殊情况
				var caids []string
				for i := 1; ; i++ {
					if caid, exists := item[fmt.Sprintf("caid_%d", i)]; exists {
						caids = append(caids, caid)
					} else {
						break
					}
				}
				if len(caids) > 0 {
					values = append(values, strings.Join(caids, ","))
				} else {
					values = append(values, "") // 空值占位
				}
			} else {
				// 普通字段处理
				if val, exists := item[field]; exists {
					values = append(values, val)
				} else {
					values = append(values, "") // 空值占位
				}
			}
		}

		// 如果有数据，写入文件
		if len(values) > 0 {
			line := strings.Join(values, "\t") + "\n"
			if _, err := file.WriteString(line); err != nil {
				return "", fmt.Errorf("failed to write to file: %w", err)
			}
		}
	}

	log.Printf("Saved %d records to file: %s", len(batch), filePath)
	return filePath, nil
}
