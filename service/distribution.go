package service

import (
	"archive/zip"
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
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
	StreamBatchSize    = 100000
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
	distModel *module.Distribution
	crowdRule *module.CrowdRule
	ctx       context.Context
	cancel    context.CancelFunc
	taskChan  chan *module.Distribution
	workerSem chan struct{}
	wg        sync.WaitGroup
	IsRunning bool
	//progressTicker *time.Ticker

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
		distModel: model,
		crowdRule: &module.CrowdRule{},
		ctx:       ctx,
		cancel:    cancel,
		taskChan:  make(chan *module.Distribution),
		workerSem: make(chan struct{}, MaxParallelWorkers),
		IsRunning: true,
		//progressTicker: time.NewTicker(ProgressUpdateInterval),
		lastUpdateTime: make(map[int]time.Time),
	}

	// 启动必要的后台任务
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
			s.flushAllProgress()
			return
		case task, ok := <-s.taskChan:
			log.Printf("Received task %d for processing, ok: %v", task.ID, ok)
			// 获取工作协程信号量
			s.workerSem <- struct{}{}

			s.wg.Add(1)
			go func(t *module.Distribution) {
				defer s.wg.Done()
				defer func() { <-s.workerSem }() // 释放工作协程信号量

				log.Printf("Processing task %d started", t.ID)

				if err := s.processTask(t); err != nil {
					s.finalizeTask(t, TaskFailStatus, err)
					return
				}
			}(task)
		case <-time.After(30 * time.Second): // 30秒超时，可根据需要调整
			s.wg.Wait()
			log.Printf("No tasks received for 30 seconds, stopping service")
			s.Stop()
			return
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

// processTask 处理单个任务
func (s *DistributionService) processTask(task *module.Distribution) error {
	// 1. 初始化任务状态
	if err := s.initTaskStatus(task); err != nil {
		return err
	}

	dorisDB := mysqldb.Doris
	if dorisDB == nil {
		return fmt.Errorf("doris connection is not initialized")
	}

	// 创建临时表
	tempTableName := fmt.Sprintf("temp_device_ids_%d_%d", task.ID, time.Now().Unix())
	if err := s.createTempTable(dorisDB, tempTableName); err != nil {
		return fmt.Errorf("create temp table error: %w", err)
	}

	// 确保临时表在函数结束时被删除
	defer func() {
		s.dropTempTable(dorisDB, tempTableName)
	}()

	// 处理策略ID分发
	if err := s.processByStrategyID(task, int64(task.StrategyID), dorisDB, tempTableName); err != nil {
		s.finalizeTask(task, TaskFailStatus, err)
		return err
	} else {
		s.finalizeTask(task, TaskDoneStatus, nil)
		log.Printf("Task %d completed successfully", task.ID)
		s.distModel.UpdateExecTime(task.ID, time.Now().Unix())
	}

	// 将数据保存到文件中
	// 保存数据文件
	if err := s.saveFileByStrategyID(task, int64(task.StrategyID), dorisDB, tempTableName); err != nil {
		log.Printf("SaveFile %d failed: %v", task.ID, err)
	}
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

	return nil
}

// initTaskStatus 任务初始化
func (s *DistributionService) initTaskStatus(task *module.Distribution) error {
	if err := s.distModel.UpdateStatus(task.ID, TaskRunStatus); err != nil {
		return fmt.Errorf("update status error: %w", err)
	}
	// 初始化行数为0
	if err := s.distModel.UpdateLineCountZero(task.ID, 0); err != nil {
		return fmt.Errorf("init line count error: %w", err)
	}
	return nil
}

// processByStrategyID 通过StrategyID从Doris获取最新user_set，查mapping表，整理为batch推送redis，并兼容进度/状态
func (s *DistributionService) processByStrategyID(task *module.Distribution, strategyID int64, dorisDB *sql.DB, tempTableName string) error {
	// 构建查询字段 - 只查询需要的device_id字段
	// 构建查询字段
	var selectFields []string
	var imei, oaid, caid, idfa, user_id string
	var scanFields []interface{}
	if task.Imei == 1 {
		selectFields = append(selectFields, "imei")
		scanFields = append(scanFields, &imei)
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
	if task.UserID == 1 {
		selectFields = append(selectFields, "user_id")
		scanFields = append(scanFields, &user_id)
	}

	if len(selectFields) == 0 {
		return fmt.Errorf("no fields selected for task %d", task.ID)
	}

	// 将查询结果保存到临时表
	var totalCount int64
	var err error
	if totalCount, err = s.saveToTempTable(dorisDB, selectFields, tempTableName, strategyID, task); err != nil {
		return fmt.Errorf("save to temp table error: %w", err)
	}
	if totalCount == 0 {
		log.Printf("No records found for task %d", task.ID)
		s.finalizeTask(task, TaskFailStatus, nil)
		return nil
	}

	const partitionSize = int64(5000000) // 每分区500万，可根据资源调整
	const maxConcurrency = 8             // 最大并发数
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)
	var firstErr error
	var mu sync.Mutex
	var start int64

	for start = 1; start <= totalCount; start += partitionSize {
		end := start + partitionSize
		if end > totalCount {
			end = totalCount
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(start, end int64) {
			defer wg.Done()
			defer func() { <-sem }()

			query := fmt.Sprintf(`SELECT %s FROM %s
				WHERE row_num >= ? AND row_num <= ?`,
				strings.Join(selectFields, ", "), tempTableName)

			log.Printf("Executing query: %s with range [%d, %d)", query, start, end)

			rows, err := dorisDB.Query(query, start, end)
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
				log.Printf("Reading row... userID: %s, oaid: %s, caid: %s, idfa: %s, imei: %s", user_id, oaid, caid, idfa, imei)
				item := make(map[string]string)
				if user_id != "" {
					item["user_id"] = user_id
				}
				if oaid != "" {
					item["oaid"] = oaid
				}
				if caid != "" {
					caids := strings.Split(caid, ",")
					for k, c := range caids {
						c = strings.TrimSpace(c)
						if c != "" {
							item[fmt.Sprintf("caid_%d", k+1)] = c
						}
					}
				}
				if idfa != "" {
					item["idfa"] = idfa
				}
				if imei != "" {
					item["imei"] = imei
				}
				if len(item) > 0 {
					batch = append(batch, item)
				}
				if len(batch) >= StreamBatchSize {
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

	s.flushProgress() // 确保所有进度都被刷新到数据库
	s.finalizeTask(task, TaskDoneStatus, nil)
	return nil
}

// createTempTable 创建临时表 - Doris兼容版本（简化版）
func (s *DistributionService) createTempTable(db *sql.DB, tableName string) error {
	// 简化的Doris表结构，ID仅用于分页，可以重复
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			row_num BIGINT,
			imei VARCHAR(128),
			oaid VARCHAR(128),
			caid VARCHAR(128),
			idfa VARCHAR(128),
			user_id VARCHAR(128)
		) ENGINE=OLAP
		DUPLICATE KEY(row_num)
		DISTRIBUTED BY HASH(row_num) BUCKETS 10
		PROPERTIES (
			"replication_allocation" = "tag.location.default: 1",
			"storage_format" = "V2"
		)
	`, tableName)

	_, err := db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create temp table %s: %w", tableName, err)
	}

	log.Printf("Created temporary table: %s", tableName)
	return nil
}

// dropTempTable 删除临时表 - Doris兼容版本
func (s *DistributionService) dropTempTable(db *sql.DB, tableName string) {
	dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if _, err := db.Exec(dropTableSQL); err != nil {
		log.Printf("Warning: failed to drop temp table %s: %v", tableName, err)
	} else {
		log.Printf("Dropped temporary table: %s", tableName)
	}
}

// saveToTempTable 将查询结果保存到临时表 - 简化版本
func (s *DistributionService) saveToTempTable(db *sql.DB, selectFields []string, tempTableName string, strategyID int64, task *module.Distribution) (int64, error) {

	// 构建插入字段和选择字段，使用简单的行号
	insertFields := "row_num, " + strings.Join(selectFields, ", ")
	selectFieldsStr := strings.Join(selectFields, ", ")

	// 简化的INSERT语句，直接查询并插入
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT ROW_NUMBER() OVER() as row_num, %s 
		FROM dmp_user_mapping_v4 m
		WHERE bitmap_contains(
			(SELECT user_set FROM dmp_crowd_user_bitmap WHERE crowd_rule_id = ? ORDER BY event_date DESC LIMIT 1),
			m.hash_id
		)
	`, tempTableName, insertFields, selectFieldsStr)

	log.Printf("Executing INSERT ... SELECT query: %s with strategyID %d", insertSQL, strategyID)

	startTime := time.Now()

	result, err := db.Exec(insertSQL, strategyID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert data into temp table: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	log.Printf("Inserted %d records into temp table %s, took %v", rowsAffected, tempTableName, time.Since(startTime))
	return rowsAffected, nil
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

		log.Printf("Flushing progress for task %d: currentCount=%d, lastDBCount=%d, lastUpdate=%v", taskID, currentCount, lastDBCount, progress.lastUpdate)
		if err := s.distModel.UpdateLineCount(taskID, currentCount); err != nil {
			log.Printf("Failed to update line count for task %d: %v", taskID, err)
		} else {
			atomic.StoreInt64(&progress.lastDBCount, currentCount)
			progress.lastUpdate = now
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
	atomic.StoreInt64(&progress.currentCount, delta)
}

// saveFileByStrategyID 从临时表获取数据，按数据类型分文件保存，压缩成zip
func (s *DistributionService) saveFileByStrategyID(task *module.Distribution, strategyID int64, dorisDB *sql.DB, tempTableName string) error {
	// 获取保存地址
	baseAddress := core.GetConfig().OUTPUT_DIR
	if baseAddress == "" {
		baseAddress = "./output" // 默认输出目录
	}

	// 创建任务专用文件夹
	taskDir := filepath.Join(baseAddress, fmt.Sprintf("task_%d_%s", task.ID, time.Now().Format("200601021504")))
	if err := os.MkdirAll(taskDir, 0755); err != nil {
		return fmt.Errorf("failed to create task directory: %w", err)
	}

	// 构建查询字段
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

	if len(selectFields) == 0 {
		return fmt.Errorf("no fields selected for task %d", task.ID)
	}

	// 文件管理器，每种数据类型对应一个文件管理器（线程安全版本）
	const maxFileSize = 1024 * 1024 * 1024 // 1GB

	fileManagers := make(map[string]*fileManager)

	for _, field := range selectFields {
		fileManagers[field] = newFileManager(taskDir, field, maxFileSize)
	}

	// 获取总记录数
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tempTableName)
	var totalCount int64
	if err := dorisDB.QueryRow(countQuery).Scan(&totalCount); err != nil {
		return fmt.Errorf("failed to get total count: %w", err)
	}

	log.Printf("SaveFile Total records to process: %d", totalCount)

	// 分批处理数据，每次处理100万条，并行处理
	const partitionSize = 1000000 // 100万条
	const maxConcurrency = 4      // 最大并行数，可根据服务器性能调整
	var processed int64

	// 创建协程池和任务通道
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var start int64

	for start = 1; start <= totalCount; start += partitionSize {
		end := start + partitionSize
		if end > totalCount {
			end = totalCount
		}

		wg.Add(1)
		sem <- struct{}{} // 获取信号量

		go func(start, end int64) {
			defer wg.Done()
			defer func() { <-sem }() // 释放信号量

			// 分批查询数据
			query := fmt.Sprintf(`SELECT %s FROM %s WHERE row_num >= ? AND row_num <= ?`,
				strings.Join(selectFields, ", "), tempTableName)

			log.Printf("SaveFile Processing bat	ch query %s: start=%d, end=%d", query, start, end)

			rows, err := dorisDB.Query(query, start, end)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("query temp table error at batch [%d,%d]: %w", start, end, err)
				}
				mu.Unlock()
				return
			}
			defer rows.Close()

			// 处理当前批次的数据
			batchProcessed := 0
			for rows.Next() {
				if err := rows.Scan(scanFields...); err != nil {
					log.Printf("Scan error in batch [%d, %d]: %v", start, end, err)
					continue
				}
				log.Printf("Reading saveFile row... userID: %s, oaid: %s, caid: %s, idfa: %s, imei: %s", userID, oaid, caid, idfa, imei)

				// 保存非空数据到对应文件
				if strings.TrimSpace(userID) != "" {
					if err := fileManagers["user_id"].writeData(userID); err != nil {
						log.Printf("Failed to write user_id in batch [%d, %d]: %v", start, end, err)
					}
				}
				if strings.TrimSpace(oaid) != "" {
					if err := fileManagers["oaid"].writeData(oaid); err != nil {
						log.Printf("Failed to write oaid in batch [%d, %d]: %v", start, end, err)
					}
				}
				if strings.TrimSpace(caid) != "" {
					// caid可能包含多个值，用逗号分隔
					caids := strings.Split(caid, ",")
					for _, c := range caids {
						if c = strings.TrimSpace(c); c != "" {
							if err := fileManagers["caid"].writeData(c); err != nil {
								log.Printf("Failed to write caid in batch [%d, %d]: %v", start, end, err)
							}
						}
					}
				}
				if strings.TrimSpace(idfa) != "" {
					if err := fileManagers["idfa"].writeData(idfa); err != nil {
						log.Printf("Failed to write idfa in batch [%d, %d]: %v", start, end, err)
					}
				}
				if strings.TrimSpace(imei) != "" {
					if err := fileManagers["imei"].writeData(imei); err != nil {
						log.Printf("Failed to write imei in batch [%d, %d]: %v", start, end, err)
					}
				}

				batchProcessed++
			}
			// 更新总处理计数
			mu.Lock()
			processed += int64(batchProcessed)
			mu.Unlock()

			log.Printf("Batch [%d, %d] completed: processed %d records, total processed: %d/%d",
				start, end, batchProcessed, processed, totalCount)

		}(start, end)
	}

	// 等待所有批次完成
	wg.Wait()

	// 检查是否有错误
	if firstErr != nil {
		return fmt.Errorf("batch processing failed: %w", firstErr)
	}
	log.Printf("All parallel batches completed: %d records processed", processed)

	// 关闭所有文件管理器的文件，确保数据写入磁盘
	for dataType, manager := range fileManagers {
		if manager.currentFile != nil {
			log.Printf("Closing file for data type: %s", dataType)
			if err := manager.currentFile.Sync(); err != nil {
				log.Printf("Warning: failed to sync file for %s: %v", dataType, err)
			}
			if err := manager.currentFile.Close(); err != nil {
				log.Printf("Warning: failed to close file for %s: %v", dataType, err)
			}
			manager.currentFile = nil
		}
	}

	// 直接压缩文件夹为zip（无需复制，直接压缩现有文件）
	zipPath := filepath.Join(baseAddress, fmt.Sprintf("task_%d_%s.zip", task.ID, time.Now().Format("200601021504")))
	if err := s.zipDirectory(taskDir, zipPath); err != nil {
		return fmt.Errorf("failed to create zip file: %w", err)
	}

	// 删除临时文件夹
	if err := os.RemoveAll(taskDir); err != nil {
		log.Printf("Warning: failed to remove temp directory %s: %v", taskDir, err)
	}

	// 存储zip文件路径
	s.taskFilePaths.Store(task.ID, zipPath)
	log.Printf("Successfully created zip file: %s", zipPath)

	return nil
}

// fileManager 文件管理器，用于管理单个数据类型的文件写入（线程安全版本）
type fileManager struct {
	baseDir     string
	dataType    string
	maxFileSize int64
	currentFile *os.File
	currentSize int64
	fileIndex   int
	mutex       sync.Mutex // 添加互斥锁保证并发安全
}

// newFileManager 创建新的文件管理器
func newFileManager(baseDir, dataType string, maxFileSize int64) *fileManager {
	return &fileManager{
		baseDir:     baseDir,
		dataType:    dataType,
		maxFileSize: maxFileSize,
		fileIndex:   0,
	}
}

// writeData 写入数据（线程安全版本）
func (fm *fileManager) writeData(data string) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	// md5加密
	data = fmt.Sprintf("%x", md5.Sum([]byte(data)))
	// 计算数据大小
	dataSize := int64(len(data) + 1) // +1 for newline

	// 如果当前文件不存在或者会超过大小限制，创建新文件
	if fm.currentFile == nil || (fm.currentSize+dataSize > fm.maxFileSize) {
		if err := fm.createNewFile(); err != nil {
			return err
		}
	}

	// 写入数据
	if _, err := fm.currentFile.WriteString(data + "\n"); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	fm.currentSize += dataSize
	return nil
}

// createNewFile 创建新文件
func (fm *fileManager) createNewFile() error {
	// 关闭当前文件
	if fm.currentFile != nil {
		fm.currentFile.Close()
	}

	// 生成文件名
	var fileName string
	if fm.fileIndex == 0 {
		fileName = fmt.Sprintf("%s.txt", fm.dataType)
	} else {
		fileName = fmt.Sprintf("%s_%d.txt", fm.dataType, fm.fileIndex)
	}

	filePath := filepath.Join(fm.baseDir, fileName)

	// 创建新文件
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}

	fm.currentFile = file
	fm.currentSize = 0
	fm.fileIndex++

	log.Printf("Created new file: %s", filePath)
	return nil
}

// zipDirectory 将目录压缩为zip文件（保持文件夹结构版本）
func (s *DistributionService) zipDirectory(sourceDir, zipPath string) error {
	log.Printf("Starting zip compression: %s -> %s", sourceDir, zipPath)
	startTime := time.Now()

	// 创建zip文件
	zipFile, err := os.Create(zipPath)
	if err != nil {
		return fmt.Errorf("failed to create zip file: %w", err)
	}
	defer zipFile.Close()

	// 创建zip writer，设置为最快压缩级别（减少CPU时间）
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	// 遍历源目录并保持目录结构
	err = filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 跳过目录本身
		if info.IsDir() {
			return nil
		}

		// 创建zip文件内的路径，保持文件夹结构
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}

		log.Printf("Adding file to zip: %s (size: %d bytes)", zipPath, info.Size())

		// 在zip中创建文件（使用最快压缩方式）
		// 在zip中创建文件
		zipFileWriter, err := zipWriter.Create(relPath)
		if err != nil {
			return fmt.Errorf("failed to create file in zip: %w", err)
		}

		// 读取源文件
		sourceFile, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open source file: %w", err)
		}
		defer sourceFile.Close()

		// 使用大缓冲区快速复制（减少系统调用）
		buffer := make([]byte, 1024*1024) // 1MB缓冲区，提高IO效率
		_, err = io.CopyBuffer(zipFileWriter, sourceFile, buffer)
		if err != nil {
			return fmt.Errorf("failed to copy file to zip: %w", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	log.Printf("Zip compression completed in %v: %s", time.Since(startTime), zipPath)

	return nil
}
