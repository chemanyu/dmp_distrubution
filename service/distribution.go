package service

import (
	"context"
	"database/sql"
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
				begin := time.Now()
				defer s.wg.Done()
				defer func() { <-s.workerSem }() // 释放工作协程信号量

				startTime := time.Now()
				log.Printf("Processing task %d started", t.ID)

				if err := s.processTask(t); err != nil {
					log.Printf("Task %d failed after %v: %v", t.ID, time.Since(startTime), err)
					s.finalizeTask(t, TaskFailStatus, err)
					return
				}
				log.Printf("Task %d completed successfully after %v", t.ID, time.Since(startTime))
				s.distModel.UpdateExecTime(t.ID, time.Now().Unix())

				// 保存数据文件
				end := time.Now()
				log.Printf("Task %d processing time: %v", t.ID, end.Sub(begin))
				if err := s.saveFileByStrategyID(task, int64(task.StrategyID), end.Sub(begin)); err != nil {
					log.Printf("SaveFile %d failed after %v: %v", t.ID, time.Since(startTime), err)
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

	// 处理策略ID分发
	if err := s.processByStrategyID(task, int64(task.StrategyID)); err != nil {
		s.finalizeTask(task, TaskFailStatus, err)
		return err
	}

	// 3. 最终状态更新
	return s.finalizeTask(task, TaskDoneStatus, nil)
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
func (s *DistributionService) processByStrategyID(task *module.Distribution, strategyID int64) error {
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

	for start = 0; start <= totalCount; start += partitionSize {
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
				WHERE id >= ? AND id < ? )`,
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
				log.Printf("Reading row... userID: %s, oaid: %s, caid: %s, idfa: %s, imei: %s", user_id, oaid, caid, idfa, imei)
				if err := rows.Scan(scanFields...); err != nil {
					log.Printf("Scan error: %v", err)
					continue
				}
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

// createTempTable 创建临时表
func (s *DistributionService) createTempTable(db *sql.DB, tableName string) error {
	createTableSQL := fmt.Sprintf(`
		CREATE TEMPORARY TABLE %s (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			imei VARCHAR(128) NOT NULL,
			oaid VARCHAR(128) NOT NULL,
			caid VARCHAR(128) NOT NULL,
			idfa VARCHAR(128) NOT NULL,
			user_id VARCHAR(128) NOT NULL,
			INDEX idx_imei (imei),
			INDEX idx_oaid (oaid),
			INDEX idx_caid (caid),
			INDEX idx_idfa (idfa),
			INDEX idx_user_id (user_id)
		)
	`, tableName)

	_, err := db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create temp table %s: %w", tableName, err)
	}

	log.Printf("Created temporary table: %s", tableName)
	return nil
}

// dropTempTable 删除临时表
func (s *DistributionService) dropTempTable(db *sql.DB, tableName string) {
	dropTableSQL := fmt.Sprintf("DROP TEMPORARY TABLE IF EXISTS %s", tableName)
	if _, err := db.Exec(dropTableSQL); err != nil {
		log.Printf("Warning: failed to drop temp table %s: %v", tableName, err)
	} else {
		log.Printf("Dropped temporary table: %s", tableName)
	}
}

// saveToTempTable 将查询结果保存到临时表
func (s *DistributionService) saveToTempTable(db *sql.DB, selectFields []string, tempTableName string, strategyID int64, task *module.Distribution) (int64, error) {

	// 构建插入字段和选择字段
	insertFields := strings.Join(selectFields, ", ")
	selectFieldsStr := strings.Join(selectFields, ", ")

	// 直接使用 INSERT ... SELECT 语句
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s FROM dmp_user_mapping_v4
		WHERE bitmap_contains(
			(SELECT user_set FROM dmp_crowd_user_bitmap WHERE crowd_rule_id = ? ORDER BY event_date DESC LIMIT 1),
			user_hash_id
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

// saveFileByStrategyID 通过StrategyID从Doris获取最新user_set，查mapping表，整理为batch保存数据文件
func (s *DistributionService) saveFileByStrategyID(task *module.Distribution, strategyID int64, duration time.Duration) error {
	// 获取保存地址
	baseAddress := core.GetConfig().OUTPUT_DIR
	if baseAddress == "" {
		baseAddress = "./output" // 默认输出目录
	}
	// 创建输出目录
	if err := os.MkdirAll(baseAddress, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}
	// 生成文件名，只包含任务ID，不包含时间戳，确保同一任务写入同一文件
	log.Print("Generating file name for task", task.ID, "with duration", duration)
	fileName := fmt.Sprintf("task_%d_%s_%s.csv", task.ID, time.Now().Format("200601021504"), duration.String())
	filePath := filepath.Join(baseAddress, fileName)
	s.taskFilePaths.Store(task.ID, filePath)

	dorisDB := mysqldb.Doris
	if dorisDB == nil {
		return fmt.Errorf("doris connection is not initialized")
	}

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
		WHERE bitmap_contains(
			(SELECT user_set FROM dmp_crowd_user_bitmap WHERE crowd_rule_id = ? ORDER BY event_date DESC LIMIT 1),
			hash_id
		)`,
		strings.Join(selectFields, ", "))
	log.Printf("Executing query: %s with range and strategyID %d", query, strategyID)
	rows, err := dorisDB.Query(query, strategyID)
	if err != nil {
		return fmt.Errorf("query mapping error in range: %w", err)
	}
	defer rows.Close()

	batch := make([]map[string]string, 0)
	processed := 0

	for rows.Next() {
		//og.Printf("Reading row... userID: %s, oaid: %s, caid: %s, idfa: %s, imei: %s", userID, oaid, caid, idfa, imei)
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
			if err := s.saveBatchToFile(task, batch, selectFields, filePath); err != nil {
				log.Printf("Failed to save batch to file: %v", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		// 保存batch数据到文件
		if err := s.saveBatchToFile(task, batch, selectFields, filePath); err != nil {
			log.Printf("Failed to save batch to file: %v", err)
		}
	}
	log.Printf("Processed %d records for hash_id range", processed)
	return nil
}

// saveBatchToFile 将batch数据按selectFields顺序保存到文件，返回文件路径
func (s *DistributionService) saveBatchToFile(task *module.Distribution, batch []map[string]string, selectFields []string, filePath string) error {

	// 打开文件进行追加写入
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file for writing: %w", err)
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
				return fmt.Errorf("failed to write to file: %w", err)
			}
		}
	}

	log.Printf("Saved %d records to file", len(batch))
	return nil
}
