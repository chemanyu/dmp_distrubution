package service

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	mysqldb "dmp_distribution/common/mysql"
	"dmp_distribution/core"
	"dmp_distribution/module"
)

// UploadFile 上传文件结构
type UploadFile struct {
	FileName    string `json:"file_name"`
	FilePath    string `json:"file_path"`
	FileSize    string `json:"file_size"`
	RecordCount string `json:"record_count"`
}

// UploadFileData 上传文件数据结构
type UploadFileData struct {
	ImeiFiles   []UploadFile `json:"imei-files"`
	OaidFiles   []UploadFile `json:"oaid-files"`
	CaidFiles   []UploadFile `json:"caid-files"`
	IdfaFiles   []UploadFile `json:"idfa-files"`
	UserIdFiles []UploadFile `json:"user_id-files"`
}

// UploadProcessorService 上传处理服务
type UploadProcessorService struct {
	uploadModel    *module.UploadRecords
	crowdRuleModel *module.CrowdRule
	mu             sync.Mutex
	isRunning      bool
	taskChan       chan *module.UploadRecords
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	workerSem      chan struct{}
}

// NewUploadProcessorService 创建新的上传处理服务
func NewUploadProcessorService() *UploadProcessorService {
	ctx, cancel := context.WithCancel(context.Background())
	return &UploadProcessorService{
		uploadModel:    &module.UploadRecords{},
		crowdRuleModel: &module.CrowdRule{},
		isRunning:      false,
		taskChan:       make(chan *module.UploadRecords, 100), // 任务通道缓冲区
		ctx:            ctx,
		cancel:         cancel,
		workerSem:      make(chan struct{}, MaxParallelWorkers),
	}
}

// StartProcessor 启动处理器 - 使用通道版本
func (s *UploadProcessorService) StartProcessor() {
	s.mu.Lock()
	if s.isRunning {
		log.Printf("Upload processor is already running")
		s.mu.Unlock()
		return
	}

	s.isRunning = true
	s.mu.Unlock()

	log.Printf("Starting upload processor with channel-based task management")

	// 启动任务处理协程
	go s.taskProcessor()

	// 启动任务发现协程
	go s.taskDiscovery()
}

// taskProcessor 处理任务通道中的任务
func (s *UploadProcessorService) taskProcessor() {
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
			go func(record *module.UploadRecords) {
				defer s.wg.Done()
				defer func() { <-s.workerSem }() // 释放工作协程信号量

				if crowdRuleId, err := s.processRecord(record); err != nil {
					log.Printf("Failed to process record %d: %v", record.ID, err)
					s.uploadModel.UpdateStatus(record.ID, "3")
					s.crowdRuleModel.UpdateCrowdRule(crowdRuleId, 3, "")
				} else {
					log.Printf("Successfully processed record %d", record.ID)
					s.uploadModel.UpdateStatus(record.ID, "2")
				}
			}(task)
		}
	}
}

// taskDiscovery 持续发现新任务并加入任务通道
func (s *UploadProcessorService) taskDiscovery() {
	ticker := time.NewTicker(10 * time.Second) // 每10秒检查一次新任务
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			log.Printf("Task taskDiscovery stopped: context cancelled")
			return
		case <-ticker.C:
			// 获取待处理的记录
			records, err := s.uploadModel.GetPendingRecords()
			if err != nil {
				log.Printf("Failed to get pending records: %v", err)
				continue
			}

			if len(records) == 0 {
				continue
			}

			log.Printf("Found %d pending records, adding to task queue", len(records))

			// 将任务加入通道
			for _, record := range records {
				select {
				case s.taskChan <- &record:
					log.Printf("Added record %d to task queue", record.ID)
					s.uploadModel.UpdateStatus(record.ID, "1") // 更新状态为处理中
				default:
					log.Printf("Task queue full, skipping record %d", record.ID)
				}
			}
		}
	}
}

// Stop 停止处理器
func (s *UploadProcessorService) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isRunning {
		return
	}

	if s.cancel != nil {
		s.cancel()
	}

	s.isRunning = false
	log.Printf("Upload processor stop signal sent")
}

// processRecord 处理单个记录
func (s *UploadProcessorService) processRecord(record *module.UploadRecords) (int, error) {
	log.Printf("Processing record %d: %s", record.ID, record.FileName)

	// 更新状态为处理中
	if err := s.uploadModel.UpdateStatus(record.ID, "1"); err != nil {
		return 0, fmt.Errorf("failed to update status: %w", err)
	}

	// 解析上传文件路径
	var uploadData UploadFileData
	var unescaped string
	if err := json.Unmarshal([]byte(record.UploadFilePaths), &unescaped); err != nil {
		return 0, fmt.Errorf("failed to unescape JSON: %w", err)
	}
	if err := json.Unmarshal([]byte(unescaped), &uploadData); err != nil {
		return 0, fmt.Errorf("failed to parse upload file paths: %w", err)
	}

	// 创建 crowd_rule 表数据
	crowdRuleId, err := s.crowdRuleModel.CreateCrowdRuleTable(record.FileName, record.CreateId)
	if err != nil {
		return 0, fmt.Errorf("failed to create crowd_rule table: %w", err)
	}

	// 统一处理所有文件，生成一个人群包文件和一条bitmap记录
	resultPath, err := s.processAllFiles(record, uploadData, crowdRuleId)
	if err != nil {
		return 0, fmt.Errorf("failed to process all files: %w", err)
	}

	// 更新记录文件路径
	if resultPath != "" {
		if err := s.uploadModel.UpdateRecordFilePaths(record.ID, resultPath); err != nil {
			return 0, fmt.Errorf("failed to update record file paths: %w", err)
		}
	}

	// 更新crowd_rule 表状态
	if err := s.crowdRuleModel.UpdateCrowdRule(crowdRuleId, 2, resultPath); err != nil {
		return 0, fmt.Errorf("failed to update crowd_rule table status: %w", err)
	}

	return crowdRuleId, nil
}

// processAllFiles 统一处理所有文件，生成一个人群包文件和一条bitmap记录
func (s *UploadProcessorService) processAllFiles(record *module.UploadRecords, uploadData UploadFileData, crowdRuleId int) (string, error) {
	log.Printf("Processing all files for record %d as a single task", record.ID)

	// 创建统一的临时表来存储所有设备ID
	tempTableName := fmt.Sprintf("temp_unified_%d_%d", record.ID, time.Now().UnixNano())
	if err := s.createUnifiedTempTable(tempTableName); err != nil {
		return "", fmt.Errorf("failed to create unified temp table: %w", err)
	}
	defer s.dropTempTable(tempTableName)

	// 处理所有类型的文件，将数据导入到统一的临时表
	totalFiles := 0

	// 处理 IMEI 文件
	if len(uploadData.ImeiFiles) > 0 {
		for _, file := range uploadData.ImeiFiles {
			log.Printf("Processing IMEI file: %s", file.FileName)
			if err := s.importFileToUnifiedTable(file.FilePath, tempTableName, "imei"); err != nil {
				return "", fmt.Errorf("failed to import IMEI file %s: %w", file.FileName, err)
			}
			totalFiles++
		}
	}

	// 处理 OAID 文件
	if len(uploadData.OaidFiles) > 0 {
		for _, file := range uploadData.OaidFiles {
			log.Printf("Processing OAID file: %s", file.FileName)
			if err := s.importFileToUnifiedTable(file.FilePath, tempTableName, "oaid"); err != nil {
				return "", fmt.Errorf("failed to import OAID file %s: %w", file.FileName, err)
			}
			totalFiles++
		}
	}

	// 处理 CAID 文件
	if len(uploadData.CaidFiles) > 0 {
		for _, file := range uploadData.CaidFiles {
			log.Printf("Processing CAID file: %s", file.FileName)
			if err := s.importFileToUnifiedTable(file.FilePath, tempTableName, "caid"); err != nil {
				return "", fmt.Errorf("failed to import CAID file %s: %w", file.FileName, err)
			}
			totalFiles++
		}
	}

	// 处理 IDFA 文件
	if len(uploadData.IdfaFiles) > 0 {
		for _, file := range uploadData.IdfaFiles {
			log.Printf("Processing IDFA file: %s", file.FileName)
			if err := s.importFileToUnifiedTable(file.FilePath, tempTableName, "idfa"); err != nil {
				return "", fmt.Errorf("failed to import IDFA file %s: %w", file.FileName, err)
			}
			totalFiles++
		}
	}

	// 处理 UserID 文件
	if len(uploadData.UserIdFiles) > 0 {
		for _, file := range uploadData.UserIdFiles {
			log.Printf("Processing UserID file: %s", file.FileName)
			if err := s.importFileToUnifiedTable(file.FilePath, tempTableName, "user_id"); err != nil {
				return "", fmt.Errorf("failed to import UserID file %s: %w", file.FileName, err)
			}
			totalFiles++
		}
	}

	if totalFiles == 0 {
		return "", fmt.Errorf("no files to process")
	}

	log.Printf("Successfully imported %d files to unified temp table %s", totalFiles, tempTableName)

	// 与 doris 进行匹配并生成统一的结果文件
	resultPath, err := s.matchUnifiedDataAndGenerateFile(crowdRuleId, tempTableName, record.FileName)
	if err != nil {
		return "", fmt.Errorf("failed to match unified data and generate file: %w", err)
	}

	return resultPath, nil
}

// createTempTable 创建临时表 - 针对 Doris 优化
// createUnifiedTempTable 创建统一的临时表，用于存储所有类型的设备ID
func (s *UploadProcessorService) createUnifiedTempTable(tableName string) error {
	dorisDB := mysqldb.Doris
	if dorisDB == nil {
		return fmt.Errorf("doris connection is not initialized")
	}

	createSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT,
			device_id VARCHAR(255) NOT NULL,
			device_type VARCHAR(10) NOT NULL
		) ENGINE=OLAP
		DUPLICATE KEY(id)
		DISTRIBUTED BY HASH(device_id) BUCKETS 10
		PROPERTIES (
			"replication_num" = "1"
		);
	`, tableName)

	log.Printf("Creating unified temp table with SQL: %s", createSQL)
	_, err := dorisDB.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create unified temp table: %w", err)
	}

	log.Printf("Created unified temp table: %s", tableName)
	return nil
}

// importFileToTempTable 导入文件数据到临时表 - 优化版本，支持大文件流式处理
// importFileToUnifiedTable 导入文件数据到统一的临时表
func (s *UploadProcessorService) importFileToUnifiedTable(filePath, tableName, deviceType string) error {
	startTime := time.Now()
	dorisDB := mysqldb.Doris
	if dorisDB == nil {
		return fmt.Errorf("doris connection is not initialized")
	}

	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// 获取文件大小用于日志
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()

	// 使用 bufio.Scanner 进行流式读取
	scanner := bufio.NewScanner(file)

	// 增加缓冲区大小以处理大文件
	const maxCapacity = 1024 * 1024 // 1MB
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	// 准备批量插入
	batchSize := 5000
	batch := make([]string, 0, batchSize)
	totalLines := 0
	batchCount := 0

	// 预编译插入语句
	insertSQL := fmt.Sprintf("INSERT INTO %s (device_id, device_type) VALUES ", tableName)

	log.Printf("Starting import of %s file %s (size: %d bytes) to unified table %s", deviceType, filePath, fileSize, tableName)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// 转义单引号防止SQL注入
		escapedLine := strings.ReplaceAll(line, "'", "''")
		batch = append(batch, fmt.Sprintf("('%s', '%s')", escapedLine, deviceType))
		totalLines++

		// 当批次满了时，执行插入
		if len(batch) >= batchSize {
			batchStart := time.Now()
			if err := s.executeBatchInsert(dorisDB, insertSQL, batch); err != nil {
				return fmt.Errorf("failed to insert batch %d at line %d: %w", batchCount+1, totalLines, err)
			}
			batchCount++
			batch = batch[:0] // 清空批次但保留容量

			// 记录批次处理时间
			batchDuration := time.Since(batchStart)
			if batchCount%10 == 0 { // 每10个批次记录一次
				log.Printf("Processed %d batches, %d lines, batch duration: %v", batchCount, totalLines, batchDuration)
			}
		}
	}

	// 处理剩余的数据
	if len(batch) > 0 {
		if err := s.executeBatchInsert(dorisDB, insertSQL, batch); err != nil {
			return fmt.Errorf("failed to insert final batch: %w", err)
		}
		batchCount++
	}

	// 检查扫描错误
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	duration := time.Since(startTime)
	log.Printf("Successfully imported %d %s lines from file %s to unified table %s in %v (%d batches, avg %.2f lines/sec)",
		totalLines, deviceType, filePath, tableName, duration, batchCount, float64(totalLines)/duration.Seconds())
	return nil
}

// matchDataAndGenerateFile 匹配数据并生成文件 - 使用 Doris INTO OUTFILE 直接导出
// dropTempTable 删除临时表
func (s *UploadProcessorService) dropTempTable(tableName string) {
	dorisDB := mysqldb.Doris
	if dorisDB == nil {
		log.Printf("Doris connection is not initialized, cannot drop table %s", tableName)
		return
	}

	// 为 Doris 使用 DROP TABLE 而不是 DROP TEMPORARY TABLE
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if _, err := dorisDB.Exec(dropSQL); err != nil {
		log.Printf("Failed to drop temp table %s: %v", tableName, err)
	} else {
		log.Printf("Successfully dropped temp table: %s", tableName)
	}
}

// IsRunning 检查处理器是否正在运行
func (s *UploadProcessorService) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isRunning
}

// executeBatchInsert 执行批量插入操作，带重试机制
func (s *UploadProcessorService) executeBatchInsert(db *sql.DB, insertSQL string, batch []string) error {
	if len(batch) == 0 {
		return nil
	}

	// 构建完整的插入语句
	fullSQL := insertSQL + strings.Join(batch, ",")

	// 重试机制
	maxRetries := 3
	for retry := 0; retry < maxRetries; retry++ {
		// 使用事务确保数据一致性
		tx, err := db.Begin()
		if err != nil {
			if retry == maxRetries-1 {
				return fmt.Errorf("failed to begin transaction after %d retries: %w", maxRetries, err)
			}
			log.Printf("Failed to begin transaction (retry %d/%d): %v", retry+1, maxRetries, err)
			time.Sleep(time.Duration(retry+1) * time.Second)
			continue
		}

		// 执行插入
		_, err = tx.Exec(fullSQL)
		if err != nil {
			tx.Rollback()
			if retry == maxRetries-1 {
				return fmt.Errorf("failed to execute batch insert after %d retries: %w", maxRetries, err)
			}
			log.Printf("Failed to execute batch insert (retry %d/%d): %v", retry+1, maxRetries, err)
			time.Sleep(time.Duration(retry+1) * time.Second)
			continue
		}

		// 提交事务
		if err := tx.Commit(); err != nil {
			if retry == maxRetries-1 {
				return fmt.Errorf("failed to commit transaction after %d retries: %w", maxRetries, err)
			}
			log.Printf("Failed to commit transaction (retry %d/%d): %v", retry+1, maxRetries, err)
			time.Sleep(time.Duration(retry+1) * time.Second)
			continue
		}

		// 成功执行，跳出重试循环
		break
	}

	return nil
}

// matchUnifiedDataAndGenerateFile 统一匹配数据并生成单个结果文件
func (s *UploadProcessorService) matchUnifiedDataAndGenerateFile(crowdRuleId int, tempTableName, fileName string) (string, error) {
	dorisDB := mysqldb.Doris
	if dorisDB == nil {
		return "", fmt.Errorf("doris connection is not initialized")
	}

	// 获取Doris IP
	dorisIP := core.GetConfig().DORIS_IP
	if dorisIP == "" {
		return "", fmt.Errorf("failed to parse doris ip from IP: %s", dorisIP)
	}

	// 使用 UNION ALL 将所有设备类型的匹配结果合并，使用 INTO OUTFILE 直接导出
	exportSQL := fmt.Sprintf(`
		SELECT d.hash_id, d.user_id, d.oaid, d.caid, d.idfa, d.imei
		FROM dmp_user_mapping_v4 d
		INNER JOIN %s t ON (
			(t.device_type = 'imei' AND d.imei = t.device_id) OR
			(t.device_type = 'oaid' AND d.oaid = t.device_id) OR
			(t.device_type = 'caid' AND d.caid = t.device_id) OR
			(t.device_type = 'idfa' AND d.idfa = t.device_id)
		)
		INTO OUTFILE "file:///tmp/"
		PROPERTIES (
			"column_separator" = "\t",
			"line_delimiter" = "\n"
		)
	`, tempTableName)

	log.Printf("Exporting unified data using SQL: %s", exportSQL)

	// 执行导出操作并获取返回数据
	rows, err := dorisDB.Query(exportSQL)
	if err != nil {
		return "", fmt.Errorf("failed to export unified data to file: %w", err)
	}
	defer rows.Close()

	// Doris返回的列通常为 FileNumber, TotalRows, FileSize, URL
	var fileURL string
	if rows.Next() {
		var fileNumber, totalRows, fileSize sql.NullInt64
		var url sql.NullString
		err := rows.Scan(&fileNumber, &totalRows, &fileSize, &url)
		if err != nil {
			return "", fmt.Errorf("failed to scan doris export result: %w", err)
		}
		if url.Valid {
			fileURL = url.String
		} else {
			return "", fmt.Errorf("doris export did not return file url")
		}
	} else {
		return "", fmt.Errorf("doris export did not return any result row")
	}

	// 统一聚合插入 bitmap - 将所有设备类型的匹配结果合并到一个bitmap中
	eventDate := time.Now().Format("2006-01-02 15:04:05")
	insertSQL := fmt.Sprintf(`
		INSERT INTO dmp_crowd_user_bitmap (crowd_rule_id, event_date, user_set)
		SELECT %d, '%s', bitmap_union(to_bitmap(d.hash_id))
		FROM dmp_user_mapping_v4 d
		INNER JOIN %s t ON (
			(t.device_type = 'imei' AND d.imei = t.device_id) OR
			(t.device_type = 'oaid' AND d.oaid = t.device_id) OR
			(t.device_type = 'caid' AND d.caid = t.device_id) OR
			(t.device_type = 'idfa' AND d.idfa = t.device_id)
		)
	`, crowdRuleId, eventDate, tempTableName)

	log.Printf("Inserting unified bitmap with SQL: %s", insertSQL)
	if _, err := dorisDB.Exec(insertSQL); err != nil {
		return "", fmt.Errorf("failed to insert unified bitmap: %w", err)
	}

	log.Printf("Successfully created unified crowd bitmap for crowd_rule_id %d with file URL: %s", crowdRuleId, fileURL)
	return fileURL, nil
}
