package service

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	mysqldb "dmp_distribution/common/mysql"
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
	ImeiFiles []UploadFile `json:"imei-files"`
	OaidFiles []UploadFile `json:"oaid-files"`
	CaidFiles []UploadFile `json:"caid-files"`
	IdfaFiles []UploadFile `json:"idfa-files"`
}

// UploadProcessorService 上传处理服务
type UploadProcessorService struct {
	uploadModel *module.UploadRecords
	mu          sync.Mutex
	isRunning   bool
}

// NewUploadProcessorService 创建新的上传处理服务
func NewUploadProcessorService() *UploadProcessorService {
	return &UploadProcessorService{
		uploadModel: &module.UploadRecords{},
		isRunning:   false,
	}
}

// StartProcessor 启动处理器
func (s *UploadProcessorService) StartProcessor() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunning {
		log.Printf("Upload processor is already running")
		return
	}

	s.isRunning = true
	log.Printf("Starting upload processor")

	// 获取待处理的记录
	records, err := s.uploadModel.GetPendingRecords()
	if err != nil {
		log.Printf("Failed to get pending records: %v", err)
		return
	}

	if len(records) == 0 {
		log.Printf("No pending records found")
		s.isRunning = false
		return
	}

	// 有序处理每个记录
	for _, record := range records {
		if err := s.processRecord(&record); err != nil {
			log.Printf("Failed to process record %d: %v", record.ID, err)
			s.uploadModel.UpdateStatus(record.ID, "failed")
		} else {
			log.Printf("Successfully processed record %d", record.ID)
			s.uploadModel.UpdateStatus(record.ID, "completed")
		}
	}

	s.isRunning = false
	log.Printf("Upload processor finished")
}

// processRecord 处理单个记录
func (s *UploadProcessorService) processRecord(record *module.UploadRecords) error {
	log.Printf("Processing record %d: %s", record.ID, record.FileName)

	// 更新状态为处理中
	if err := s.uploadModel.UpdateStatus(record.ID, "processing"); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	// 解析上传文件路径
	var uploadData UploadFileData
	var unescaped string
	if err := json.Unmarshal([]byte(record.UploadFilePaths), &unescaped); err != nil {
		return fmt.Errorf("failed to unescape JSON: %w", err)
	}
	if err := json.Unmarshal([]byte(unescaped), &uploadData); err != nil {
		return fmt.Errorf("failed to parse upload file paths: %w", err)
	}

	var resultPaths []string

	// 处理不同类型的文件
	if len(uploadData.ImeiFiles) > 0 {
		paths, err := s.processFilesByType(record, uploadData.ImeiFiles, "imei")
		if err != nil {
			return fmt.Errorf("failed to process imei files: %w", err)
		}
		resultPaths = append(resultPaths, paths...)
	}

	if len(uploadData.OaidFiles) > 0 {
		paths, err := s.processFilesByType(record, uploadData.OaidFiles, "oaid")
		if err != nil {
			return fmt.Errorf("failed to process oaid files: %w", err)
		}
		resultPaths = append(resultPaths, paths...)
	}

	if len(uploadData.CaidFiles) > 0 {
		paths, err := s.processFilesByType(record, uploadData.CaidFiles, "caid")
		if err != nil {
			return fmt.Errorf("failed to process caid files: %w", err)
		}
		resultPaths = append(resultPaths, paths...)
	}

	if len(uploadData.IdfaFiles) > 0 {
		paths, err := s.processFilesByType(record, uploadData.IdfaFiles, "idfa")
		if err != nil {
			return fmt.Errorf("failed to process idfa files: %w", err)
		}
		resultPaths = append(resultPaths, paths...)
	}

	// 更新记录文件路径
	if len(resultPaths) > 0 {
		recordFilePaths := strings.Join(resultPaths, ",")
		if err := s.uploadModel.UpdateRecordFilePaths(record.ID, recordFilePaths); err != nil {
			return fmt.Errorf("failed to update record file paths: %w", err)
		}
	}

	return nil
}

// processFilesByType 按类型处理文件
func (s *UploadProcessorService) processFilesByType(record *module.UploadRecords, files []UploadFile, dataType string) ([]string, error) {
	var resultPaths []string

	for _, file := range files {
		log.Printf("Processing %s file: %s", dataType, file.FileName)

		// 创建临时表 - 使用唯一的表名
		tempTableName := fmt.Sprintf("temp_%s_%d_%d", dataType, record.ID, time.Now().UnixNano())
		if err := s.createTempTable(tempTableName, dataType); err != nil {
			return nil, fmt.Errorf("failed to create temp table: %w", err)
		}

		// 导入文件数据到临时表
		if err := s.importFileToTempTable(file.FilePath, tempTableName, dataType); err != nil {
			s.dropTempTable(tempTableName)
			return nil, fmt.Errorf("failed to import file to temp table: %w", err)
		}

		// 与 doris 进行匹配并生成结果文件
		resultPath, err := s.matchDataAndGenerateFile(record, tempTableName, dataType)
		if err != nil {
			s.dropTempTable(tempTableName)
			return nil, fmt.Errorf("failed to match data and generate file: %w", err)
		}

		resultPaths = append(resultPaths, resultPath)

		// 清理临时表
		s.dropTempTable(tempTableName)
	}

	return resultPaths, nil
}

// createTempTable 创建临时表 - 针对 Doris 优化
func (s *UploadProcessorService) createTempTable(tableName, dataType string) error {
	dorisDB := mysqldb.Doris
	if dorisDB == nil {
		return fmt.Errorf("doris connection is not initialized")
	}

	var createSQL string
	switch dataType {
	case "imei":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT NOT NULL AUTO_INCREMENT,
				imei VARCHAR(255) NOT NULL
			) ENGINE=OLAP
			DUPLICATE KEY(id)
			DISTRIBUTED BY HASH(imei) BUCKETS 10
			PROPERTIES (
				"replication_num" = "1"
			);
		`, tableName)
	case "oaid":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT NOT NULL AUTO_INCREMENT,
				oaid VARCHAR(255) NOT NULL
			) ENGINE=OLAP
			DUPLICATE KEY(id)
			DISTRIBUTED BY HASH(oaid) BUCKETS 10
			PROPERTIES (
				"replication_num" = "1"
			);
		`, tableName)
	case "caid":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT NOT NULL AUTO_INCREMENT,
				caid VARCHAR(255) NOT NULL
			) ENGINE=OLAP
			DUPLICATE KEY(id)
			DISTRIBUTED BY HASH(caid) BUCKETS 10
			PROPERTIES (
				"replication_num" = "1"
			);
		`, tableName)
	case "idfa":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT NOT NULL AUTO_INCREMENT,
				idfa VARCHAR(255) NOT NULL
			) ENGINE=OLAP
			DUPLICATE KEY(id)
			DISTRIBUTED BY HASH(idfa) BUCKETS 10
			PROPERTIES (
				"replication_num" = "1"
			);
		`, tableName)
	default:
		return fmt.Errorf("unsupported data type: %s", dataType)
	}
	log.Print("Creating temp table with SQL:", createSQL)
	_, err := dorisDB.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	log.Printf("Created temp table: %s", tableName)
	return nil
}

// importFileToTempTable 导入文件数据到临时表 - 优化版本，支持大文件流式处理
func (s *UploadProcessorService) importFileToTempTable(filePath, tableName, dataType string) error {
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
	batchSize := 5000 // 增加批次大小以提高性能
	batch := make([]string, 0, batchSize)
	totalLines := 0
	batchCount := 0

	// 预编译插入语句以提高性能
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tableName, dataType)

	log.Printf("Starting import of file %s (size: %d bytes) to table %s", filePath, fileSize, tableName)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// 转义单引号防止SQL注入
		escapedLine := strings.ReplaceAll(line, "'", "''")
		batch = append(batch, fmt.Sprintf("('%s')", escapedLine))
		totalLines++

		// 当批次满了或者是最后一批时，执行插入
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
	log.Printf("Successfully imported %d lines from file %s to table %s in %v (%d batches, avg %.2f lines/sec)",
		totalLines, filePath, tableName, duration, batchCount, float64(totalLines)/duration.Seconds())
	return nil
}

// matchDataAndGenerateFile 匹配数据并生成文件
func (s *UploadProcessorService) matchDataAndGenerateFile(record *module.UploadRecords, tempTableName, dataType string) (string, error) {
	// 从 doris 查询匹配的数据
	matchedData, err := s.queryMatchedDataFromDoris(tempTableName, dataType)
	if err != nil {
		return "", fmt.Errorf("failed to query matched data from doris: %w", err)
	}

	// 生成结果文件
	resultPath, err := s.generateResultFile(record, matchedData, dataType)
	if err != nil {
		return "", fmt.Errorf("failed to generate result file: %w", err)
	}

	// 保存 hash_id 到 dmp_crowd_user_bitmap
	if err := s.saveToCrowdUserBitmap(record, matchedData); err != nil {
		return "", fmt.Errorf("failed to save to crowd user bitmap: %w", err)
	}

	return resultPath, nil
}

// queryMatchedDataFromDoris 从 doris 查询匹配的数据
func (s *UploadProcessorService) queryMatchedDataFromDoris(tempTableName, dataType string) ([]map[string]interface{}, error) {
	dorisDB := mysqldb.Doris
	if dorisDB == nil {
		return nil, fmt.Errorf("doris connection is not initialized")
	}

	var querySQL string
	switch dataType {
	case "imei":
		querySQL = fmt.Sprintf(`
			SELECT d.hash_id, d.user_id, d.imei
			FROM dmp_user_mapping_v2 d
			INNER JOIN %s t ON d.imei = t.imei
		`, tempTableName)
	case "oaid":
		querySQL = fmt.Sprintf(`
			SELECT d.hash_id, d.user_id, d.oaid
			FROM dmp_user_mapping_v2 d
			INNER JOIN %s t ON d.oaid = t.oaid
		`, tempTableName)
	case "caid":
		querySQL = fmt.Sprintf(`
			SELECT d.hash_id, d.user_id, d.caid
			FROM dmp_user_mapping_v2 d
			INNER JOIN %s t ON d.caid = t.caid
		`, tempTableName)
	case "idfa":
		querySQL = fmt.Sprintf(`
			SELECT d.hash_id, d.user_id, d.idfa
			FROM dmp_user_mapping_v2 d
			INNER JOIN %s t ON d.idfa = t.idfa
		`, tempTableName)
	default:
		return nil, fmt.Errorf("unsupported data type: %s", dataType)
	}

	rows, err := dorisDB.Query(querySQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query doris: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var hashID, userID, deviceID sql.NullString
		if err := rows.Scan(&hashID, &userID, &deviceID); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		result := map[string]interface{}{
			"hash_id":   hashID.String,
			"user_id":   userID.String,
			"device_id": deviceID.String,
			"type":      dataType,
		}
		results = append(results, result)
	}

	return results, nil
}

// generateResultFile 生成结果文件
func (s *UploadProcessorService) generateResultFile(record *module.UploadRecords, data []map[string]interface{}, dataType string) (string, error) {
	// 创建结果文件目录
	resultDir := filepath.Join("results", fmt.Sprintf("%d", record.ID))
	if err := os.MkdirAll(resultDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create result directory: %w", err)
	}

	// 生成结果文件名
	timestamp := time.Now().Format("20060102150405")
	resultFileName := fmt.Sprintf("%s_%s_%s.txt", record.FileName, dataType, timestamp)
	resultPath := filepath.Join(resultDir, resultFileName)

	// 创建结果文件
	file, err := os.Create(resultPath)
	if err != nil {
		return "", fmt.Errorf("failed to create result file: %w", err)
	}
	defer file.Close()

	// 写入数据
	for _, row := range data {
		line := fmt.Sprintf("%s\t%s\t%s\n",
			row["hash_id"], row["user_id"], row["device_id"])
		if _, err := file.WriteString(line); err != nil {
			return "", fmt.Errorf("failed to write to result file: %w", err)
		}
	}

	log.Printf("Generated result file: %s with %d records", resultPath, len(data))
	return resultPath, nil
}

// saveToCrowdUserBitmap 保存到人群用户位图
func (s *UploadProcessorService) saveToCrowdUserBitmap(record *module.UploadRecords, data []map[string]interface{}) error {
	dorisDB := mysqldb.Doris
	if dorisDB == nil {
		return fmt.Errorf("doris connection is not initialized")
	}

	// 批量插入到 dmp_crowd_user_bitmap
	batchSize := 1000
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		var values []string
		for j := i; j < end; j++ {
			// 假设 crowd_id 来自 record.ID，event_date 为今天
			eventDate := time.Now().Format("2006-01-02")
			values = append(values, fmt.Sprintf("(%d, '%s', b'1')",
				record.ID, eventDate))
		}

		if len(values) > 0 {
			insertSQL := fmt.Sprintf(`
				INSERT INTO dmp_crowd_user_bitmap (crowd_id, event_date, user_set) 
				VALUES %s
			`, strings.Join(values, ","))

			if _, err := dorisDB.Exec(insertSQL); err != nil {
				return fmt.Errorf("failed to insert into crowd user bitmap: %w", err)
			}
		}
	}

	log.Printf("Saved %d hash_ids to dmp_crowd_user_bitmap for record %d", len(data), record.ID)
	return nil
}

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
