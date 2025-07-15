package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
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
	if err := json.Unmarshal([]byte(record.UploadFilePaths), &uploadData); err != nil {
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

		// 创建临时表
		tempTableName := fmt.Sprintf("temp_%s_%d_%d", dataType, record.ID, time.Now().Unix())
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

// createTempTable 创建临时表
func (s *UploadProcessorService) createTempTable(tableName, dataType string) error {
	db := mysqldb.GetConnectedDoris()

	var createSQL string
	switch dataType {
	case "imei":
		createSQL = fmt.Sprintf(`
			CREATE TEMPORARY TABLE %s (
				id INT AUTO_INCREMENT PRIMARY KEY,
				imei VARCHAR(255) NOT NULL,
				INDEX idx_imei (imei)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		`, tableName)
	case "oaid":
		createSQL = fmt.Sprintf(`
			CREATE TEMPORARY TABLE %s (
				id INT AUTO_INCREMENT PRIMARY KEY,
				oaid VARCHAR(255) NOT NULL,
				INDEX idx_oaid (oaid)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		`, tableName)
	case "caid":
		createSQL = fmt.Sprintf(`
			CREATE TEMPORARY TABLE %s (
				id INT AUTO_INCREMENT PRIMARY KEY,
				caid VARCHAR(255) NOT NULL,
				INDEX idx_caid (caid)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		`, tableName)
	case "idfa":
		createSQL = fmt.Sprintf(`
			CREATE TEMPORARY TABLE %s (
				id INT AUTO_INCREMENT PRIMARY KEY,
				idfa VARCHAR(255) NOT NULL,
				INDEX idx_idfa (idfa)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		`, tableName)
	default:
		return fmt.Errorf("unsupported data type: %s", dataType)
	}

	result, err := db.Exec(createSQL)
	if err != nil {
		return err
	}
	log.Printf("Created temporary table %s with result: %v", tableName, result)
	return nil
}

// importFileToTempTable 导入文件数据到临时表
func (s *UploadProcessorService) importFileToTempTable(filePath, tableName, dataType string) error {
	db := mysqldb.GetConnected()

	// 读取文件内容
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// 按行分割文件内容
	lines := strings.Split(string(content), "\n")

	// 批量插入数据
	batchSize := 1000
	for i := 0; i < len(lines); i += batchSize {
		end := i + batchSize
		if end > len(lines) {
			end = len(lines)
		}

		var values []string
		for j := i; j < end; j++ {
			line := strings.TrimSpace(lines[j])
			if line == "" {
				continue
			}
			values = append(values, fmt.Sprintf("('%s')", line))
		}

		if len(values) > 0 {
			insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
				tableName, dataType, strings.Join(values, ","))
			if err := db.Exec(insertSQL).Error; err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
		}
	}

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
	db := mysqldb.GetConnected()
	if err := db.Exec(fmt.Sprintf("DROP TEMPORARY TABLE IF EXISTS %s", tableName)).Error; err != nil {
		log.Printf("Failed to drop temp table %s: %v", tableName, err)
	}
}

// IsRunning 检查处理器是否正在运行
func (s *UploadProcessorService) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isRunning
}
