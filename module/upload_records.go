package module

import (
	mysqldb "dmp_distribution/common/mysql"
	"time"
)

// UploadRecords 上传记录表
type UploadRecords struct {
	ID              int       `gorm:"column:id;primaryKey;autoIncrement"`
	FileName        string    `gorm:"column:file_name;not null;size:255" json:"file_name"`
	DataType        string    `gorm:"column:data_type;size:50" json:"data_type"`
	FileSize        int64     `gorm:"column:file_size" json:"file_size"`
	RecordCount     int       `gorm:"column:record_count" json:"record_count"`
	UploadFilePaths string    `gorm:"column:upload_file_paths;type:text" json:"upload_file_paths"`
	UploadTime      time.Time `gorm:"column:upload_time;not null" json:"upload_time"`
	Status          string    `gorm:"column:status;not null;size:20" json:"status"`
	RecordFilePaths string    `gorm:"column:record_file_paths;type:text" json:"record_file_paths"`
}

// TableName 指定表名
func (u *UploadRecords) TableName() string {
	return "upload_records"
}

var (
	UploadRecordsMapper = new(UploadRecords)
)

// GetPendingRecords 获取状态为 pending 的记录
func (u *UploadRecords) GetPendingRecords() ([]UploadRecords, error) {
	db := mysqldb.GetConnected()
	var records []UploadRecords

	err := db.Model(&UploadRecords{}).
		Where("status = ?", "pending").
		Find(&records).Error

	return records, err
}

// List 获取上传记录列表
func (u *UploadRecords) List(query map[string]interface{}, page, pageSize int) ([]UploadRecords, int, error) {
	db := mysqldb.GetConnected()
	var records []UploadRecords

	queryDB := db.Model(&UploadRecords{})

	// 动态条件
	if status, ok := query["status"]; ok {
		queryDB = queryDB.Where("status = ?", status)
	}

	if dataType, ok := query["data_type"]; ok {
		queryDB = queryDB.Where("data_type = ?", dataType)
	}

	// 分页
	if page > 0 && pageSize > 0 {
		queryDB = queryDB.Offset((page - 1) * pageSize).Limit(pageSize)
	}

	var total int64
	err := queryDB.Count(&total).Error
	if err != nil {
		return nil, 0, err
	}

	err = queryDB.Order("upload_time DESC").Find(&records).Error
	if err != nil {
		return nil, 0, err
	}

	return records, int(total), nil
}

// UpdateStatus 更新记录状态
func (u *UploadRecords) UpdateStatus(id int, status string) error {
	db := mysqldb.GetConnected()
	return db.Model(&UploadRecords{}).
		Where("id = ?", id).
		Update("status", status).Error
}

// UpdateRecordFilePaths 更新记录文件路径
func (u *UploadRecords) UpdateRecordFilePaths(id int, paths string) error {
	db := mysqldb.GetConnected()
	return db.Model(&UploadRecords{}).
		Where("id = ?", id).
		Update("record_file_paths", paths).Error
}
