package module

import (
	mysqldb "dmp_distribution/common/mysql"
)

// Distribution 人群包分发任务表
type Distribution struct {
	ID         int    `gorm:"column:id;primaryKey;autoIncrement"`
	StrategyID int    `gorm:"column:strategy_id;not null" json:"strategy_id"`
	Imei       int8   `gorm:"column:imei;not null;default:0" json:"imei"`
	Oaid       int8   `gorm:"column:oaid;not null;default:0" json:"oaid"`
	Idfa       int8   `gorm:"column:idfa;not null;default:0" json:"idfa"`
	Caid       int8   `gorm:"column:caid;not null;default:0" json:"caid"`
	Caid2      int8   `gorm:"column:caid2;not null;default:0" json:"caid2"`
	UserID     int8   `gorm:"column:user_id;not null;default:0" json:"user_id"`
	Type       int8   `gorm:"column:type;not null;default:0" json:"type"` // 1-内部平台，2-外部平台，3-下载数据文件
	Platform   string `gorm:"column:platform;size:32" json:"platform"`    // 平台名称
	Crowd      string `gorm:"column:crowd;size:32" json:"crowd"`          // 人群包id
	Path       string `gorm:"column:path;size:128" json:"path"`           // 源文件地址
	Status     int8   `gorm:"column:status;default:0" json:"status"`      // 0-等待中，1-执行中，2-已结束
	CreateTime int64  `gorm:"column:create_time;not null" json:"create_time"`
	IsDel      int8   `gorm:"column:is_del;not null;default:0" json:"is_del"` // 0-未删除，1-已删除
}

// TableName 指定表名
func (d *Distribution) TableName() string {
	return "dmp_distribution"
}

var (
	DistributionMapper = new(Distribution)
)

// List 获取分发任务列表
func (m *Distribution) List(query map[string]interface{}, page, pageSize int) ([]Distribution, int, error) {
	db := mysqldb.GetConnected()
	records := []Distribution{}

	// 动态条件
	queryDB := db.Model(&Distribution{})
	if strategyID, ok := query["strategy_id"]; ok {
		queryDB = queryDB.Where("strategy_id = ?", strategyID)
	}
	if status, ok := query["status"]; ok {
		queryDB = queryDB.Where("status = ?", status)
	}
	queryDB = queryDB.Where("is_del = ?", 0) // 只查询未删除的记录

	// 分页
	if page > 0 && pageSize > 0 {
		queryDB = queryDB.Offset((page - 1) * pageSize).Limit(pageSize)

	}
	var total int64
	queryDB.Count(&total)
	err := queryDB.Find(&records).Error
	if err != nil {
		return nil, 0, err
	}

	return records, int(total), nil
}

func (d *Distribution) UpdateStatus(id int, status int) error {
	db := mysqldb.GetConnected()
	return db.Model(&Distribution{}).Where("id = ?", id).Update("status", status).Error
}
