package module

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
	records := []Distribution{}
	db := DbInstance.Debug()

	db.Find(&records)
	return records, len(records), nil
}

func (d *Distribution) UpdateStatus(id int, status int) error {
	// TODO: Implement the logic to update the status in the database.
	// For example, using GORM:
	// return db.Model(&Distribution{}).Where("id = ?", id).Update("status", status).Error
	return nil
}
