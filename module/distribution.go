package module

import (
	"time"

	"gorm.io/gorm"
	"gorm.io/driver/mysql"
	"github.com/pkg/errors"
	
	"github.com/chemanyu/workspace/meishu/dmp_distribution/core"
)

var (
	db  *gorm.DB
	err error
)

// InitDB 初始化数据库连接
func InitDB() error {
	config := core.GetConfig()
	if config == nil {
		return errors.New("config not loaded")
	}

	db, err = gorm.Open(mysql.Open(config.MYSQL_DB), &gorm.Config{})
	if err != nil {
		return errors.Wrap(err, "failed to connect to database")
	}

	// 设置连接池
	sqlDB, err := db.DB()
	if err != nil {
		return errors.Wrap(err, "failed to get sql.DB")
	}

	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	return nil
}

// GetDB 获取数据库连接实例
func GetDB() *gorm.DB {
	return db
}

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

// DistributionModel 处理Distribution相关的数据库操作
type DistributionModel struct {
	db *gorm.DB
}

// NewDistributionModel 创建新的DistributionModel实例
func NewDistributionModel(db *gorm.DB) *DistributionModel {
	return &DistributionModel{db: db}
}

// Create 创建新的分发任务
func (m *DistributionModel) Create(dist *Distribution) error {
	dist.CreateTime = time.Now().Unix()
	return m.db.Create(dist).Error
}

// GetByID 通过ID获取分发任务
func (m *DistributionModel) GetByID(id int) (*Distribution, error) {
	var dist Distribution
	err := m.db.First(&dist, id).Error
	if err != nil {
		return nil, err
	}
	return &dist, nil
}

// List 获取分发任务列表
func (m *DistributionModel) List(query map[string]interface{}, page, pageSize int) ([]Distribution, int64, error) {
	var dists []Distribution
	var total int64

	db := m.db.Model(&Distribution{})

	// 应用查询条件
	for k, v := range query {
		if v != nil && v != "" {
			db = db.Where(k+" = ?", v)
		}
	}

	// 获取总数
	err := db.Count(&total).Error
	if err != nil {
		return nil, 0, err
	}

	// 获取分页数据
	err = db.Offset((page - 1) * pageSize).
		Limit(pageSize).
		Order("id DESC").
		Find(&dists).Error
	if err != nil {
		return nil, 0, err
	}

	return dists, total, nil
}

// UpdateStatus 更新任务状态
func (m *DistributionModel) UpdateStatus(id int, status int8) error {
	return m.db.Model(&Distribution{}).
		Where("id = ?", id).
		Update("status", status).Error
}

// Update 更新分发任务
func (m *DistributionModel) Update(id int, updates map[string]interface{}) error {
	return m.db.Model(&Distribution{}).
		Where("id = ?", id).
		Updates(updates).Error
}

// Delete 删除分发任务
func (m *DistributionModel) Delete(id int) error {
	return m.db.Delete(&Distribution{}, id).Error
}

// BatchCreate 批量创建分发任务
func (m *DistributionModel) BatchCreate(dists []*Distribution) error {
	now := time.Now().Unix()
	for _, dist := range dists {
		dist.CreateTime = now
	}
	return m.db.Create(dists).Error
}

// GetByStrategyID 通过策略ID获取分发任务
func (m *DistributionModel) GetByStrategyID(strategyID int) ([]Distribution, error) {
	var dists []Distribution
	err := m.db.Where("strategy_id = ?", strategyID).Find(&dists).Error
	return dists, err
}

// UpdateStatusByIDs 批量更新任务状态
func (m *DistributionModel) UpdateStatusByIDs(ids []int, status int8) error {
	return m.db.Model(&Distribution{}).
		Where("id IN ?", ids).
		Update("status", status).Error
}
