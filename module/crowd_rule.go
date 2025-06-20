package module

import (
	mysqldb "dmp_distribution/common/mysql"
	"time"
)

// CrowdRule 人群规则表
type CrowdRule struct {
	ID          int       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Name        string    `gorm:"column:name;size:64;not null;default:''" json:"name"`                  // 规则名称
	CategoryID  int       `gorm:"column:category_id;not null;default:0" json:"category_id"`             // 分类id
	Desc        string    `gorm:"column:desc;size:255;not null;default:''" json:"desc"`                 // 规则描述
	ExecID      int8      `gorm:"column:exec_id;not null;default:0" json:"exec_id"`                     // 执行类型 1-实时执行 2-每日执行 3-每周执行
	CreateID    int       `gorm:"column:create_id;not null;default:0" json:"create_id"`                 // 创建人id
	CreateName  string    `gorm:"column:create_name;size:32;not null;default:''" json:"create_name"`    // 创建人名称
	FilePath    string    `gorm:"column:file_path;size:255;not null;default:''" json:"file_path"`       // 标签规则结果文件路径
	LabelJSON   string    `gorm:"column:label_json;type:text" json:"label_json"`                        // 标签json
	ExecStatus  int8      `gorm:"column:exec_status;not null;default:0" json:"exec_status"`             // 状态 0-待执行 1-执行中 2-执行成功 3-执行失败
	ExecTime    int       `gorm:"column:exec_time;not null;default:0" json:"exec_time"`                 // 执行时间
	FailMessage string    `gorm:"column:fail_message;size:255;not null;default:''" json:"fail_message"` // 执行失败原因
	Status      int8      `gorm:"column:status;not null;default:1" json:"status"`                       // 规则状态 1-有效 2-无效
	CreateTime  time.Time `gorm:"column:create_time;type:timestamp;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  time.Time `gorm:"column:update_time;type:timestamp;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" json:"update_time"`
}

// TableName 指定表名
func (c *CrowdRule) TableName() string {
	return "crowd_rule"
}

// ExecType 执行类型常量
const (
	ExecTypeRealTime = int8(1) // 实时执行
	ExecTypeDaily    = int8(2) // 每日执行
	ExecTypeWeekly   = int8(3) // 每周执行
)

// ExecStatus 执行状态常量
const (
	ExecStatusPending = int8(0) // 待执行
	ExecStatusRunning = int8(1) // 执行中
	ExecStatusSuccess = int8(2) // 执行成功
	ExecStatusFailed  = int8(3) // 执行失败
)

// Status 规则状态常量
const (
	StatusValid   = int8(1) // 有效
	StatusInvalid = int8(2) // 无效
)

var (
	CrowdRuleMapper = new(CrowdRule)
)

// GetExecTime 获取指定规则的执行时间
func (c *CrowdRule) GetExecTime(ruleID int) (int, error) {
	db := mysqldb.GetConnected()
	var rule CrowdRule
	err := db.Model(&CrowdRule{}).
		Select("exec_time").
		Where("id = ?", ruleID).
		First(&rule).Error
	if err != nil {
		return 0, err
	}
	return rule.ExecTime, nil
}
