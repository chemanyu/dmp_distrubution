package mysqldb

import (
	"dmp_distribution/core"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Config 结构体定义了需要读取的配置项
var DB *gorm.DB

// LoadConfig 从配置文件中读取配置项
func InitMysql() {
	dsn := core.GetConfig().MYSQL_DB + "?charset=utf8&parseTime=True&loc=Local"
	var err error
	DB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database: " + err.Error())
	}

	// 获取底层 *sql.DB 并设置连接池
	sqlDB, err := DB.DB()
	if err != nil {
		panic("failed to get underlying *sql.DB: " + err.Error())
	}

	// 设置连接池参数
	sqlDB.SetMaxOpenConns(100)                 // 最大连接数
	sqlDB.SetMaxIdleConns(50)                  // 最大空闲连接数
	sqlDB.SetConnMaxLifetime(60 * time.Second) // 连接最大存活时间
}

func GetConnected() *gorm.DB {
	return DB
}
