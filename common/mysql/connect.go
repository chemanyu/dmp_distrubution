package mysqldb

import (
	"dmp_distribution/core"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"database/sql"
)

// Config 结构体定义了需要读取的配置项
var db *gorm.DB

// LoadConfig 从配置文件中读取配置项
func InitMysql() {
	dsn := core.GetConfig().MYSQL_DB + "?charset=utf8&parseTime=True&loc=Local"
	var err error
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database: " + err.Error())
	}

	// 获取底层 *sql.DB 并设置连接池
	sqlDB, err := db.DB()
	if err != nil {
		panic("failed to get underlying *sql.DB: " + err.Error())
	}

	// 设置连接池参数
	sqlDB.SetMaxOpenConns(100)                 // 最大连接数
	sqlDB.SetMaxIdleConns(50)                  // 最大空闲连接数
	sqlDB.SetConnMaxLifetime(10 * time.Second) // 连接最大存活时间

	// 添加连接池状态监控
	go monitorConnectionPool(sqlDB)
}

func monitorConnectionPool(sqlDB *sql.DB) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		stats := sqlDB.Stats()
		log.Printf("DB Pool Stats - Open: %d, InUse: %d, Idle: %d, WaitCount: %d, WaitDuration: %v",
			stats.OpenConnections,
			stats.InUse,
			stats.Idle,
			stats.WaitCount,
			stats.WaitDuration,
		)

		// 如果等待连接的次数过多，记录警告
		if stats.WaitCount > 100 {
			log.Printf("WARNING: High connection wait count detected: %d", stats.WaitCount)
		}
	}
}

func GetConnected() *gorm.DB {
	if db == nil {
		log.Panic("Database connection is not initialized. Call InitMysql() first")
	}
	return db
}
