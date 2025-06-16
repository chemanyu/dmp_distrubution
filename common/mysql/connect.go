package mysqldb

import (
	"database/sql"
	"dmp_distribution/core"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Config 结构体定义了需要读取的配置项
var MysqlDbs *sql.DB

// LoadConfig 从配置文件中读取配置项
func InitMysql() {
	db, err := sql.Open("mysql", core.GetConfig().MYSQL_DB+"?charset=utf8&multiStatements=true")
	if err != nil {
		panic(err.Error())
		return
	}
	err = db.Ping()
	if err != nil {
		fmt.Println("Failed to connect to mysql, err:" + err.Error())
		panic(err.Error())
		return
	}
	MysqlDbs = db
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(50)
	db.SetConnMaxLifetime(60 * time.Second)
}

// GetConfig 返回已经读取的配置项
func GetConnected() *sql.DB {
	return MysqlDbs
}
