package mysqldb

import (
	"database/sql"
	"dmp_distribution/core"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Config 结构体定义了需要读取的配置项
var Doris *sql.DB

// LoadConfig 从配置文件中读取配置项
func InitDoris() {
	db, err := sql.Open("mysql", core.GetConfig().DORIS_DB+"?charset=utf8&multiStatements")
	if err != nil {
		panic(err.Error())
	}
	err = db.Ping()
	if err != nil {
		fmt.Println("Failed to connect to mysql, err:" + err.Error())
		panic(err.Error())
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(50)
	db.SetConnMaxLifetime(60 * time.Second)
	Doris = db
}

func GetConnectedDoris() *sql.DB {
	if Doris == nil {
		log.Panic("Database connection doris is not initialized. Call InitDoris() first")
	}
	return Doris
}
