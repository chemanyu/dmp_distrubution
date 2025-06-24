package core

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/spf13/viper"
)

// Config 结构体定义了需要读取的配置项
type Config struct {
	MYSQL_DB          string
	DORIS_DB          string
	REDIS_DB          string
	REDIS_POOL_DB     string
	REDIS_POOL_DB_CRC string
	SERVER_PORT       string
	SERVER_ADDRESS    string
	LOG_PATH          string
	SYSTEM_MODE       string
	CALLBACK_BASE_URL string
	PPROF             string
}

var config *Config

// LoadConfig 从配置文件中读取配置项
func LoadConfig(cfg string) *Config {

	absPath, _ := filepath.Abs(cfg)
	// 配置viper
	viper.SetConfigFile(absPath)
	viper.SetConfigType("yaml") // 明确指定配置类型
	viper.ReadInConfig()

	config = &Config{
		MYSQL_DB:          getViperStringValue("MYSQL_DB"),
		DORIS_DB:          getViperStringValue("DORIS_DB"),
		REDIS_DB:          getViperStringValue("REDIS_DB"),
		REDIS_POOL_DB:     getViperStringValue("REDIS_POOL_DB"),
		REDIS_POOL_DB_CRC: getViperStringValue("REDIS_POOL_DB_CRC"),
		SERVER_PORT:       getViperStringValue("SERVER_PORT"),
		// SERVER_ADDRESS:    getViperStringValue("SERVER_ADDRESS"),
		// LOG_PATH:          getViperStringValue("LOG_PATH"),
		// SYSTEM_MODE:       getViperStringValue("SYSTEM_MODE"),       //release debug
		// CALLBACK_BASE_URL: getViperStringValue("CALLBACK_BASE_URL"), //release debug
		// PPROF:             getViperStringValue("PPROF"),             //release debug
	}
	return config
}

// GetConfig 返回已经读取的配置项
func GetConfig() *Config {
	if config == nil {
		panic("Config not initialized. Call LoadConfig first.")
	}
	return config
}

// getViperStringValue 从 viper 中读取配置项的值
func getViperStringValue(key string) string {
	value := viper.GetString(key)
	if value == "" {
		configFile := viper.ConfigFileUsed()
		log.Printf("Failed to get value for key %s. Current config file: %s", key, configFile)
		log.Printf("Available keys: %v", viper.AllKeys())
		panic(fmt.Errorf("%s 必须在环境变量或 config.yaml 文件中提供", key))
	}
	return value
}
