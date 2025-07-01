package platform

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	"dmp_distribution/common/redis"
	"dmp_distribution/module"
)

// Adn ADN平台实现
type Adn struct{}

// StrMd5 计算字符串的MD5值
func StrMd5(str string) string {
	hash := md5.New()
	hash.Write([]byte(str))
	return hex.EncodeToString(hash.Sum(nil))
}

// IsMD5 判断字符串是否是MD5值
func IsMD5(str string) bool {
	if len(str) != 32 {
		return false
	}
	// MD5值只包含16进制字符 (0-9, a-f)
	for _, c := range str {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// Distribution 实现批量数据分发到ADN平台
func (a *Adn) Distribution(task *module.Distribution, batches []map[string]string) error {
	// 获取当前小时和剩余小时数
	now := time.Now()
	hour := now.Hour()
	remainingHours := 24 - hour

	// 计算明天的日期
	tomorrow := now.AddDate(0, 0, 1).Format("2006-01-02")
	expirationDate, err := time.Parse("2006-01-02", tomorrow)
	if err != nil {
		return fmt.Errorf("failed to parse expiration date: %v", err)
	}

	// 计算过期时间（秒）
	expirationSeconds := int(expirationDate.Sub(now).Seconds())
	crowdID := task.Crowd

	// 批量处理所有设备
	for _, device := range batches {
		// 对每个设备标识进行处理
		for _, deviceID := range device {
			if deviceID == "" {
				continue
			}

			// 计算设备ID的MD5作为key
			// 判断是否已经是MD5值
			var deviceMD5 string
			if IsMD5(deviceID) {
				deviceMD5 = strings.ToLower(deviceID) // 确保MD5值为小写
			} else {
				deviceMD5 = StrMd5(deviceID)
			}
			keyName := fmt.Sprintf("crowd_%s", deviceMD5)

			// 设置字段值（crowd_id -> expiration_timestamp）
			fieldsAndValues := map[string]interface{}{
				crowdID: now.Unix() + int64(remainingHours)*3600, // 当前时间戳 + 剩余小时数 * 3600秒
			}

			// 使用CRC32_RedisPool推送数据
			// log.Print("keyName", keyName)
			// log.Print("Setting hash data", fieldsAndValues)
			err := redis.C32_Redis_Pools.MSetHash_KeyData_Redis(keyName, fieldsAndValues)
			if err != nil {
				log.Printf("Failed to set hash data in Redis for device %s: %v", deviceID, err)
				continue
			}

			// 设置过期时间
			// log.Print("keyName", keyName)
			// log.Print("expires in seconds", expirationSeconds)
			err = redis.C32_Redis_Pools.Expire(keyName, expirationSeconds)
			if err != nil {
				log.Printf("Failed to set expiration for device %s: %v", deviceID, err)
				continue
			}
		}
	}

	return nil
}
