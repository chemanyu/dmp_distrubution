package platform

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"dmp_distribution/common/redis"
	"dmp_distribution/module"

	redis_cluster "github.com/redis/go-redis/v9"
)

// Adn ADN平台实现
type Adn struct{}

// StrMd5 计算字符串的MD5值
func StrMd5(str string) string {
	hash := md5.New()
	hash.Write([]byte(str))
	return hex.EncodeToString(hash.Sum(nil))
}

// Distribution 实现批量数据分发到ADN平台
func (a *Adn) Distribution(rdb *redis_cluster.ClusterClient, task *module.Distribution, batches []map[string]string) error {
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
			deviceMD5 := StrMd5(deviceID)
			keyName := fmt.Sprintf("crowd_%s", deviceMD5)

			// 设置字段值（crowd_id -> expiration_timestamp）
			fieldsAndValues := map[string]interface{}{
				crowdID: now.Unix() + int64(remainingHours)*3600, // 当前时间戳 + 剩余小时数 * 3600秒
			}

			// 使用CRC32_RedisPool推送数据
			err := redis.C32_Redis_Pools.MSetHash_KeyData_Redis(keyName, fieldsAndValues)
			if err != nil {
				log.Printf("Failed to set hash data in Redis for device %s: %v", deviceID, err)
				continue
			}

			// 设置过期时间
			err = redis.C32_Redis_Pools.Expire(keyName, expirationSeconds)
			if err != nil {
				log.Printf("Failed to set expiration for device %s: %v", deviceID, err)
				continue
			}
		}
	}

	return nil
}

func PushToRedisNew(device string, crowdId string) {

	hour := time.Now().Hour()
	ehour := 24 - hour
	eday := 1
	oaidmd5 := StrMd5(device)
	keyName := "crowd_" + oaidmd5

	exdate := time.Now().AddDate(0, 0, +1).Format("2006-01-02")
	layout := "2006-01-02"
	specifiedDate, err := time.Parse(layout, exdate)
	if err != nil {
		log.Println("日期解析错误:", err)
		return
	}

	expirationTime := specifiedDate.AddDate(0, 0, eday)
	expirationInMinutes := int(expirationTime.Sub(specifiedDate).Seconds())
	fieldsAndValues := map[string]interface{}{
		crowdId: time.Now().Unix() + 3600*int64(ehour),
	}

	// Assuming crc32_redis is a global redis pool instance
	resultText, err = crc32_redis.C32_Redis_Pools.MSetHash_KeyData_Redis(keyName, fieldsAndValues)
	if err != nil {
		log.Println("Failed to set hash data in Redis:", err)
	}
	log.Println(resultText)
	err = crc32_redis.C32_Redis_Pools.Expire(keyName, expirationInMinutes)
	if err != nil {
		log.Println("Failed to set expiration in Redis:", err)
	}
	return
}
