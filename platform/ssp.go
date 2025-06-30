package platform

import (
	"context"
	"fmt"
	"log"
	"time"

	"dmp_distribution/common/redis"
	"dmp_distribution/module"
)

// Ssp SSP平台实现
type Ssp struct{}

// Distribution 实现批量数据分发到SSP平台
// 数据格式：hset h:user:{USER_ID} TG:{crowd_id} timestamp
func (s *Ssp) Distribution(task *module.Distribution, batches []map[string]string) error {
	ctx := context.Background()
	pipe := redis.Mates.RedisPool.Pipeline()
	now := time.Now().UnixMilli() // 获取13位毫秒级时间戳
	processedCount := 0

	// 批量处理所有设备
	for _, device := range batches {
		for fieldName, userID := range device {
			// 跳过空值和非用户ID字段
			if userID == "" || fieldName != "user_id" {
				continue
			}

			// 构造Redis命令
			// 格式：hset h:user:{USER_ID} TG:{crowd_id} timestamp
			key := fmt.Sprintf("h:user:%s", userID)
			field := fmt.Sprintf("TG:%s", task.Crowd) // task.Crowd 是字符串类型

			//log.Printf("Setting data - key: %s, field: %s, value: %d", key, field, now)
			// err := redis.Mates.RedisHSet(key, field, now)
			// if err == nil {
			// 	log.Printf("Failed to err %v", err)
			// 	break
			// }
			//使用pipeline添加命令
			pipe.HSet(ctx, key, field, now)
			processedCount++

			// 每1000条数据执行一次pipeline，避免单个pipeline太大
			if processedCount%1000 == 0 {
				if _, err := pipe.Exec(ctx); err != nil {
					log.Printf("Failed to execute pipeline at count %d: %v", processedCount, err)
					return fmt.Errorf("failed to push data to redis at count %d: %v", processedCount, err)
				}
				pipe = redis.Mates.RedisPool.Pipeline() // 创建新的pipeline
			}
		}
	}

	// 处理剩余的数据
	if processedCount%1000 != 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("Failed to execute final pipeline: %v", err)
			return fmt.Errorf("failed to push remaining data to redis: %v", err)
		}
	}

	log.Printf("Successfully processed %d users for crowd %s", processedCount, task.Crowd)
	return nil
}
