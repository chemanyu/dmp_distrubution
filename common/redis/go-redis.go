package redis

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang/groupcache/lru"
	"github.com/redis/go-redis/v9" // Redis 集群库
)

var Mates *Mate

type Mate struct {
	lruData   *lru.Cache
	RedisPool *redis.ClusterClient // 使用 Redis 集群客户端
	pushLock  *sync.RWMutex
}

func init() {
	Mates = &Mate{}
}

func (r *Mate) InitRedis(redisAddrs string) {
	// 初始化 Redis 地址
	if redisAddrs == "" {
		log.Fatal("REDIS_POOL_DB must be set in the configuration")
	}

	Ip_ports := strings.Split(redisAddrs, ",")
	clusterOptions := &redis.ClusterOptions{
		Addrs:           Ip_ports,
		Password:        "",                     // 集群密码，没有则留空
		ReadTimeout:     100 * time.Millisecond, // 读超时,写超时默认等于读超时
		PoolSize:        512,                    // 每个节点的连接池容量
		MinIdleConns:    64,                     // 维持的最小空闲连接数
		PoolTimeout:     1 * time.Minute,        // 当所有连接都忙时的等待超时时间
		ConnMaxLifetime: 30 * time.Minute,       // 连接生存时间
		PoolFIFO:        true,
		//IdleTimeout:     5 * time.Minute, // 空闲连接在被关闭之前的保持时间
	}

	rdb := redis.NewClusterClient(clusterOptions)

	r.RedisPool = rdb
}

func (c *Mate) lruGet(key string) (ret []byte) {
	c.pushLock.RLock()
	tmpRet, ok := c.lruData.Get(key)
	c.pushLock.RUnlock()
	if ok {
		return tmpRet.([]byte)
	} else {
		return nil
	}
}

// hSet 缓存数据到 Redis
func (c *Mate) RedisHSet(key string, field string, value int64) error {
	ctx := context.Background() // 创建上下文
	err := c.RedisPool.HSet(ctx, key, field, value).Err()
	if err != nil && gin.DebugMode == "debug" {
		log.Println("RedisHSet:", err.Error())
		return err
	}
	return redis.Nil
}

func (c *Mate) RedisGet(key string) []byte {
	ctx := context.Background() // 创建上下文
	ret, err := c.RedisPool.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil { // key 不存在
			return nil
		}
		if gin.DebugMode == "debug" {
			log.Println("RedisGet:", err.Error())
		}
	}
	if ret != nil {
		// 加入本地 LRU 缓存
		c.pushLock.Lock()
		c.lruData.Add(key, ret)
		c.pushLock.Unlock()
	}
	return ret
}

func (c *Mate) SetCaches(key string, value []byte) {
	ctx := context.Background()                                      // 创建上下文
	err := c.RedisPool.Set(ctx, key, value, 86400*time.Second).Err() // 设置缓存并设置过期时间
	if err != nil && gin.DebugMode == "debug" {
		log.Println("SetCaches:", err.Error())
	}
}
