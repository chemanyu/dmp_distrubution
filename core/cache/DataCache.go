package rdcache

import (
	"context"
	"database/sql"
	"dmp_distribution/core"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/gin-gonic/gin"
	"github.com/golang/groupcache/lru"
	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9" // Redis 集群库
)

var sg singleflight.Group
var ctx = context.TODO()

type Mate struct {
	lruData   *lru.Cache
	redisPool *redis.ClusterClient // 使用 Redis 集群客户端
	mysqlPool *sql.DB
	pushLock  *sync.RWMutex
	hash      core.Hasher
}

type CallbackConf struct {
	Upstream   string `json:"upstream"`
	Downstream string `json:"downstream"`
	Rate       int    `json:"rate"`
	IsMaster   bool   `json:"is_master"`
}

func NewData(redisAddrs []string, msPool *sql.DB) *Mate {
	clusterOptions := &redis.ClusterOptions{
		Addrs:           redisAddrs,
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

	// 使用Ping方法来检查连接是否成功
	// if err := rdb.Ping(ctx).Err(); err != nil {
	// 	fmt.Printf("Error connecting to Go-Redis cluster: %v\n", err)
	// 	return nil
	// }
	// fmt.Println("Connected to Go-Redis cluster successfully")

	return &Mate{
		lruData:   lru.New(100000), // 使用 LRU 缓存
		redisPool: rdb,             // 替换为 Redis 集群客户端
		mysqlPool: msPool,
		hash:      core.NewDefaultHasher64(),
		pushLock:  new(sync.RWMutex),
	}
}

type mateData map[string]interface{}

// 查找获取缓存
func (c *Mate) Get(key string) map[string]interface{} {
	key = "ocpx-unikey:" + key

	// result, ok := c.lruData.Get(key)
	// if !ok {
	// 	result = c.redisGet(key)
	// }

	//没用sg测试
	// var result interface{}
	// result = c.redisGet(key)
	// ret := mateData{}
	// if result != nil {

	// 	err := jsoniter.Unmarshal(result.([]byte), &ret)
	// 	if err != nil {
	// 		if gin.DebugMode == "debug" {
	// 			log.Println(err.Error(), string(result.([]byte)))
	// 		}
	// 	}
	// } else {
	// 	return nil
	// }
	// return ret
	v, _, _ := sg.Do(key, func() (interface{}, error) {
		var result interface{}
		result = c.redisGet(key)
		ret := mateData{}
		if result != nil {

			err := jsoniter.Unmarshal(result.([]byte), &ret)
			if err != nil {
				if gin.DebugMode == "debug" {
					//log.Println(err.Error(), string(result.([]byte)))
				}
			}
		} else {
			return nil, nil
		}
		//拦截并发
		//fmt.Println(1)
		return ret, nil
	})
	//复用
	//fmt.Println(2)
	if v != nil {
		ret, _ := v.(mateData)

		return ret
	}
	return nil
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

func (c *Mate) redisGet(key string) []byte {
	ctx := context.Background() // 创建上下文
	ret, err := c.redisPool.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil { // key 不存在
			return nil
		}
		if gin.DebugMode == "debug" {
			log.Println("redisGet:", err.Error())
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

func (c *Mate) setCaches(key string, value []byte) {
	ctx := context.Background()                                      // 创建上下文
	err := c.redisPool.Set(ctx, key, value, 86400*time.Second).Err() // 设置缓存并设置过期时间
	if err != nil && gin.DebugMode == "debug" {
		log.Println("setCaches:", err.Error())
	}
}

func (c *Mate) PushRedisAll() error {
	//log.Println("Starting PushRedisAll...")
	rows, err := c.getAll("select main.*,ad.advertiser_id,ad.agent_id,ad.support_id,p.channel as product_channel,p.event_key,p.form as product_form,d.form as channel_form from ad_tool_monitor as main left join ad_tool_product as p on main.product_id = p.id left join ad_tool_channel as d on main.channel_id = d.id left join ad_tool_advertiser as ad on ad.advertiser_id = main.advertiser_id where main.del_flag=0")

	if err != nil {
		log.Println("Error fetching data:", err)
		return err
	}

	for _, v := range rows {
		//log.Printf("Processing row %d: %v", i, "")
		unikey, ok := v["unikey"].(string)
		if !ok {
			log.Println("Missing or invalid unikey, skipping")
			continue
		}

		keyName := "ocpx-unikey:" + unikey
		retString, err := jsoniter.Marshal(v)
		if err != nil {
			log.Println("JSON marshal error for key:", keyName, "error:", err)
			continue
		}
		//log.Println("JSON prepared for key:", keyName)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = c.redisPool.Set(ctx, keyName, retString, 24*time.Hour).Err()
		if err != nil {
			log.Println("Redis Set error for key:", keyName, "error:", err)
		}
	}
	//log.Println("Finished PushRedisAll.")
	return nil
}

func (c *Mate) scanRow(rows *sql.Rows) (a mateData, er error) {
	columns, _ := rows.Columns()

	vals := make([]interface{}, len(columns))
	valsPtr := make([]interface{}, len(columns))

	for i := range vals {
		valsPtr[i] = &vals[i]
	}

	err := rows.Scan(valsPtr...)

	if err != nil {
		if gin.DebugMode == "debug" {
			log.Println("scan err", err.Error())
		}
		return nil, err
	}

	r := make(mateData)

	for i, v := range columns {
		if va, ok := vals[i].([]byte); ok {
			r[v] = string(va)
		} else {
			r[v] = vals[i]
		}
	}

	return r, nil

}

// 获取一行记录
func (c *Mate) getOne(sql string, args ...interface{}) (mateData, error) {
	rows, err := c.mysqlPool.Query(sql, args...)
	if err != nil {
		if gin.DebugMode == "debug" {
			log.Println(11111111, err.Error())
		}
		return nil, err
	}
	rows.Next()
	defer rows.Close()

	result, err := c.scanRow(rows)
	return result, err
}

// 获取多行记录
func (c *Mate) getAll(sql string, args ...interface{}) ([]mateData, error) {
	rows, err := c.mysqlPool.Query(sql, args...)
	if err != nil {
		if gin.DebugMode == "debug" {
			log.Println(11111, sql, err.Error())
		}
		return nil, err
	}

	defer rows.Close()

	result := make([]mateData, 0)

	for rows.Next() {
		r, err := c.scanRow(rows)
		if err != nil {
			if gin.DebugMode == "debug" {
				log.Println(11111, err.Error())
			}
			continue
		}

		result = append(result, r)
	}

	return result, nil

}
