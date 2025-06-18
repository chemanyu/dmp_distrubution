package redis

import (
	"errors"

	//"fmt"
	"hash/crc32"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

var C32_Redis_Pools *Crc32_RedisPool

type Crc32_RedisPool struct {
	Ms_pool     []*redis.Pool
	Ip_ports    []string
	Ms_pool_num int
}

func init() {
	C32_Redis_Pools = &Crc32_RedisPool{}
}

func (r *Crc32_RedisPool) Init_RedisPool(ip_port_str string) {
	r.Ip_ports = strings.Split(ip_port_str, ",")
	readTimeout := redis.DialReadTimeout(time.Duration(1000) * time.Millisecond)
	writeTimeout := redis.DialWriteTimeout(time.Duration(1000) * time.Millisecond)
	connectTimeout := redis.DialConnectTimeout(time.Duration(1000) * time.Millisecond)

	r.Ms_pool = make([]*redis.Pool, 0, 30)
	for i := 0; i < len(r.Ip_ports); i++ {
		ip_info_string := strings.Split(r.Ip_ports[i], "@") //拆分出密码
		redis_pool := &redis.Pool{
			MaxIdle:     256,
			MaxActive:   1024,
			IdleTimeout: 1 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", ip_info_string[0], readTimeout, writeTimeout, connectTimeout)
				if err != nil {
					return nil, err
				}
				if len(ip_info_string) == 2 { //如果有密码
					if _, err := c.Do("AUTH", ip_info_string[1]); err != nil {
						c.Close()
						return nil, err
					}
				}
				return c, err
			},
		}
		r.Ms_pool = append(r.Ms_pool, redis_pool)
		//r.Ms_hash_consistent.Add(ms_hashlib.NewNode(i, ip_port[i], 1))
	}
	r.Ms_pool_num = len(r.Ip_ports)
}

func (r *Crc32_RedisPool) Set_KeyData_Redis(key, data string) error {
	k := r.getkey(key)
	if r.Ms_pool[k] != nil {
		c := r.Ms_pool[k].Get()
		defer c.Close()
		_, err := c.Do("SET", key, data)
		return err
	}
	return errors.New("not new redis pool!")
}

/*
将键key的值设置为value ，并将键key的生存时间设置为seconds秒钟。
如果键key已经存在， 那么SETEX命令将覆盖已有的值。
*/
func (r *Crc32_RedisPool) Setex(key, data string, seconds int) error {
	k := r.getkey(key)

	if r.Ms_pool[k] != nil {
		c := r.Ms_pool[k].Get()
		defer c.Close()
		_, err := c.Do("SET", key, data, "EX", seconds)
		return err
	}
	return errors.New("not new redis pool!")

}

// 设置过期时间点(参数是UNIX时间戳)
func (r *Crc32_RedisPool) Expireat(key string, timestamp int64) error {
	k := r.getkey(key)
	if r.Ms_pool[k] != nil {
		c := r.Ms_pool[k].Get()
		defer c.Close()
		_, err := c.Do("EXPIREAT", key, timestamp)
		return err
	}
	return errors.New("not new redis pool!")
}

// 为给定key设置生存时间(秒数)
func (r *Crc32_RedisPool) Expire(key string, seconds int) error {
	k := r.getkey(key)
	if r.Ms_pool[k] != nil {
		c := r.Ms_pool[k].Get()
		defer c.Close()
		_, err := c.Do("EXPIRE", key, seconds)
		return err
	}
	return errors.New("not new redis pool!")
}

func (r *Crc32_RedisPool) Get_KeyData_Redis(key string) (string, error) {
	k := r.getkey(key)
	if r.Ms_pool[k] != nil {
		c := r.Ms_pool[k].Get()
		defer c.Close()
		res, err := redis.String(c.Do("GET", key))
		if err == nil {
			return res, err
		}
		if err.Error() == "redigo: nil returned" {
			err = nil
		}
		return "", err
	}
	return "", errors.New("not new redis pool!")
}

// 为键key储存的数字值加上增量
func (r *Crc32_RedisPool) Incr(key string) (int64, error) {
	k := r.getkey(key)
	if r.Ms_pool[k] != nil {
		c := r.Ms_pool[k].Get()
		defer c.Close()
		res, err := redis.Int64(c.Do("INCR", key))
		if err == nil {
			return res, err
		}
	}

	return -1, errors.New("not new redis pool!")
}

// 为键key储存的数字值减去增量
func (r *Crc32_RedisPool) Decr(key string) (int64, error) {
	k := r.getkey(key)
	if r.Ms_pool[k] != nil {
		c := r.Ms_pool[k].Get()
		defer c.Close()
		res, err := redis.Int64(c.Do("DECR", key))
		if err == nil {
			return res, err
		}
	}

	return -1, errors.New("not new redis pool!")
}

func (r *Crc32_RedisPool) Del_KeyData_Redis(key string) error {
	k := r.getkey(key)
	if r.Ms_pool[k] != nil {
		c := r.Ms_pool[k].Get()
		defer c.Close()
		_, err := c.Do("DEL", key)
		return err
	}
	return errors.New("not new redis pool!")
}

func (r *Crc32_RedisPool) Ttl_KeyData_Redis(key string) (int, error) {
	k := r.getkey(key)
	if r.Ms_pool[k] != nil {
		c := r.Ms_pool[k].Get()
		defer c.Close()
		res, err := redis.Int(c.Do("TTL", key))
		return res, err
	}
	return 0, errors.New("not new redis pool!")
}

func (r *Crc32_RedisPool) MSetHash_KeyData_Redis(key string, setvs map[string]interface{}) error {
	k := r.getkey(key)
	//fmt.Println("k:", k, "len:", len(r.Ms_pool), key)
	if r.Ms_pool[k] != nil {
		c := r.Ms_pool[k].Get()
		defer c.Close()
		//构造命令参数

		args := redis.Args{}.Add(key).AddFlat(setvs)
		_, err := redis.String(c.Do("HMSET", args...))
		return err
	}
	return errors.New("not new redis pool!")
}

// 获取hash指定字段
func (r *Crc32_RedisPool) GetHash_KeyData_Redis(key, field string) (string, error) {
	k := r.getkey(key)

	if r.Ms_pool[k] != nil {
		c := r.Ms_pool[k].Get()
		defer c.Close()
		res, err := redis.String(c.Do("HGET", key, field))
		if err == nil {
			return res, err
		}
		if err.Error() == "redigo: nil returned" {
			err = nil
		}
		return "", err
	}
	return "", errors.New("not new redis pool!")
}

// 获取流量池的key值
func (r *Crc32_RedisPool) getkey(key string) int {
	c32 := crc32.ChecksumIEEE([]byte(key))
	return int(c32 % uint32(len(r.Ip_ports)))
}
