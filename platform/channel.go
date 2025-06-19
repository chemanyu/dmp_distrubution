package platform

import (
	"dmp_distribution/module"
	"errors"
	"log"
	"sync"
	"time"

	ginprom "dmp_distribution/common/ginporm"

	"github.com/redis/go-redis/v9"
)

type Platform interface {
	Distribution(rdb *redis.ClusterClient, task *module.Distribution, batches []map[string]string) error //接受回调事件AcceptTrack ocpx
}

type TaskServerRegistry struct {
	servers map[string]*serverPool
	mu      sync.RWMutex // 用于保护servers映射
}

type serverPool struct {
	name           string
	pool           chan Platform
	factory        func() Platform
	minSize        int
	maxSize        int
	idleTimeout    time.Duration
	lastActivity   time.Time
	mu             sync.RWMutex
	shrinkTimer    *time.Timer
	shrinkInterval time.Duration
}

func newServerPool(name string, initialSize, maxSize int, factory func() Platform, idleTimeout, shrinkInterval time.Duration) *serverPool {
	return &serverPool{
		name:           name,
		pool:           make(chan Platform, maxSize),
		factory:        factory,
		minSize:        initialSize,
		maxSize:        maxSize,
		idleTimeout:    idleTimeout,
		lastActivity:   time.Now(),
		shrinkInterval: shrinkInterval,
	}
}

func (sp *serverPool) adjustPool() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	// 计算自上次活动以来经过的时间
	idleTime := time.Since(sp.lastActivity)

	if idleTime >= sp.idleTimeout && len(sp.pool) > sp.minSize {
		// 计算需要回收的资源数量，可以根据实际情况调整回收策略
		//微批释放adjustPool
		// toRemove := (len(sp.pool) - sp.minSize) / 5
		toRemove := (len(sp.pool) - sp.minSize) / 5
		if toRemove < 5 {
			toRemove = len(sp.pool) - sp.minSize
		}
		for i := 0; i < toRemove; i++ {
			select {
			case <-sp.pool:
				log.Printf("adjustPool: Destroyed resource from '%s', new size: %d\n", sp.name, len(sp.pool))
				// 监控销毁次数
				ginprom.ServerPoolCounterVec.WithLabelValues("destroyed", "product", sp.name).Inc()
			default:
				// 如果资源池已经是最小大小或没有更多资源可以回收，则跳出循环
				break
			}
		}
	}

	// 重新设置或启动缩小计时器
	if sp.shrinkTimer == nil || !sp.shrinkTimer.Reset(sp.shrinkInterval) {
		sp.shrinkTimer = time.AfterFunc(sp.shrinkInterval, sp.adjustPool)
	}
}
func (r *TaskServerRegistry) Register(name string, initialSize, maxSize int, factory func() Platform, idleTimeout, shrinkInterval time.Duration) {
	pool := newServerPool(name, initialSize, maxSize, factory, idleTimeout, shrinkInterval)
	for i := 0; i < initialSize; i++ {
		//监控创建次数
		ginprom.ServerPoolCounterVec.WithLabelValues("created", "channel", name).Inc()
		pool.pool <- factory()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.servers[name] = pool
	pool.adjustPool() // 启动调整池大小逻辑
}

// Get 方法需要相应地更新以处理动态池逻辑
func (r *TaskServerRegistry) Get(name string) (Platform, error) {
	r.mu.RLock()
	pool, ok := r.servers[name]
	r.mu.RUnlock()
	if !ok {
		return nil, errors.New("Channel server with name '" + name + "' does not exist")
	}

	select {
	case tsi := <-pool.pool:
		return tsi, nil
	default:
		//监控创建次数
		ginprom.ServerPoolCounterVec.WithLabelValues("created", "channel", name).Inc()
		return pool.factory(), nil
	}
}

// Put 方法也需要更新来维护lastActivity时间并触发adjustPool
func (r *TaskServerRegistry) Put(name string, tsi Platform) error {
	r.mu.RLock()
	pool, ok := r.servers[name]
	r.mu.RUnlock()
	if !ok {
		return errors.New("Channel server with name '" + name + "' does not exist")
	}

	select {
	case pool.pool <- tsi:

		pool.lastActivity = time.Now()
		return nil
	default:
		return errors.New("Channel server with name '" + name + "' is full")
	}
}

func NewTaskServerRegistry() *TaskServerRegistry {
	return &TaskServerRegistry{
		servers: make(map[string]*serverPool),
	}
}

var Servers = NewTaskServerRegistry()

func init() {
	// 你的初始化逻辑
	Servers.Register("adn", 10, 1000, func() Platform { return &Adn{} }, 5*time.Second, 3*time.Second)
	Servers.Register("ssp", 10, 1000, func() Platform { return &Ssp{} }, 5*time.Second, 3*time.Second)
}
