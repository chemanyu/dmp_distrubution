package service

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"

	"dmp_distribution/module"
	"dmp_distribution/platform"
)

const (
	TaskQueueKey   = "dmp:distribution:task:queue"
	TaskStatusKey  = "dmp:distribution:task:status:%d"
	RetryMaxTimes  = 3
	BatchSize      = 100000 // 单次处理的最大数据量
	TaskWaitStatus = 0
	TaskRunStatus  = 1
	TaskDoneStatus = 2
	TaskFailStatus = 3
)

type DistributionService struct {
	distModel *module.Distribution
	rdb       *redis.ClusterClient
	ctx       context.Context
	cancel    context.CancelFunc // 用于取消上下文
	taskChan  chan *module.Distribution
	wg        sync.WaitGroup
	isRunning bool // 用于标记服务是否在运行
}

func NewDistributionService(model *module.Distribution, rdb *redis.ClusterClient) *DistributionService {
	ctx, cancel := context.WithCancel(context.Background())
	srv := &DistributionService{
		distModel: model,
		rdb:       rdb,
		ctx:       ctx,
		cancel:    cancel,
		taskChan:  make(chan *module.Distribution, 100),
		isRunning: true,
	}
	// 启动任务处理器
	go srv.taskProcessor()
	return srv
}

// Stop 优雅地停止服务
func (s *DistributionService) Stop() {
	if !s.isRunning {
		return
	}
	s.isRunning = false

	// 取消上下文
	if s.cancel != nil {
		s.cancel()
	}

	// 关闭任务通道
	close(s.taskChan)

	// 等待所有任务完成
	s.wg.Wait()

	// 关闭Redis连接
	if s.rdb != nil {
		_ = s.rdb.Close()
	}
}

// StartTaskScheduler 启动任务调度器
func (s *DistributionService) StartTaskScheduler() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if !s.isRunning {
				return
			}
			tasks, _, err := s.distModel.List(map[string]interface{}{
				"status": TaskWaitStatus,
			}, 1, 100)
			if err != nil {
				continue
			}

			for _, task := range tasks {
				s.taskChan <- &task
			}
		}
	}
}

// taskProcessor 任务处理器
func (s *DistributionService) taskProcessor() {
	for task := range s.taskChan {
		s.wg.Add(1)
		go func(t *module.Distribution) {
			defer s.wg.Done()
			err := s.processTask(t)
			if err != nil {
				// 更新任务状态为失败
				log.Printf("task %d failed: %v", t.ID, err)
				s.distModel.UpdateStatus(t.ID, TaskFailStatus)
			}
		}(task)
	}
}

// processTask 处理单个任务
func (s *DistributionService) processTask(task *module.Distribution) error {
	// 更新任务状态为执行中
	if err := s.distModel.UpdateStatus(task.ID, TaskRunStatus); err != nil {
		return fmt.Errorf("update task status error: %v", err)
	}

	// 1. 读取源文件数据
	data, err := s.readSourceFile(task.Path, task)
	if err != nil {
		return fmt.Errorf("read source file error: %v", err)
	}

	// 2. 数据分片
	batches := s.splitDataIntoBatches(data, BatchSize)

	// TODO: 调用内部平台API
	platformServers, _ := platform.Servers.Get(task.Platform)
	defer func() {
		platform.Servers.Put(task.Platform, platformServers)
	}()

	// 处理平台服务器
	if platformServers == nil {
		return fmt.Errorf("no platform servers available for platform: %s", task.Platform)
	}

	err = platformServers.Distribution(task, batches)
	if err != nil {
		return fmt.Errorf("distribution to platform error: %v", err)
	}

	// 更新任务状态为完成
	return s.distModel.UpdateStatus(task.ID, TaskDoneStatus)
}

// readSourceFile 读取源文件并进行数据清洗
func (s *DistributionService) readSourceFile(path string, task *module.Distribution) ([]map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data []map[string]string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		deviceInfo := make(map[string]string)

		// 解析行数据（假设是CSV格式）
		fields := strings.Split(line, ",")
		if len(fields) == 0 {
			continue
		}

		// 根据启用的字段提取数据
		if task.Imei == 1 {
			if imei := s.cleanIMEI(fields[0]); imei != "" {
				deviceInfo["imei"] = imei
			}
		}
		if task.Oaid == 1 {
			if oaid := s.cleanOAID(fields[1]); oaid != "" {
				deviceInfo["oaid"] = oaid
			}
		}
		if task.Idfa == 1 {
			if idfa := s.cleanIDFA(fields[2]); idfa != "" {
				deviceInfo["idfa"] = idfa
			}
		}
		// ... 处理其他字段

		if len(deviceInfo) > 0 {
			data = append(data, deviceInfo)
		}
	}

	return data, scanner.Err()
}

// cleanIMEI 清洗IMEI数据
func (s *DistributionService) cleanIMEI(imei string) string {
	// 去除空格和特殊字符
	imei = strings.TrimSpace(imei)
	imei = strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, imei)

	// 验证IMEI长度
	if len(imei) != 15 {
		return ""
	}

	return imei
}

// cleanOAID 清洗OAID数据
func (s *DistributionService) cleanOAID(oaid string) string {
	return strings.TrimSpace(oaid)
}

// cleanIDFA 清洗IDFA数据
func (s *DistributionService) cleanIDFA(idfa string) string {
	idfa = strings.TrimSpace(strings.ToUpper(idfa))
	if len(idfa) != 36 { // UUID格式
		return ""
	}
	return idfa
}

// splitDataIntoBatches 将数据分片
func (s *DistributionService) splitDataIntoBatches(data []map[string]string, batchSize int) [][]map[string]string {
	var batches [][]map[string]string
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}
		batches = append(batches, data[i:end])
	}
	return batches
}
