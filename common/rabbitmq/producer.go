package rabbitmq

import (
	"fmt"
	"log"
	"strings"
	"time"

	pool "dmp_distribution/common/Pool"

	jsoniter "github.com/json-iterator/go"
	"github.com/streadway/amqp"
)

type SendRabbitMQ struct {
	addr      string
	queuename string
	conn      *amqp.Connection
	ch        *amqp.Channel
	q         amqp.Queue
	//mq_lock *sync.Mutex
	linkstatus int8
	mqPool     *pool.RabbitmqPool
}

func NewSendRabbitMQ(url string, queuename string) *SendRabbitMQ {
	mysend := new(SendRabbitMQ)
	//mysend.mq_lock = new(sync.Mutex)
	mysend.addr = url
	mysend.queuename = queuename
	mysend.rabbitmq()
	//mysend.connect()
	return mysend
}

func (sr *SendRabbitMQ) rabbitmq() {
	//factory 创建连接的方法
	var err error
	poolConfig := &pool.PoolConfig{
		MaxOpen:   10,
		MinOpen:   5,
		Setintval: 15 * time.Second,
	}

	sr.mqPool, err = pool.NewPool(poolConfig, func() (*pool.ObjMq, error) {
		c, err := amqp.Dial(sr.addr)
		if err != nil {
			return nil, err
		}
		ch, _ := c.Channel()
		if err != nil {
			return nil, err
		}
		qu, _ := ch.QueueDeclare(
			sr.queuename, // name
			true,         // durable
			false,        // delete when unused
			false,        // exclusive
			false,        // no-wait
			nil,          // arguments
		)
		if err != nil {
			return nil, err
		}
		return &pool.ObjMq{Cn: c, Ch: ch, Qu: qu}, nil
	})

	if err != nil {
		fmt.Print(err)
	}
	//从连接池中取得一个连接
	//v, err := p.Get()
	//do something
	//conn :=v.(*amqp.Connection)
	//将连接放回连接池中
	//p.Put(v)
	//释放连接池中的所有连接
	//p.Release()
	//查看当前连接中的数量

}

func (sr *SendRabbitMQ) connect() bool {
	if sr.linkstatus == 1 {
		return true
	}
	// sr.mq_lock.Lock()
	// defer sr.mq_lock.Unlock()
	var err error

	sr.conn, err = amqp.Dial(sr.addr)
	fmt.Println(err, "Failed to connect to RabbitMQ")

	//v, err := p.Get()
	//do something
	//conn :=v.(*amqp.Connection)
	//将连接放回连接池中
	//p.Put(v)
	//释放连接池中的所有连接
	//p.Release()
	sr.ch, err = sr.conn.Channel()
	fmt.Println(err, "Failed to open a channel")

	sr.q, err = sr.ch.QueueDeclare(
		sr.queuename, // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)

	if err != nil {
		sr.linkstatus = 1
		return true

	} else {
		fmt.Println(err, "Failed to declare a queue")

		return false
	}

}

// 发送string
func (sr *SendRabbitMQ) SendData(Data string) {
	var err error
	err = sr.ch.Publish(
		"",        // exchange
		sr.q.Name, // routing key
		false,     // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(Data),
		})
	fmt.Println(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", Data)
}

// 发送map
func (sr *SendRabbitMQ) SendDataMap(Data map[string]interface{}) {
	var err error

	conn, err := sr.mqPool.Get()
	if err != nil {
		log.Println("sr.mqPool.Get", err)
	}
	//sr.mqPool.Release(conn)

	str, _ := jsoniter.Marshal(Data)
	err = conn.Ch.Publish(
		"",           // exchange
		conn.Qu.Name, // routing key
		false,        // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         str,
		})

	//sr.mqPool.Release(conn)
	if err != nil {
		log.Printf(" [x] ERRORERRORERRORERRORERRORERRORERRORERROR %s", str)
		if strings.Compare("channel/connection is not open", err.Error()) != -1 {
			sr.mqPool.Close(conn)
			//重连成功 恢复日志
			//sr.SendDataMap(Data)
		}
	} else {
		//log.Printf(" [x] Sent %s", str)
	}
}

func (sr *SendRabbitMQ) Close() {

}
