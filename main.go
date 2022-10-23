package main

import (
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

var (
	p     rocketmq.Producer
	pushC rocketmq.PushConsumer
	pullC rocketmq.PullConsumer
)

// type Logger interface {
//     Debug(msg string, fields map[string]interface{})
//     Info(msg string, fields map[string]interface{})
//     Warning(msg string, fields map[string]interface{})
//     Error(msg string, fields map[string]interface{})
//     Fatal(msg string, fields map[string]interface{})
//     Level(level string)
//     OutputPath(path string) (err error)
// }

type MyLogger struct {
}

func (l *MyLogger) Debug(string, map[string]interface{})      {}
func (l *MyLogger) Info(msg string, m map[string]interface{}) {}
func (l *MyLogger) Warning(string, map[string]interface{})    {}
func (l *MyLogger) Error(msg string, m map[string]interface{}) {
	fmt.Printf("[ERROR][%v][%v]\n", msg, m)
}
func (l *MyLogger) Fatal(string, map[string]interface{}) {}
func (l *MyLogger) Level(string)                         {}
func (l *MyLogger) OutputPath(string) error              { return nil }

func main() {
	// name service
	endPoint := []string{"124.222.8.21:9876"}

	// logger := logrus.New()
	fmt.Println("程序启动")
	logger := &MyLogger{}
	rlog.SetLogger(logger)

	// create producer
	p, _ = rocketmq.NewProducer(
		producer.WithNameServer(endPoint),
		producer.WithRetry(2),
		producer.WithGroupName("MyProducerGroup01"),
		// producer.WithQueueSelector(producer.NewManualQueueSelector()),
	)
	// create push consumer
	pushC, _ = rocketmq.NewPushConsumer(consumer.WithNameServer(endPoint),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName("MyConsumerGroup01"),
	)
	// create pull consumer
	var err error
	// consumer.NewPullConsumer(...)
	pullC, err = rocketmq.NewPullConsumer(consumer.WithNameServer(endPoint),
		// pullC, err = consumer.NewPullConsumer(
		consumer.WithNameServer(endPoint),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName("MyConsumerGroup01"),
	)
	if err != nil {
		fmt.Printf("[ERROR][%v]\n", err)
	}
	// 创建channel，添加一个chan 确保程序退出前 消费者不被关闭
	ch := make(chan int)
	// go SubcribeMessageByPull(ch, "MyTopic01")
	go SubcribeMessageByPuSh(ch)
	//SendSyncMessageToTheSpecifiedQueue("测试消息发送")
	time.Sleep(100000 * time.Second)
	ch <- 1
}
