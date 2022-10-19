package main

import (
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

var (
	p rocketmq.Producer
	c rocketmq.PushConsumer
)

func main() {
	// name service
	endPoint := []string{"124.222.8.21:9876"}
	// create producer
	p, _ = rocketmq.NewProducer(
		producer.WithNameServer(endPoint),
		producer.WithRetry(2),
		producer.WithGroupName("MyProducerGroup01"),
	)
	// create push consumer
	c, _ = rocketmq.NewPushConsumer(consumer.WithNameServer(endPoint),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName("MyConsumerGroup01"),
	)
	// 创建channel，添加一个chan 确保程序退出前 消费者不被关闭
	ch := make(chan int)
	go SubcribeMessage(ch)
	SendMessage("测试消息发送")
	ch <- 1
}
