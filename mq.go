package main

import (
	"context"
	"fmt"
	"os"
	"time"
	//"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func SendMessage(message string) {
	// 启动生产者
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}

	// 发送消息
	for i := 0; i < 10; i++ {
		msg := message + time.Now().String()
		result, err := p.SendSync(context.Background(), &primitive.Message{
			Topic: "MyTopic01",
			Body:  []byte(msg),
		})

		if err != nil {
			fmt.Printf("send message error: %s\n", err.Error())
		} else {
			fmt.Printf("send message seccess: result=%s\n", result.String())
		}
		time.Sleep(1 * time.Second)
	}
}

func SubcribeMessage(ch chan int) {
	// 订阅topic
	err := c.Subscribe("MyTopic01", consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for i := range msgs {
			fmt.Printf("============================收到了消息============================ : [%s] \n", msgs[i].Message.Body)
		}
		return consumer.ConsumeSuccess, nil
	})

	if err != nil {
		fmt.Printf("subscribe message error: %s\n", err.Error())
	}

	// 启动consumer
	err = c.Start()
	if err != nil {
		fmt.Printf("consumer start error: %s\n", err.Error())
		os.Exit(-1)
	}

	// 在这里阻塞 不要关闭客户端
	<-ch

	err = c.Shutdown()
	if err != nil {
		fmt.Printf("shutdown Consumer error: %s\n", err.Error())
	}
}
