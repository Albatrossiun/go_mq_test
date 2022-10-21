package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
	//"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// SendAsyncMessage 发送异步消息
func SendAsyncMessage(message string) {
	// 启动生产者
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}

	ctx := context.Background()
	// 发送消息
	for i := 0; i < 5; i++ {
		msg := message + time.Now().String()
		err = p.SendAsync(ctx, func(ctx context.Context, result *primitive.SendResult, err error) {
			if err != nil {
				fmt.Printf("send message error: %s\n", err.Error())
			} else {
				fmt.Printf("send message seccess: result=%s\n", result.String())
			}
		},
			&primitive.Message{
				Topic: "MyTopic01",
				Body:  []byte(msg),
			})

		time.Sleep(1 * time.Second)
	}
}

// SendBatchSyncMessage 发送延迟同步消息
func SendBatchSyncMessage(message string) {
	// 启动生产者
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}

	// 发送消息
	//var msgs []*primitive.Message
	msgs := make([]*primitive.Message, 0)
	for i := 0; i < 3; i++ {
		msg := &primitive.Message{
			Topic: "MyTopic01",
			Body:  []byte(message + " -> num:" + strconv.Itoa(i)),
		}
		msgs = append(msgs, msg)
	}
	res, err := p.SendSync(context.Background(), msgs...)
	if err != nil {
		fmt.Printf("batch send sync message error:%s\n", err)
	} else {
		fmt.Printf("batch send sync message success. result=%s\n", res.String())
	}
}

// SendDelaySyncMessage 发送延迟同步消息
func SendDelaySyncMessage(message string) {
	// 启动生产者
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}

	// 发送消息
	for i := 0; i < 2; i++ {
		msg := message + time.Now().String()
		delayMsg := &primitive.Message{
			Topic: "MyTopic01",
			Body:  []byte(msg),
		}
		delayMsg.WithDelayTimeLevel(3)
		result, err := p.SendSync(context.Background(), delayMsg)

		if err != nil {
			fmt.Printf("send message error: %s\n", err.Error())
		} else {
			fmt.Printf("send message seccess: result=%s\n", result.String())
		}
		time.Sleep(1 * time.Second)
	}
}

// SendSyncMessageToTheSpecifiedQueue 发送同步消息到指定队列
func SendSyncMessageToTheSpecifiedQueue(message string) {
	// 启动生产者
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}

	// 发送消息
	for i := 0; i < 10; i++ {
		msg := message + time.Now().String()
		que := &primitive.MessageQueue{
			Topic:      "MyTopic01",
			BrokerName: "VM-4-6-ubuntu",
			QueueId:    0,
		}
		result, err := p.SendSync(context.Background(), &primitive.Message{
			Topic: "MyTopic01",
			Body:  []byte(msg),
			Queue: que,
		})

		if err != nil {
			fmt.Printf("send message error: %s\n", err.Error())
		} else {
			fmt.Printf("send message seccess: result=%s\n", result.String())
		}
		time.Sleep(1 * time.Second)
	}
}

// SendSyncMessage 发送同步消息
func SendSyncMessage(message string) {
	// 启动生产者
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}

	// 发送消息
	for i := 0; i < 5; i++ {
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

// SubcribeMessageByPuSh 订阅消息
func SubcribeMessageByPuSh(ch chan int) {
	// 订阅topic
	err := pushC.Subscribe("MyTopic01", consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for i := range msgs {
			fmt.Printf("============================收到了消息============================ \n msgs[i].Message.Body:[%s] \n msgs[i].Message:[%s] \n", msgs[i].Message.Body, msgs[i].Message)
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Printf("subscribe message error: %s\n", err.Error())
	}

	// 启动consumer
	err = pushC.Start()
	if err != nil {
		fmt.Printf("consumer start error: %s\n", err.Error())
		os.Exit(-1)
	}
	// 在这里阻塞 不要关闭客户端
	<-ch

	err = pushC.Shutdown()
	if err != nil {
		fmt.Printf("shutdown Consumer error: %s\n", err.Error())
	}
}

// SubcribeMessageByPull 拉取消息
func SubcribeMessageByPull(ch chan int, topicName string) {
	ctx := context.Background()
	// 默认拉取消息条数为10条
	nums := 10
	// 订阅topic
	messageQueue := pullC.MessageQueues("MyTopic01")
	if len(messageQueue) == 0 {
		fmt.Printf("get message queues is empty\n")
		return
	}
	// 获取mq进行最新的偏移
	offset, err := pullC.Lookup(ctx, messageQueue[0], math.MaxInt64)
	if err != nil {
		fmt.Printf("pull mode look up offset err:%s \n", err)
		return
	}
	selector := consumer.MessageSelector{}
	err = pullC.Subscribe("MyTopic01", selector)
	if err != nil {
		fmt.Printf("subscribe message error: %s\n", err.Error())
	}
	// 启动consumer
	err = pullC.Start()
	if err != nil {
		fmt.Printf("consumer start error: %s\n", err.Error())
		os.Exit(-1)
	}
	// pull topic
	pullResult, err := pullC.PullFrom(ctx, messageQueue[0], offset, nums)
	if err != nil {
		fmt.Printf("subscribe message error: %s\n", err.Error())
	}
	if pullResult != nil {
		fmt.Printf("============================收到了消息============================ \n msgs[i].Message.Body:[%s]\n", pullResult.GetBody())
	}
	// 在这里阻塞 不要关闭客户端
	<-ch

	err = pushC.Shutdown()
	if err != nil {
		fmt.Printf("shutdown Consumer error: %s\n", err.Error())
	}
}
