package main

import (
	"bytes"
	"encoding/base64"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/nsqio/go-nsq"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type FirehoseHandler struct {
	FirehoseClient *firehose.Firehose
}

func (handler *FirehoseHandler) HandleMessage(m *nsq.Message) error {
	b64BodyBuf := &bytes.Buffer{}
	encoder := base64.NewEncoder(base64.StdEncoding, b64BodyBuf)
	encoder.Write(m.Body)
	encoder.Close()
	params := &firehose.PutRecordInput{
		DeliveryStreamName: aws.String("DFDeliveryService"), // Required
		Record: &firehose.Record{
			Data: b64BodyBuf.Bytes(),
		},
	}
	_, err := handler.FirehoseClient.PutRecord(params)
	return err
}

func main() {
	topic := "test"
	channel := "firehose-test"
	cfg := nsq.NewConfig()

	consumer, err := nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		log.Fatal(err)
	}
	err = consumer.ConnectToNSQD("127.0.0.1:4150")
	handler := &FirehoseHandler{}
	consumer.AddHandler(handler)

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-termChan:
			consumer.Stop()
		}
	}
}
