// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	config := MqttConfig{
		Endpoint: "mqtt://127.0.0.1:1883",
		QoS:      1,
	}

	client, err := NewMqttClient(&config)
	if err != nil {
		log.Fatal("NewMqttClient error:", err)
	}

	time.Sleep(5 * time.Second)

	err = client.Subscribe(context.Background(), "testtopic/test")
	if err != nil {
		log.Fatal("Subscribe error:", err)
	}

	<-done

	log.Println("Caught signal, stopping...")

	//<-client.Client.Done() // Wait for clean shutdown (cancelling the context triggered the shutdown)
}
