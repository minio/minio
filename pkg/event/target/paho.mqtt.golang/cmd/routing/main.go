/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

/*----------------------------------------------------------------------
This sample is designed to demonstrate the ability to set individual
callbacks on a per-subscription basis. There are three handlers in use:
 brokerLoadHandler -        $SYS/broker/load/#
 brokerConnectionHandler -  $SYS/broker/connection/#
 brokerClientHandler -      $SYS/broker/clients/#
The client will receive 100 messages total from those subscriptions,
and then print the total number of messages received from each.
It may take a few moments for the sample to complete running, as it
must wait for messages to be published.
-----------------------------------------------------------------------*/

package main

import (
	"fmt"
	"os"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var brokerLoad = make(chan bool)
var brokerConnection = make(chan bool)
var brokerClients = make(chan bool)

func brokerLoadHandler(client MQTT.Client, msg MQTT.Message) {
	brokerLoad <- true
	fmt.Printf("BrokerLoadHandler         ")
	fmt.Printf("[%s]  ", msg.Topic())
	fmt.Printf("%s\n", msg.Payload())
}

func brokerConnectionHandler(client MQTT.Client, msg MQTT.Message) {
	brokerConnection <- true
	fmt.Printf("BrokerConnectionHandler   ")
	fmt.Printf("[%s]  ", msg.Topic())
	fmt.Printf("%s\n", msg.Payload())
}

func brokerClientsHandler(client MQTT.Client, msg MQTT.Message) {
	brokerClients <- true
	fmt.Printf("BrokerClientsHandler      ")
	fmt.Printf("[%s]  ", msg.Topic())
	fmt.Printf("%s\n", msg.Payload())
}

func main() {
	opts := MQTT.NewClientOptions().AddBroker("tcp://iot.eclipse.org:1883").SetClientID("router-sample")
	opts.SetCleanSession(true)

	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := c.Subscribe("$SYS/broker/load/#", 0, brokerLoadHandler); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	if token := c.Subscribe("$SYS/broker/connection/#", 0, brokerConnectionHandler); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	if token := c.Subscribe("$SYS/broker/clients/#", 0, brokerClientsHandler); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	loadCount := 0
	connectionCount := 0
	clientsCount := 0

	for i := 0; i < 100; i++ {
		select {
		case <-brokerLoad:
			loadCount++
		case <-brokerConnection:
			connectionCount++
		case <-brokerClients:
			clientsCount++
		}
	}

	fmt.Printf("Received %3d Broker Load messages\n", loadCount)
	fmt.Printf("Received %3d Broker Connection messages\n", connectionCount)
	fmt.Printf("Received %3d Broker Clients messages\n", clientsCount)

	c.Disconnect(250)
}
