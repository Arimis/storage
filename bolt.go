// Copyright © 2016 Casa Platform
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/casaplatform/casa"
	"github.com/casaplatform/casa/cmd/casa/environment"
	"github.com/casaplatform/mqtt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var timeFormat = "2006-01-02T15:04:05.000000000Z07:00" //RFC3339Nano with fixed nano

type DB struct {
	*bolt.DB
	casa.Logger
	casa.MessageClient
	topics []string
}

func init() {
	environment.RegisterService("storage", &DB{})
}

func (db *DB) UseLogger(logger casa.Logger) {
	db.Logger = logger
}
func (db *DB) Start(config *viper.Viper) error {
	newDB, err := bolt.Open("my.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	db.DB = newDB

	client, err := mqtt.NewClient("tcp://127.0.0.1:1883")

	if err != nil {
		return err
	}

	db.MessageClient = client

	db.Handle(db.handler)

	// Subscribe to specified topics
	db.topics = config.GetStringSlice("Topics")
	for _, topic := range db.topics {
		err := client.Subscribe(topic)
		if err != nil {
			return errors.Wrap(err, "subscribing to '"+topic+"' failed")

		}
		db.Log("Bolt store subscribed to topic: " + topic)
	}

	return nil
}

// Handle aabode messages
func (db *DB) handler(msg *casa.Message, err error) {
	switch {
	case err != nil:
		db.Log(err)
	case msg != nil:
		c := make(chan error, 1)
		go func(ch chan error) {
			ch <- db.store(msg)
		}(c)
		select {
		case err := <-c:
			if err != nil {
				db.Log(err)
			}
		case <-time.After(1 * time.Second):
			db.Log("Timeout while storing message")
		}

	default:
		db.Log("Handler called with nil message and error")
	}

}

func (db *DB) Stop() error {
	if db.DB != nil {
		err := db.DB.Close()
		if err != nil {
			return err
		}
	}

	if db.MessageClient == nil {
		return nil
	}

	return db.MessageClient.Close()
}

func (db *DB) store(msg *casa.Message) error {
	return db.Update(func(tx *bolt.Tx) error {
		messages, err := tx.CreateBucketIfNotExists([]byte(msg.Topic))
		if err != nil {
			return err
		}

		err = messages.Put([]byte(time.Now().Format(timeFormat)), msg.Payload)
		if err != nil {
			return err
		}
		return nil
	})

}

func (db *DB) retrieve(topic string) (*casa.Message, error) {
	var payload []byte
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(topic))
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", topic)
		}

		_, payload = bucket.Cursor().Last()

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &casa.Message{
		Topic:   topic,
		Payload: payload,
	}, nil
}
