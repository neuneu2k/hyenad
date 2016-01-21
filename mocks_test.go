/*
Copyright 2016 Assoba S.A.S.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package hyenad

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"sync"
)

type logConnection string

func (l *logConnection) Init(recv chan<- *Frame) {
}

func (l *logConnection) Queue() int {
	return 0
}

func (l *logConnection) Send(frame *Frame) error {
	log.WithField("Name", *l).WithField("Frame", frame.String()).Debug("RECV")
	return nil
}

func (l *logConnection) Ok() bool {
	return true
}

func (l *logConnection) Close() error {
	return nil
}

type logConnectionFactory struct {
	connections map[Address]Connection
	lock        sync.Mutex
}

func newLogConnectionFactory() ConnectionFactory {
	res := logConnectionFactory{}
	res.connections = make(map[Address]Connection)
	return &res
}

func (f *logConnectionFactory) SetRouter(router Router) {}
func (f *logConnectionFactory) Get(address Address, recv chan<- *Frame) (Connection, error) {
	if address == INVALID_ADDRESS {
		return nil, errors.New("Invalid Address")
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	c, ok := f.connections[address]
	if !ok {
		lc := logConnection(fmt.Sprintf("Connection{%v,%v}", address.Node, address.Process))
		c = &lc
		f.connections[address] = c
	}
	return c, nil
}

type singleTargetRouting struct{}

func (r *singleTargetRouting) Route(destination string) Addresses {
	return Addresses{Address{0, 1}}
}
