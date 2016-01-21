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
	"encoding/binary"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type LocalConnection struct {
	conn   net.Conn
	closed uint32
	recv   chan<- *Frame
	send   chan *Frame
}

func newLocalConnection(pid uint32, conn net.Conn, recv chan<- *Frame) *LocalConnection {
	res := LocalConnection{}
	res.conn = conn
	res.send = make(chan *Frame)
	res.recv = recv
	go res.write()
	go res.read()
	return &res
}

func (l *LocalConnection) write() {
	for f := range l.send {
		buf := f.Buffer()
		size := byte(len(buf))
		n, err := l.conn.Write([]byte{size})
		if err != nil || n != 1 {
			log.WithError(err).WithField("Wrote", n).Error("Writing frame size")
			frameBuffers.Return(buf)
			break
		}
		n, err = l.conn.Write(buf)
		if err != nil || n != len(buf) {
			log.WithError(err).WithField("Wrote", n).WithField("Expected", len(buf)).Error("Writing frame ")
			frameBuffers.Return(buf)
			break
		}
		frameBuffers.Return(buf)
	}
}

func (l *LocalConnection) read() {
	size := [1]byte{0}
	for {
		n, err := l.conn.Read(size[:])
		if err != nil || n != 1 {
			//log.WithError(err).WithField("Read", n).Error("Reading frame size")
			break
		}
		buf := frameBuffers.Get()
		buf = buf[0:size[0]]
		n, err = io.ReadFull(l.conn, buf)
		if err != nil || n != len(buf) {
			//log.WithError(err).WithField("Read", n).WithField("Expected", len(buf)).Error("Reading frame ")
			frameBuffers.Return(buf)
			break
		}
		f, err := ReadFrame(buf)
		if err != nil {
			log.WithField("Size", size[0]).WithError(err).Error("Reading frame")
			frameBuffers.Return(buf)
			break
		}
		if debug {
			log.WithField("Frame", f.String()).Debug("RECV")
		}
		l.recv <- &f
	}
	l.conn.Close()
	atomic.StoreUint32(&l.closed, 1)
}

func (l *LocalConnection) Queue() int {
	return len(l.send)
}

func (l *LocalConnection) Send(frame *Frame) error {
	if debug {
		log.WithField("Frame", frame.String()).Debug("Sending frame")
	}
	//TODO: Flow Control
	l.send <- frame
	return nil
}

func (l *LocalConnection) Ok() bool {
	return atomic.LoadUint32(&l.closed) == 0
}

func (l *LocalConnection) Close() error {
	err := l.conn.Close()
	atomic.StoreUint32(&l.closed, 1)
	return err
}

const PROCESS_ADDRESS = "localhost:6887"

type LocalConnectionFactory struct {
	connections map[uint32]*LocalConnection
	lock        sync.RWMutex
	recv        chan<- *Frame
	serverConn  net.Listener
}

func NewLocalConnectionFactory() (*LocalConnectionFactory, error) {
	res := LocalConnectionFactory{}
	var err error
	res.serverConn, err = net.Listen("tcp", PROCESS_ADDRESS)
	if err != nil {
		return &res, err
	}
	res.connections = make(map[uint32]*LocalConnection)
	go res.listen()
	return &res, nil
}

func (l *LocalConnectionFactory) SetRouter(router Router) {
	l.recv = router.Recv()
}

func (l *LocalConnectionFactory) listen() {
	for {
		conn, err := l.serverConn.Accept()
		if err != nil {
			panic(err.Error())
		}
		//TODO: Replace with secure tokens
		// Read ProcessId
		pid := [4]byte{0, 0, 0, 0}
		n, err := conn.Read(pid[0:4])
		if err != nil || n != 4 {
			conn.Close()
			log.WithField("Read", n).WithError(err).Error("Reading Connection Header")
		} else {
			pid := binary.BigEndian.Uint32(pid[:])
			l.lock.Lock()
			connection := newLocalConnection(pid, conn, l.recv)
			l.connections[pid] = connection
			if debug {
				log.WithField("Pid", pid).Debug("Registering new connection")
			}
			l.lock.Unlock()
		}
	}
}

func (l *LocalConnectionFactory) Get(address Address, recv chan<- *Frame) (Connection, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	conn, ok := l.connections[address.Process]
	if !ok {
		return nil, fmt.Errorf("No Connection found for process %v", address.Process)
	}
	return conn, nil
}
