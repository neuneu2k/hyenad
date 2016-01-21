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

import "net"

import (
	"encoding/binary"
	log "github.com/Sirupsen/logrus"
	"io"
	"sync/atomic"
)

type HyenaClient struct {
	address     Address
	conn        net.Conn
	send        chan *Frame
	handlerChan chan inboundStream
	nextId      uint64
	listener    StreamListener
}

func NewHyenaClient(pid uint32, listener StreamListener) (HyenaClient, error) {
	InitFrameBuffers()
	res := HyenaClient{}
	conn, err := net.Dial("tcp", PROCESS_ADDRESS)
	if err != nil {
		return res, err
	}
	res.address = Address{0, pid}
	res.conn = conn
	res.send = make(chan *Frame)
	res.handlerChan = make(chan inboundStream, 256)
	res.listener = listener
	pidBuf := [4]byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(pidBuf[0:4], pid)
	n, err := res.conn.Write(pidBuf[0:4])
	if err != nil || n != 4 {
		return res, err
	}
	go res.write()
	go res.read()
	go res.handlers()
	return res, nil
}

type StreamListener interface {
	OnStream(stream ReadStream)
}

func (hc *HyenaClient) CreateStream(dest string) *WriteStream {
	id := atomic.AddUint64(&hc.nextId, 1)
	s := NewWriteStream(CreateMid(hc.address.Node, hc.address.Process, id), dest, hc.send)
	return s
}

func (hc *HyenaClient) StreamTo(dest string, reader io.Reader) {
	s := hc.CreateStream(dest)
	io.Copy(s, reader)
	s.Close()
}

func (hc *HyenaClient) write() {
	stop := false
	for f := range hc.send {
		buf := f.Buffer()
		if !stop {
			size := byte(len(buf))
			n, err := hc.conn.Write([]byte{size})
			if err != nil || n != 1 {
				log.WithError(err).Error("Writing to hyenad client connection")
				hc.conn.Close()
				stop = true
			}
			n, err = hc.conn.Write(buf)
			if err != nil || n != len(buf) {
				log.WithError(err).Error("Writing to hyenad client connection")
				hc.conn.Close()
				stop = true
			}
			frameBuffers.Return(buf)
		} else {
			frameBuffers.Return(buf)
			log.WithField("Frame", f).Warn("Dropping frame, connection down")
		}
	}
}

type inboundStream struct {
	frames chan *Frame
	stream ReadStream
}

func (hc *HyenaClient) handlers() {
	for s := range hc.handlerChan {
		log.WithField("StreamId", s.stream.id).WithField("Listener", hc.listener).Debug("Calling stream handler")
		hc.listener.OnStream(s.stream)
	}
}

func (hc *HyenaClient) read() {
	size := [1]byte{0}
	streams := make(map[MsgId]inboundStream)
	var stream inboundStream
	var ok bool
	for {
		n, err := hc.conn.Read(size[0:1])
		if err != nil || n != 1 {
			log.WithError(err).WithField("Wrote", n).Error("Reading frame size")
			break
		}
		buf := frameBuffers.Get()
		buf = buf[0:size[0]]
		n, err = io.ReadFull(hc.conn, buf)
		if err != nil || n != len(buf) {
			log.WithError(err).WithField("Read", n).WithField("Expected", len(buf)).Error("Reading frame ")
			frameBuffers.Return(buf)
			break
		}
		f, err := ReadFrame(buf)
		if err != nil {
			frameBuffers.Return(buf)
			log.WithError(err).Error("Reading frame")
			break
		}
		if debug {
			log.WithField("Frame", f.String()).Debug("Client RECV")
		}
		if f.Flags.Is(FIRSTFRAME) {
			stream = inboundStream{frames: make(chan *Frame, 2)}
			stream.frames <- &f
			stream.stream, err = NewReadStream(stream.frames)
			if err != nil {
				frameBuffers.Return(buf)
				log.WithField("Frame", f.String()).WithError(err).Error("Creating read stream")
				break
			}
			if debug {
				log.WithField("Stream", stream.stream).WithField("Listener", hc.listener).Debug("Sending stream to listener")
			}
			hc.handlerChan <- stream
			if !f.Flags.Is(LASTFRAME) {
				streams[f.Id] = stream
			} else {
				close(stream.frames)
			}
		} else {
			stream, ok = streams[f.Id]
			if !ok {
				frameBuffers.Return(buf)
				log.WithField("Frame", f.String()).Error("No stream found")
				continue
			}
			if debug {
				log.WithField("Stream", stream.stream).WithField("Listener", hc.listener).Debug("Sending frame to existing stream")
			}
			stream.frames <- &f
		}
		if f.Flags.Is(LASTFRAME) && !f.Flags.Is(FIRSTFRAME) {
			close(stream.frames)
			delete(streams, f.Id)
		}
	}
	hc.conn.Close()
}
