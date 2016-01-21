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
)

type WriteStream struct {
	Id      MsgId
	dest    string
	frameId uint64
	closed  bool
	toSend  []byte
	output  chan<- *Frame
}

func NewWriteStream(id MsgId, dest string, output chan<- *Frame) *WriteStream {
	res := WriteStream{Id: id, dest: dest}
	res.output = output
	res.toSend = make([]byte, 0, 128)
	return &res
}

func (s *WriteStream) Write(p []byte) (n int, err error) {
	s.toSend = append(s.toSend, p...)
	if len(s.toSend) > MaxFrameSize-FrameHeaderSize {
		n2, frame, err2 := s.writeFrame(s.toSend, false)
		if err2 != nil {
			err = err2
		} else {
			remaining := len(s.toSend) - n2
			copy(s.toSend, s.toSend[n2:n2+remaining])
			s.toSend = s.toSend[0:remaining]
			s.output <- frame
		}
	}
	return len(p), err
}

func (s *WriteStream) Flush(close bool) error {
	remaining := len(s.toSend)
	sent := 0
	for remaining > 0 {
		n, frame, err := s.writeFrame(s.toSend, false)
		if err != nil {
			return err
		}
		remaining := len(s.toSend) - n
		if remaining == 0 {
			// Redo last frame to close stream
			// Rollback frame
			s.frameId = s.frameId - 1
			frameBuffers.Return(frame.buffer)
			n, frame, err = s.writeFrame(s.toSend, true)
		}
		copy(s.toSend, s.toSend[n:n+remaining])
		s.toSend = s.toSend[0:remaining]
		sent++
		s.output <- frame
	}
	if sent == 0 {
		// Damn, write an empty close frame
		_, frame, err := s.writeFrame([]byte{}, true)
		if err != nil {
			return err
		} else {
			s.output <- frame
		}
	}
	return nil
}

func (s *WriteStream) Close() error {
	s.Flush(true)
	s.closed = true
	return nil
}

func (s *WriteStream) writeFrame(p []byte, close bool) (n int, frame *Frame, err error) {
	if s.closed {
		return 0, nil, errors.New("Stream closed")
	}
	header := FrameHeader{}
	header.Id = s.Id
	header.FrameNumber = s.frameId
	s.frameId = s.frameId + 1
	var f Frame
	var remaining int
	if header.FrameNumber == 0 {
		header.Flags = FIRSTFRAME
		if close {
			header.Flags += LASTFRAME
			s.closed = true
		}
		header.Dest = s.dest
		remaining = MaxFrameSize - FrameHeaderSize - (len(s.dest) + 1)
	} else {
		if close {
			header.Flags = LASTFRAME
			s.closed = true
		}
		remaining = MaxFrameSize - FrameHeaderSize
	}
	if remaining > len(p) {
		remaining = len(p)
	}
	f, err = NewFrame(header, p[0:remaining])
	return remaining, &f, err
}
