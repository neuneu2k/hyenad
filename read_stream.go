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
	"github.com/Sirupsen/logrus"
	"io"
)

type ReadStream struct {
	frames       <-chan *Frame
	currentFrame *Frame
	dest         string
	id           MsgId
	currentIndex int
}

func NewReadStream(frames <-chan *Frame) (stream ReadStream, err error) {
	res := ReadStream{frames: frames}
	res.currentFrame = <-frames
	if res.currentFrame == nil {
		return res, errors.New("Empty stream")
	}
	if res.currentFrame.FrameNumber != 0 {
		return res, errors.New("First frame of stream not Frame0")
	}
	res.dest = res.currentFrame.Dest
	res.id = res.currentFrame.Id
	return res, nil
}

func (r ReadStream) Destination() string {
	return r.dest
}

func (r ReadStream) MessageId() MsgId {
	return r.id
}

func (r *ReadStream) Read(p []byte) (n int, err error) {
	if r.currentFrame == nil {
		return 0, io.EOF
	}
	size := len(p)
	wrote := 0
	if size == 0 {
		return 0, nil
	}
	for wrote < size {
		remaining := len(r.currentFrame.Contents()) - r.currentIndex
		if remaining == 0 {
			if debug {
				logrus.WithField("Stream", r).Debug("Waiting for another frame")
			}
			frameBuffers.Return(r.currentFrame.buffer)
			r.currentFrame = <-r.frames
			r.currentIndex = 0
			if r.currentFrame == nil {
				if debug {
					logrus.WithField("Stream", r).Debug("Last Frame")
				}
				return wrote, io.EOF
			}
		} else {
			if remaining > (size - wrote) {
				remaining = size - wrote
			}
			copy(p[wrote:wrote+remaining], r.currentFrame.Contents()[r.currentIndex:r.currentIndex+remaining])
			wrote += remaining
			r.currentIndex += remaining
		}
	}
	return wrote, nil
}
