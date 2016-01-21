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
	"bytes"
	log "github.com/Sirupsen/logrus"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

func TestFrameStream(t *testing.T) {
	InitFrameBuffers()
	LongString := strings.Repeat("Lorel Ipsum Dolor Sic Amet... ", 50)
	log.WithField("Bytes", len(LongString)).Info("Writing to frames")
	reader := bytes.NewReader([]byte(LongString))
	id := CreateMid(0, 0, 0)
	frames := make(chan *Frame)
	writeStream := NewWriteStream(id, "/test/toto/tata", frames)
	go func() {
		io.Copy(writeStream, reader)
		writeStream.Close()
		close(frames)
		log.Info("Wrote all frames")
	}()
	countedFrames := make(chan *Frame)
	var count uint32
	log.Info("Reading from frames")
	go func() {
		for f := range frames {
			log.WithField("Count", atomic.LoadUint32(&count)).WithField("Frame", f.String()).Debug("Count one frame")
			atomic.AddUint32(&count, 1)
			countedFrames <- f
		}
		log.WithField("Count", atomic.LoadUint32(&count)).Info("Finished counting")
		close(countedFrames)
	}()
	readStream, err := NewReadStream(countedFrames)
	if err != nil {
		log.WithError(err).Error("Creating ReadStream")
		t.Fail()
	}
	if readStream.Destination() != "/test/toto/tata" {
		log.WithField("Destination", readStream.Destination()).Error("Expected /test/toto/tata")
		t.Fail()
	}
	if readStream.MessageId() != id {
		log.WithField("Id", readStream.MessageId()).Error("Expected ", id)
		t.Fail()
	}
	writer := bytes.Buffer{}
	io.Copy(&writer, &readStream)
	roundTripped := string(writer.Bytes())
	if roundTripped != LongString {
		log.WithField("Frames", atomic.LoadUint32(&count)).WithField("Original", LongString).WithField("RoundTripped", roundTripped).Error("Round trip failed")
		t.Error("Round trip failed")
	} else {
		log.WithField("Frames", atomic.LoadUint32(&count)).Info("Round Trip ok")
	}
}
