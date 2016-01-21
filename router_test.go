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
	"strings"
	"testing"
	"time"
)

func TestRouter(t *testing.T) {
	routing := singleTargetRouting{}
	factory := newLogConnectionFactory()
	router := NewRouter(&routing, factory)
	frames := make(chan *Frame)
	stream := NewWriteStream(CreateMid(0, 0, 1), "/test", frames)
	toSend := []byte(strings.Repeat("Lorel Ipsum Dolor Sic Amet... ", 200))
	go func() {
		stream.Write(toSend)
		stream.Close()
		close(frames)
	}()
	for f := range frames {
		router.Recv() <- f
	}
	time.Sleep(100 * time.Millisecond)
	router.Stop()
}
