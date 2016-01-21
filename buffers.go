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
	log "github.com/Sirupsen/logrus"
	"sync"
)

type BuffersContainer struct {
	bufs chan []byte
	size int
}

var (
	frameBuffers     BuffersContainer
	frameBuffersInit sync.Once
	frameBuffersSize int = 1024

	httpBuffers     BuffersContainer
	httpBuffersInit sync.Once
	httpBuffersSize int = 1024
)

func InitFrameBuffers() *BuffersContainer {
	frameBuffersInit.Do(func() {
		frameBuffers = BuffersContainer{bufs: make(chan []byte, frameBuffersSize), size: 256}
		for i := 0; i < frameBuffersSize/2; i++ {
			frameBuffers.bufs <- make([]byte, 0, frameBuffers.size)
		}
		if debug {
			log.WithField("PoolSize", len(frameBuffers.bufs)).WithField("MaxPoolSize", cap(frameBuffers.bufs)).Debug("Initialized frame buffer pool")
		}
	})
	return &frameBuffers
}

func InitHttpBuffers() *BuffersContainer {
	httpBuffersInit.Do(func() {
		httpBuffers = BuffersContainer{bufs: make(chan []byte, httpBuffersSize), size: 8192}
		for i := 0; i < httpBuffersSize/2; i++ {
			httpBuffers.bufs <- make([]byte, 0, httpBuffers.size)
		}
		if debug {
			log.WithField("PoolSize", len(httpBuffers.bufs)).WithField("MaxPoolSize", cap(httpBuffers.bufs)).Debug("Initialized http buffer pool")
		}
	})
	return &frameBuffers
}

func (b BuffersContainer) Get() []byte {
	select {
	case buf := <-b.bufs:
		{
			if debug {
				log.WithField("PoolSize", len(b.bufs)).WithField("MaxPoolSize", cap(b.bufs)).Debug("Got buffer from pool")
			}
			return buf
		}
	default:
		{
			if debug {
				log.WithField("PoolSize", len(b.bufs)).WithField("MaxPoolSize", cap(b.bufs)).Debug("Creating new buffer")
			}
			return make([]byte, 0, 256)
		}
	}
}

func (b BuffersContainer) Return(buf []byte) {
	buf = buf[0:0]
	if cap(buf) == 256 {
		select {
		case b.bufs <- buf:
			{
				if debug {
					log.WithField("Buffer", &buf).WithField("PoolSize", len(b.bufs)).WithField("MaxPoolSize", cap(b.bufs)).Debug("Returned buffer to pool")
				}
			}
		default:
			{
				if debug {
					log.WithField("Buffer", buf).WithField("PoolSize", len(b.bufs)).WithField("MaxPoolSize", cap(b.bufs)).Debug("Garbaging buffer")
				}
			}
		}
	}
}
