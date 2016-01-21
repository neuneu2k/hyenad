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
)

type Router struct {
	routing   Routing
	recv      chan *Frame
	closeChan chan struct{}
	factory   ConnectionFactory
}

type ConnectionFactory interface {
	SetRouter(router Router)
	Get(address Address, recv chan<- *Frame) (Connection, error)
}

func NewRouter(routing Routing, factory ConnectionFactory) Router {
	InitFrameBuffers()
	res := Router{}
	res.routing = routing
	res.factory = factory
	res.recv = make(chan *Frame, 64)
	res.closeChan = make(chan struct{})
	res.factory.SetRouter(res)
	go res.run()
	return res
}

func bestAddress(addresses Addresses) Address {
	if len(addresses) == 0 {
		return INVALID_ADDRESS
	} else {
		return addresses[0]
	}
}

func (r *Router) run() {
	connections := make(map[MsgId]Connection)
	if debug {
		log.WithField("Router", r).Debug("Listening for frames")
	}
stop:
	for {
		select {
		case f := <-r.recv:
			{
				if f.FrameNumber == 0 {
					addresses := r.routing.Route(f.Dest)
					address := bestAddress(addresses)
					conn, err := r.factory.Get(address, r.recv)
					if err == nil {
						conn.Send(f)
						if !f.Flags.Is(LASTFRAME) {
							connections[f.Id] = conn
						}
					} else {
						log.WithField("Frame", f.String()).WithField("Destination", f.Dest).Error("No connection found for destination")
						frameBuffers.Return(f.buffer)
					}
				} else {
					conn, ok := connections[f.Id]
					if ok {
						conn.Send(f)
						if f.Flags.Is(LASTFRAME) {
							delete(connections, f.Id)
						}
					} else {
						log.WithField("Frame", f.String()).Error("No active connection found for FrameId")
						frameBuffers.Return(f.buffer)
					}
				}
			}
		case _ = <-r.closeChan:
			{
				break stop
			}
		}
	}
}

func (r *Router) Recv() chan<- *Frame {
	return r.recv
}

func (r *Router) Stop() {
	close(r.closeChan)
}
