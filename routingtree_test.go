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

func check(dest string, expected Addresses, routing *RoutingTree, t *testing.T) {
	res := routing.Route(dest)
	if len(expected) == 0 {
		if len(res) != 0 {
			t.Errorf("Invalid result, expected [] and got %v\n", res)
			return
		}
	}
	if len(res) != len(expected) {
		t.Errorf("Invalid result, expected %v and got %v\n", expected, res)
		return
	}
	for i, a := range expected {
		if res[i] != a {
			t.Errorf("Invalid result, expected %v and got %v\n", expected, res)
		}
	}
}

func TestRoutingTree(t *testing.T) {
	routing := NewRoutingTree()
	check("t:/test", Addresses{}, routing, t)
	routing.UpsertSimpleRule("s:/test1", Simple{
		Targets: Addresses{Address{0, 1}},
	})
	routing.UpsertSimpleRule("s:/test2", Simple{
		Targets: Addresses{Address{0, 2}},
	})
	check("s:/test1/toto/tata", Addresses{Address{0, 1}}, routing, t)
	check("s:/test2/toto/tata", Addresses{Address{0, 2}}, routing, t)
	routing.UpsertShardedRule("s:/test/", Sharded{
		ShardEntry{
			From:    "0000",
			To:      "0010",
			Targets: Addresses{Address{0, 1}},
		},
		ShardEntry{
			From:    "0011",
			To:      "0020",
			Targets: Addresses{Address{0, 2}},
		},
	})
	check("s:/test/0008/tata", Addresses{Address{0, 1}}, routing, t)
	check("s:/test/0018/tata", Addresses{Address{0, 2}}, routing, t)
	check("s:/test/0030/tata", Addresses{}, routing, t)
}

func TestRouterWithRoutingTree(t *testing.T) {
	routing := NewRoutingTree()
	routing.UpsertSimpleRule("s:/test1", Simple{
		Targets: Addresses{Address{0, 1}},
	})
	routing.UpsertSimpleRule("s:/test2", Simple{
		Targets: Addresses{Address{0, 2}},
	})
	routing.UpsertShardedRule("s:/test/", Sharded{
		ShardEntry{
			From:    "0000",
			To:      "0010",
			Targets: Addresses{Address{0, 1}},
		},
		ShardEntry{
			From:    "0011",
			To:      "0020",
			Targets: Addresses{Address{0, 2}},
		},
	})
	factory := newLogConnectionFactory()
	router := NewRouter(routing, factory)
	frames := make(chan *Frame)
	stream := NewWriteStream(CreateMid(0, 0, 1), "s:/test1/toto/tata", frames)
	toSend := []byte(strings.Repeat("Lorel Ipsum Dolor Sic Amet... ", 200))
	go func() {
		stream.Write(toSend)
		stream.Close()
		close(frames)
	}()
	for f := range frames {
		router.Recv() <- f
	}
	frames = make(chan *Frame)
	stream = NewWriteStream(CreateMid(0, 0, 2), "s:/test2/toto/tata", frames)
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
