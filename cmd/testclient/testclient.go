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
package main

import (
	"bytes"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/neuneu2k/hyenad"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

func runOnce(pid uint32) {
	listener := newListener("", 1)
	client, err := hyenad.NewHyenaClient(pid, listener)
	if err != nil {
		panic(err)
	}
	target := 1
	if pid == 1 {
		target = 2
	}
	dest := fmt.Sprintf("s:/test%v/toto/tata", target)
	log.WithField("pid", pid).Info("Client started")
	time.Sleep(5 * time.Second)
	message := fmt.Sprintf("Message from Process %v", pid)
	reader := bytes.NewReader([]byte(message))
	log.Info("Sending message")
	client.StreamTo(dest, reader)
	log.Info("Message sent")
	listener.wait.Wait()
	log.Info("Done")
}

func runSend(iterations int, profile bool) {
	if profile {
		f, err := os.Create("send.cpu")
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		f, err = os.Create("send.mem")
		if err != nil {
			panic(err)
		}
		defer func() {
			pprof.WriteHeapProfile(f)
			f.Close()
		}()
	}
	listener := newListener("", 0)
	client, err := hyenad.NewHyenaClient(1, listener)
	if err != nil {
		panic(err)
	}
	target := 2
	dest := fmt.Sprintf("s:/test%v/toto/tata", target)
	log.Info("Sending Client started")
	bigString := strings.Repeat("Test", 40)
	start := time.Now()
	for i := 0; i < iterations; i++ {
		message := fmt.Sprintf("Message %v from Process %v, [%v]", i, 1, bigString)
		if i == 0 {
			log.WithField("MessageLength", len(message)).Info("Message created")
		}
		reader := bytes.NewReader([]byte(message))
		client.StreamTo(dest, reader)
		if i%5000 == 0 {
			log.WithField("Messages", i).Info("Sending Messages")
		}
	}
	duration := time.Since(start)
	listener.wait.Wait()
	msgPerS := float64(iterations) / float64(duration.Seconds())
	log.WithField("Messages", iterations).WithField("Time", duration.String()).WithField("Msg/S", msgPerS).Info("Done")
}

func runRecv(iterations int, profile bool) {
	if profile {
		f, err := os.Create("recv.profile")
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		f, err = os.Create("recv.mem")
		if err != nil {
			panic(err)
		}
		defer func() {
			pprof.WriteHeapProfile(f)
			f.Close()
		}()
	}
	listener := newListener("", iterations)
	_, err := hyenad.NewHyenaClient(2, listener)
	if err != nil {
		panic(err)
	}
	log.WithField("Waiting for messages", iterations).Info("Receiving Client started")
	listener.wait.Wait()
	duration := time.Since(listener.startTime)
	msgPerS := float64(iterations) / float64(duration.Seconds())
	log.WithField("Messages", iterations).WithField("Time", duration.String()).WithField("Msg/S", msgPerS).Info("Done")
}

func run(c *cli.Context) {
	pid := uint32(c.Int("pid"))
	bench := c.Bool("bench")
	iterations := 1000000
	profile := c.Bool("profile")
	if bench {
		if pid == 1 {
			runSend(iterations, profile)
		} else {
			runRecv(iterations, profile)
		}
	} else {
		runOnce(pid)
	}
}

type listener struct {
	name      string
	wait      sync.WaitGroup
	expected  int
	received  uint32
	startTime time.Time
	startFunc sync.Once
}

func newListener(name string, expected int) *listener {
	res := listener{}
	res.expected = expected
	res.wait.Add(expected)
	return &res
}

func (s *listener) start() {
	s.startTime = time.Now()
}

func (s *listener) OnStream(stream hyenad.ReadStream) {
	s.startFunc.Do(s.start)
	buf := make([]byte, 4096)
	n, _ := stream.Read(buf)
	if s.expected == 1 {
		fmt.Fprintf(os.Stdout, "Message %v on destination %v\n", stream.MessageId(), stream.Destination())
		os.Stdout.Write(buf[0:n])
		fmt.Fprint(os.Stdout, "\nEnd of message\n")
	}
	s.wait.Done()
}

func main() {
	app := cli.NewApp()
	app.Name = "testclient"
	app.Usage = "Hyena Net Daemon test client"
	app.Action = run
	app.Version = "0.1.0"
	app.Flags = []cli.Flag{
		cli.IntFlag{
			Name:  "pid, p",
			Value: 1,
			Usage: "ProcessId for client (1 or 2)",
		},
		cli.BoolFlag{
			Name:  "bench, b",
			Usage: "Do a Benchmark",
		},
		cli.BoolFlag{
			Name:  "profile",
			Usage: "Save profiling data",
		},
	}
	app.Run(os.Args)
}
