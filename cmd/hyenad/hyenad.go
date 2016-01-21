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
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/neuneu2k/hyenad"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
)

func run(c *cli.Context) {
	if c.Bool("profile") {
		f, err := os.Create("hyenad.cpu")
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		f, err = os.Create("hyenad.mem")
		if err != nil {
			panic(err)
		}
		defer func() {
			pprof.WriteHeapProfile(f)
			f.Close()
		}()
	}
	config := Config{}
	configFile, err := os.Open(c.String("config"))
	if err != nil {
		panic(err)
	}
	err = json.NewDecoder(configFile).Decode(&config)
	if err != nil {
		panic(err)
	}
	log.WithField("Config", config).Debug("Config file loaded")
	factory, err := hyenad.NewLocalConnectionFactory()
	if err != nil {
		panic(err)
	}
	routing := hyenad.NewRoutingTree()
	routing.Apply(config.Routing)
	router := hyenad.NewRouter(routing, factory)
	log.Info("Started Router")
	closeChan := make(chan os.Signal, 1)
	signal.Notify(closeChan, os.Interrupt)
	signal.Notify(closeChan, syscall.SIGTERM)
	<-closeChan
	router.Stop()
	log.Info("Stopped Router")
}

func main() {
	app := cli.NewApp()
	app.Name = "router"
	app.Usage = "Hyena Net Routing Daemon"
	app.Action = run
	app.Version = "0.1.0"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: "hyenad.json",
			Usage: "HyenaD configuration",
		},
		cli.BoolFlag{
			Name:  "profile",
			Usage: "Save profiling data",
		},
	}
	app.Run(os.Args)
}

type Config struct {
	Routing hyenad.RoutingTreeUpdate
}
