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
	"fmt"
	"strconv"
	"strings"
)

type Routing interface {
	Route(destination string) Addresses
}

type Addresses []Address

var INVALID_ADDRESS = Address{0, 0}

type Address struct {
	Node    uint32
	Process uint32
}

func (a *Address) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Itoa(int(a.Node)) + "." + strconv.Itoa(int(a.Process))), nil
}

func (a *Address) UnmarshalJSON(input []byte) error {
	pair := strings.Split(string(input), ".")
	if len(pair) != 2 {
		return fmt.Errorf("Invalid Address Format")
	}
	ns, ps := pair[0], pair[1]
	n, err := strconv.ParseInt(ns, 10, 32)
	if err != nil {
		return err
	}
	p, err := strconv.ParseInt(ps, 10, 32)
	if err != nil {
		return err
	}
	a.Node = uint32(n)
	a.Process = uint32(p)
	return nil
}
