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
	"encoding/binary"
	"fmt"
)

type MsgId [18]byte

func (m MsgId) String() string {
	return SBase64.EncodeToString(m[:])
}

func CreateMid(nid uint32, pid uint32, mid uint64) MsgId {
	m := MsgId{}
	binary.BigEndian.PutUint32(m[2:6], nid)
	binary.BigEndian.PutUint32(m[6:10], pid)
	binary.BigEndian.PutUint64(m[10:18], mid)
	return m
}

func (m *MsgId) Split() (nid uint32, pid uint32, mid uint64) {
	nid = binary.BigEndian.Uint32(m[2:6])
	pid = binary.BigEndian.Uint32(m[6:10])
	mid = binary.BigEndian.Uint64(m[10:18])
	return
}

func (m *MsgId) WriteTo(buf *[]byte) {
	*buf = append(*buf, []byte(m[2:18])...)
}

func (m *MsgId) Bytes() []byte {
	return m[2:18]
}

func (f *MsgId) ReadFrom(buf []byte) error {
	if len(buf) != 16 {
		return fmt.Errorf("Illegal MessageIdSize %v!=16", len(buf))
	}
	copy(f[2:18], buf[0:16])
	return nil
}
