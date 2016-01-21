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

type Flags byte

func (f Flags) Is(flag Flags) bool {
	return f&flag != 0
}

const (
	FIRSTFRAME Flags = 1
	LASTFRAME  Flags = 2
)

const FrameHeaderSize = 16 + 8 + 1

const MaxFrameSize = 255

type FrameHeader struct {
	Id          MsgId
	FrameNumber uint64
	Flags       Flags
	Dest        string
}

func (f *FrameHeader) String() string {
	return fmt.Sprintf("{Id:%v,FirstFrame:%v, LastFrame:%v, FrameNum:%v, Dest:%v}", f.Id, f.Flags.Is(FIRSTFRAME), f.Flags.Is(LASTFRAME), f.FrameNumber, f.Dest)
}

func (f *FrameHeader) write(buf *[]byte) {
	f.Id.WriteTo(buf)
	*buf = append(*buf, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64((*buf)[16:24], f.FrameNumber)
	(*buf)[24] = byte(f.Flags)
	if len(f.Dest) > 0 {
		*buf = append(*buf, 0)
		(*buf)[25] = byte(len(f.Dest))
		*buf = append(*buf, []byte(f.Dest)...)
	}
}

func (f *FrameHeader) read(buf []byte) error {
	if len(buf) < FrameHeaderSize {
		return fmt.Errorf("Illegal buffer size %v < FrameHeaderSize(%v)", len(buf), FrameHeaderSize)
	}
	err := f.Id.ReadFrom(buf[0:16])
	if err != nil {
		return err
	}
	f.FrameNumber = binary.BigEndian.Uint64(buf[16:24])
	f.Flags = Flags(buf[24])
	if f.Flags.Is(FIRSTFRAME) {
		if len(buf) < 25 {
			return fmt.Errorf("Illegal buffer size for a first frame %v < 25 Buffer:%v", len(buf), buf)
		}
		destLen := int(buf[25])
		if len(buf) < 25+destLen {
			return fmt.Errorf("Illegal buffer size for a first frame %v < %v (header:25,dest:%v) Buffer:%v", len(buf), 25+destLen, destLen, buf)
		}
		f.Dest = string(buf[26 : 25+destLen+1])
	}
	return nil
}

type Frame struct {
	FrameHeader
	buffer []byte
}

func (f *Frame) Buffer() []byte {
	return f.buffer
}

func (f *Frame) Contents() []byte {
	var headerSize int
	if len(f.Dest) > 0 {
		headerSize = FrameHeaderSize + len(f.Dest) + 1
	} else {
		headerSize = FrameHeaderSize
	}
	return f.buffer[headerSize:]
}

func (f *Frame) String() string {
	return fmt.Sprintf("Frame{Header:%v, ContentsLength:%v}", f.FrameHeader.String(), len(f.Contents()))
}

func NewFrame(header FrameHeader, data []byte) (Frame, error) {
	f := Frame{FrameHeader: header}
	var remaining int = MaxFrameSize - FrameHeaderSize
	if f.Flags.Is(FIRSTFRAME) {
		remaining -= (len(f.Dest) + 1)
	}
	if len(data) > remaining {
		return f, fmt.Errorf("Provided data(%v bytes) too long for frame (max size: %v bytes)", len(data), remaining)
	}
	f.buffer = frameBuffers.Get()
	f.write(&f.buffer)
	f.buffer = append(f.buffer, data...)
	return f, nil
}

func ReadFrame(buffer []byte) (Frame, error) {
	res := Frame{}
	if len(buffer) > MaxFrameSize {
		return res, fmt.Errorf("Buffer too long (%v>%v)", len(buffer), MaxFrameSize)
	}
	res.buffer = buffer
	err := res.FrameHeader.read(buffer)
	return res, err
}
