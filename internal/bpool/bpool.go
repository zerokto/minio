// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package bpool

// BytePoolCap implements a leaky pool of []byte in the form of a bounded channel.
type BytePoolCap struct {
	c    chan []byte // 缓冲字节切片的有限容量通道
	w    int         // 字节切片的宽度
	wcap int         // 每个字节切片的容量大小
}

// NewBytePoolCap creates a new BytePool bounded to the given maxSize, with new
// byte arrays sized based on width.
// 表示创建一个新的字节池，其中 maxSize 表示池的最大容量，
// width 表示每个切片的宽度，capwidth 表示每个切片的容量宽度。
func NewBytePoolCap(maxSize int, width int, capwidth int) (bp *BytePoolCap) {
	return &BytePoolCap{
		c:    make(chan []byte, maxSize),
		w:    width,
		wcap: capwidth,
	}
}

// Get gets a []byte from the BytePool, or creates a new one if none are
// available in the pool.
func (bp *BytePoolCap) Get() (b []byte) {
	// 使用select避免在池中没有切片时，程序被阻塞等待
	select {
	case b = <-bp.c:
	// reuse existing buffer
	default:
		// create new buffer
		if bp.wcap > 0 {
			b = make([]byte, bp.w, bp.wcap)
		} else {
			b = make([]byte, bp.w)
		}
	}
	return
}

// Put returns the given Buffer to the BytePool.
func (bp *BytePoolCap) Put(b []byte) {
	select {
	case bp.c <- b:
		// buffer went back into pool
	default:
		// buffer didn't go back into pool, just discard
	}
}

// Width returns the width of the byte arrays in this pool.
func (bp *BytePoolCap) Width() (n int) {
	return bp.w
}

// WidthCap returns the cap width of the byte arrays in this pool.
func (bp *BytePoolCap) WidthCap() (n int) {
	return bp.wcap
}
