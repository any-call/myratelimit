package myratelimit

import (
	"github.com/juju/ratelimit"
	"io"
	"net"
	"sync"
)

// RateLimitedConn 封装后的限速器，实现 io.ReadWriter 接口
type RateLimitedConn struct {
	reader io.Reader
	writer io.Writer

	readBucket  *ratelimit.Bucket
	writeBucket *ratelimit.Bucket

	// 如果底层是 net.Conn，保留它，方便零拷贝优化
	underlying net.Conn
}

// Read 调用限速后的 reader
func (r *RateLimitedConn) Read(p []byte) (int, error) {
	if r.readBucket != nil {
		n := int(r.readBucket.Take(int64(len(p))))
		if n <= 0 {
			r.readBucket.Wait(int64(len(p)))
			n = len(p)
		}
		return r.reader.Read(p[:n])
	}
	return r.reader.Read(p)
}

// Write 调用限速后的 writer
func (r *RateLimitedConn) Write(p []byte) (int, error) {
	if r.writeBucket != nil {
		total := 0
		for total < len(p) {
			n := int(r.writeBucket.Take(int64(len(p) - total)))
			if n <= 0 {
				r.writeBucket.Wait(int64(len(p) - total))
				n = len(p) - total
			}
			w, err := r.writer.Write(p[total : total+n])
			total += w
			if err != nil {
				return total, err
			}
		}
		return total, nil
	}
	return r.writer.Write(p)
}

// ---------------- 零拷贝优化 (仅当 underlying 是 net.Conn) ----------------
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 32*1024)
		return &b
	},
}

func (r *RateLimitedConn) WriteTo(w io.Writer) (int64, error) {
	if r.underlying != nil && r.readBucket == nil {
		if wt, ok := r.underlying.(io.WriterTo); ok {
			return wt.WriteTo(w) // 零拷贝
		}
	}

	buf := bufPool.Get().(*[]byte)
	defer bufPool.Put(buf)

	var written int64
	for {
		n, err := r.Read(*buf)
		if n > 0 {
			wc, ew := w.Write((*buf)[:n])
			written += int64(wc)
			if ew != nil {
				return written, ew
			}
			if wc < n {
				return written, io.ErrShortWrite
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return written, err
		}
	}
	return written, nil
}

func (c *RateLimitedConn) ReadFrom(r io.Reader) (int64, error) {
	if c.underlying != nil && c.writeBucket == nil {
		if rf, ok := c.underlying.(io.ReaderFrom); ok {
			return rf.ReadFrom(r) // 零拷贝
		}
	}

	buf := bufPool.Get().(*[]byte)
	defer bufPool.Put(buf)

	var written int64
	for {
		n, er := r.Read(*buf)
		if n > 0 {
			wc, ew := c.Write((*buf)[:n])
			written += int64(wc)
			if ew != nil {
				return written, ew
			}
			if wc < n {
				return written, io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				break
			}
			return written, er
		}
	}
	return written, nil
}

// WrapWithRateLimit 包装 io.ReadWriter，为读写方向添加速率限制。
// readLimit 和 writeLimit 单位是字节每秒（Byte/s），若 <= 0 表示该方向不限制。
func WrapWithRateLimit(rw io.ReadWriter, readLimit, writeLimit int64) io.ReadWriter {
	if readLimit <= 0 && writeLimit <= 0 {
		return rw
	}

	var conn net.Conn = nil
	if c, ok := rw.(net.Conn); ok {
		conn = c
	}

	return &RateLimitedConn{
		reader:      rw,
		writer:      rw,
		readBucket:  bucketFromLimit(readLimit),
		writeBucket: bucketFromLimit(writeLimit),
		underlying:  conn,
	}
}

func bucketFromLimit(limit int64) *ratelimit.Bucket {
	if limit > 0 {
		return ratelimit.NewBucketWithRate(float64(limit), limit)
	}
	return nil
}
