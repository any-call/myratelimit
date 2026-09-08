package myratelimit

import (
	"io"
	"net"
	"sync"

	"github.com/juju/ratelimit"
)

const maxBucketWaitBytes int64 = 256 * 1024

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
	n, err := r.reader.Read(p)
	waitBucket(r.readBucket, int64(n))
	return n, err
}

// Write 调用限速后的 writer
func (r *RateLimitedConn) Write(p []byte) (int, error) {
	waitBucket(r.writeBucket, int64(len(p)))

	total := 0
	for total < len(p) {
		n, err := r.writer.Write(p[total:])
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func waitBucket(bucket *ratelimit.Bucket, count int64) {
	if bucket == nil || count <= 0 {
		return
	}

	chunkSize := bucket.Capacity()
	if chunkSize <= 0 {
		return
	}
	if chunkSize > maxBucketWaitBytes {
		chunkSize = maxBucketWaitBytes
	}

	for remaining := count; remaining > 0; {
		chunk := remaining
		if chunk > chunkSize {
			chunk = chunkSize
		}
		bucket.Wait(chunk)
		remaining -= chunk
	}
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
		capacity := limit
		if capacity < maxBucketWaitBytes {
			capacity = maxBucketWaitBytes
		}
		return ratelimit.NewBucketWithRate(float64(limit), capacity)
	}
	return nil
}
