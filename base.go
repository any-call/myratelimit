package myratelimit

import (
	"github.com/juju/ratelimit"
	"io"
)

// RateLimitedConn 封装后的限速器，实现 io.ReadWriter 接口
type RateLimitedConn struct {
	reader io.Reader
	writer io.Writer
}

// Read 调用限速后的 reader
func (r *RateLimitedConn) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

// Write 调用限速后的 writer
func (r *RateLimitedConn) Write(p []byte) (int, error) {
	return r.writer.Write(p)
}

// WrapWithRateLimit 包装 io.ReadWriter，为读写方向添加速率限制。
// readLimit 和 writeLimit 单位是字节每秒（Byte/s），若 <= 0 表示该方向不限制。
func WrapWithRateLimit(rw io.ReadWriter, readLimit, writeLimit int64) io.ReadWriter {
	var r io.Reader = rw
	var w io.Writer = rw

	if readLimit > 0 {
		rBucket := ratelimit.NewBucketWithRate(float64(readLimit), readLimit)
		r = ratelimit.Reader(r, rBucket)
	}

	if writeLimit > 0 {
		wBucket := ratelimit.NewBucketWithRate(float64(writeLimit), writeLimit)
		w = ratelimit.Writer(w, wBucket)
	}

	return &RateLimitedConn{
		reader: r,
		writer: w,
	}
}
