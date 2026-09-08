package myratelimit

import (
	"bytes"
	"io"
	"testing"

	"github.com/juju/ratelimit"
)

func TestRateLimitedConnReadDoesNotUseWaitDurationAsSliceLength(t *testing.T) {
	data := bytes.Repeat([]byte("r"), 32*1024)
	bucket := depletedTestBucket(int64(len(data)))
	conn := &RateLimitedConn{
		reader:     bytes.NewReader(data),
		readBucket: bucket,
	}

	buf := make([]byte, len(data))
	n, err := conn.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("read err: %v", err)
	}
	if n != len(data) {
		t.Fatalf("read n = %d, want %d", n, len(data))
	}
	if !bytes.Equal(buf[:n], data) {
		t.Fatal("read data does not match input")
	}
}

func TestRateLimitedConnWriteDoesNotUseWaitDurationAsSliceLength(t *testing.T) {
	data := bytes.Repeat([]byte("w"), 32*1024)
	bucket := depletedTestBucket(int64(len(data)))
	var dst bytes.Buffer
	conn := &RateLimitedConn{
		writer:      &dst,
		writeBucket: bucket,
	}

	n, err := conn.Write(data)
	if err != nil {
		t.Fatalf("write err: %v", err)
	}
	if n != len(data) {
		t.Fatalf("write n = %d, want %d", n, len(data))
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatal("written data does not match input")
	}
}

func TestRateLimitedConnWriteHandlesZeroLengthWrite(t *testing.T) {
	conn := &RateLimitedConn{
		writer:      io.Discard,
		writeBucket: bucketFromLimit(1024),
	}

	n, err := conn.Write(nil)
	if err != nil {
		t.Fatalf("write err: %v", err)
	}
	if n != 0 {
		t.Fatalf("write n = %d, want 0", n)
	}
}

func TestBucketFromLimitHasMinimumWaitCapacity(t *testing.T) {
	bucket := bucketFromLimit(1)
	if bucket.Capacity() < maxBucketWaitBytes {
		t.Fatalf("bucket capacity = %d, want at least %d", bucket.Capacity(), maxBucketWaitBytes)
	}
}

func depletedTestBucket(capacity int64) *ratelimit.Bucket {
	bucket := ratelimit.NewBucketWithRate(100*1024*1024, capacity)
	bucket.Take(capacity)
	return bucket
}
