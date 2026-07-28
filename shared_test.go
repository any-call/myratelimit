package myratelimit

import (
	"bytes"
	"io"
	"testing"
)

func TestSharedManagerGetOrUpdateSharesLimiterByKey(t *testing.T) {
	manager := newSharedManager()

	first := manager.getOrUpdate(BuildSharedKey("207.145.96.10", "user-a"), 1024, 2048)
	second := manager.getOrUpdate(BuildSharedKey("207.145.96.10", "user-a"), 4096, 8192)
	third := manager.getOrUpdate(BuildSharedKey("207.145.96.10", "user-b"), 1024, 2048)

	if first != second {
		t.Fatal("expected same limiter for same key")
	}
	if first == third {
		t.Fatal("expected different limiter for different key")
	}
	if manager.len() != 2 {
		t.Fatalf("manager len = %d, want 2", manager.len())
	}
	if first.UploadBytesSec() != 4096 {
		t.Fatalf("upload rate = %d, want 4096", first.UploadBytesSec())
	}
	if first.DownloadBytesSec() != 8192 {
		t.Fatalf("download rate = %d, want 8192", first.DownloadBytesSec())
	}
}

func TestGetOrUpdateSharedLimiterUsesDefaultManager(t *testing.T) {
	old := defaultSharedManager
	defaultSharedManager = newSharedManager()
	defer func() {
		defaultSharedManager = old
	}()

	key := BuildSharedKey("207.145.96.10", "user-a")
	first := GetOrUpdateSharedLimiter(key, 1024, 2048)
	second := GetOrUpdateSharedLimiter(key, 4096, 8192)

	if first != second {
		t.Fatal("expected default manager to share limiter for same key")
	}
	if defaultSharedManager.len() != 1 {
		t.Fatalf("default manager len = %d, want 1", defaultSharedManager.len())
	}
}

func TestBuildSharedKeyTrimsInput(t *testing.T) {
	got := BuildSharedKey(" 207.145.96.10 ", " user-a ")
	if got != "207.145.96.10|user-a" {
		t.Fatalf("key = %q, want 207.145.96.10|user-a", got)
	}
}

func TestSharedLimiterWrapPreservesReadWrite(t *testing.T) {
	limiter := newSharedLimiter(0, 0)
	rw := limiter.Wrap(&memoryReadWriter{
		read: bytes.NewBufferString("hello"),
	}, DirectionUpload, DirectionDownload)

	buf := make([]byte, 5)
	n, err := rw.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("read err: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("read = %q, want hello", string(buf[:n]))
	}

	n, err = rw.Write([]byte("world"))
	if err != nil {
		t.Fatalf("write err: %v", err)
	}
	if n != len("world") {
		t.Fatalf("write n = %d, want %d", n, len("world"))
	}
}

type memoryReadWriter struct {
	read  *bytes.Buffer
	write bytes.Buffer
}

func (rw *memoryReadWriter) Read(p []byte) (int, error) {
	return rw.read.Read(p)
}

func (rw *memoryReadWriter) Write(p []byte) (int, error) {
	return rw.write.Write(p)
}
