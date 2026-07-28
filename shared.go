package myratelimit

import (
	"io"
	"strings"
	"sync"

	"github.com/juju/ratelimit"
)

const (
	DirectionUpload Direction = iota + 1
	DirectionDownload

	maxSharedBucketWaitBytes int64 = 256 * 1024
)

type Direction int

type sharedManager struct {
	mu       sync.RWMutex
	limiters map[string]*SharedLimiter
}

type SharedLimiter struct {
	mu               sync.RWMutex
	uploadBytesSec   int64
	downloadBytesSec int64
	uploadBucket     *ratelimit.Bucket
	downloadBucket   *ratelimit.Bucket
}

type sharedRateLimitedReadWriter struct {
	io.ReadWriter
	limiter        *SharedLimiter
	readDirection  Direction
	writeDirection Direction
}

// defaultSharedManager 是进程内默认共享限速管理器。
//
// 调用方应通过 GetOrUpdateSharedLimiter 使用该单例，避免各模块独立创建
// 管理器后导致同一个业务 key 无法共享同一个限速池。
var defaultSharedManager = newSharedManager()

func newSharedManager() *sharedManager {
	return &sharedManager{
		limiters: make(map[string]*SharedLimiter),
	}
}

// BuildSharedKey 根据多个业务维度生成共享限速池 key。
//
// parts: 组成共享限速维度的字段，例如入口 IP、用户 ID、用户名等。
// 空格会被裁剪，最终用 "|" 连接。相同 key 会复用同一个共享限速池。
func BuildSharedKey(parts ...string) string {
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		items = append(items, strings.TrimSpace(part))
	}
	return strings.Join(items, "|")
}

// GetOrUpdateSharedLimiter 使用默认单例获取或更新共享限速器。
//
// key: 共享限速池的唯一标识，建议由 BuildSharedKey 生成。
// uploadBytesSec: 上传方向带宽上限，单位 Byte/s；<= 0 表示上传方向不限速。
// downloadBytesSec: 下载方向带宽上限，单位 Byte/s；<= 0 表示下载方向不限速。
//
// 同一个 key 的多条连接会拿到同一个 SharedLimiter，从而共享同一个带宽池。
// 如果后续同一个 key 传入新的上下行限速值，会复用原限速器并更新限速配置。
func GetOrUpdateSharedLimiter(key string, uploadBytesSec, downloadBytesSec int64) *SharedLimiter {
	return defaultSharedManager.getOrUpdate(key, uploadBytesSec, downloadBytesSec)
}

func (m *sharedManager) getOrUpdate(key string, uploadBytesSec, downloadBytesSec int64) *SharedLimiter {
	m.mu.RLock()
	limiter := m.limiters[key]
	m.mu.RUnlock()
	if limiter != nil {
		limiter.Update(uploadBytesSec, downloadBytesSec)
		return limiter
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	limiter = m.limiters[key]
	if limiter == nil {
		limiter = newSharedLimiter(uploadBytesSec, downloadBytesSec)
		m.limiters[key] = limiter
		return limiter
	}
	limiter.Update(uploadBytesSec, downloadBytesSec)
	return limiter
}

func (m *sharedManager) len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.limiters)
}

func newSharedLimiter(uploadBytesSec, downloadBytesSec int64) *SharedLimiter {
	limiter := &SharedLimiter{}
	limiter.Update(uploadBytesSec, downloadBytesSec)
	return limiter
}

// Update 更新共享限速器的上下行限速值。
//
// uploadBytesSec 和 downloadBytesSec 单位均为 Byte/s；<= 0 表示对应方向不限速。
func (l *SharedLimiter) Update(uploadBytesSec, downloadBytesSec int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.uploadBytesSec != uploadBytesSec {
		l.uploadBytesSec = uploadBytesSec
		l.uploadBucket = newSharedBucket(uploadBytesSec)
	}
	if l.downloadBytesSec != downloadBytesSec {
		l.downloadBytesSec = downloadBytesSec
		l.downloadBucket = newSharedBucket(downloadBytesSec)
	}
}

func (l *SharedLimiter) UploadBytesSec() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.uploadBytesSec
}

func (l *SharedLimiter) DownloadBytesSec() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.downloadBytesSec
}

// Wrap 将连接或流包装为受共享限速器控制的 io.ReadWriter。
//
// rw: 原始连接或转发流。
// readDirection: rw.Read 读取到的数据应该消耗的共享限速方向。
// writeDirection: rw.Write 写出的数据应该消耗的共享限速方向。
//
// 对客户端连接来说，通常 Read 是上传方向、Write 是下载方向；对远端连接
// 来说，方向通常相反。调用方应根据实际数据流方向传入对应参数。
func (l *SharedLimiter) Wrap(rw io.ReadWriter, readDirection, writeDirection Direction) io.ReadWriter {
	if l == nil {
		return rw
	}
	return &sharedRateLimitedReadWriter{
		ReadWriter:     rw,
		limiter:        l,
		readDirection:  readDirection,
		writeDirection: writeDirection,
	}
}

func (rw *sharedRateLimitedReadWriter) Read(p []byte) (int, error) {
	n, err := rw.ReadWriter.Read(p)
	rw.limiter.wait(rw.readDirection, n)
	return n, err
}

func (rw *sharedRateLimitedReadWriter) Write(p []byte) (int, error) {
	rw.limiter.wait(rw.writeDirection, len(p))
	return rw.ReadWriter.Write(p)
}

func (l *SharedLimiter) wait(direction Direction, n int) {
	if n <= 0 {
		return
	}

	bucket := l.bucket(direction)
	if bucket == nil {
		return
	}
	for remaining := int64(n); remaining > 0; {
		waitBytes := remaining
		if waitBytes > maxSharedBucketWaitBytes {
			waitBytes = maxSharedBucketWaitBytes
		}
		bucket.Wait(waitBytes)
		remaining -= waitBytes
	}
}

func (l *SharedLimiter) bucket(direction Direction) *ratelimit.Bucket {
	l.mu.RLock()
	defer l.mu.RUnlock()
	switch direction {
	case DirectionUpload:
		return l.uploadBucket
	case DirectionDownload:
		return l.downloadBucket
	default:
		return nil
	}
}

func newSharedBucket(bytesSec int64) *ratelimit.Bucket {
	if bytesSec <= 0 {
		return nil
	}
	burst := bytesSec
	if burst < maxSharedBucketWaitBytes {
		burst = maxSharedBucketWaitBytes
	}
	return ratelimit.NewBucketWithRate(float64(bytesSec), burst)
}
