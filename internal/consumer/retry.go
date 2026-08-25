package consumer

import (
	"time"
)

// retry 最多执行 fn n 次（含首次），失败时按 100ms*2^(i-1) 退避。
// 用于关闭/落盘路径的有界重试，避免在进程退出前数据滞留内存丢失。
func retry(n int, fn func() error) error {
	var err error
	for i := 0; i < n; i++ {
		if i > 0 {
			time.Sleep(time.Duration(1<<(i-1)) * 100 * time.Millisecond)
		}
		if err = fn(); err == nil {
			return nil
		}
	}
	return err
}
