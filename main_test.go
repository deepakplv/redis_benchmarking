package main

import (
	"testing"
)

func BenchmarkBulkPublisher(b *testing.B) {
	taskQueue := createTaskQueue()

	b.ResetTimer()
        for n := 0; n < b.N; n++ {
                BulkPublisher(taskQueue)
        }
}
