package shishiodoshi

import (
	"fmt"
	"testing"
	"time"
)

func TestShishiOdoshi(t *testing.T) {
	ch := make(chan *int, 10)
	opt := DefaultOption[int]()

	opt.HandleTickerDelay = time.Second
	opt.LenTickerDelay = time.Second / 2
	opt.BufferMax = 10
	opt.BufferReportRatio = 0.5

	f := func(data []*int) {
	}
	bp, err := New[int](ch, f, opt)
	if err != nil {
		panic(err)
	}

	go bp.Run()

	defer bp.Stop()
	go func() {
		for {
			fmt.Println(bp.RunningState())
			time.Sleep(time.Second)
		}
	}()
	var i = 1
	for {
		a := i
		ch <- &a
		i++
		time.Sleep(5 * time.Millisecond)
	}

	time.Sleep(30 * time.Second)
}
