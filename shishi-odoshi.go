package shishiodoshi

import (
	"context"
	"errors"
	"time"
)

const (
	defaultBufferMax         = 5000             // 默认缓冲区最大值
	defaultBufferReportRatio = 0.5              // 默认触发行为的缓冲区铲毒系数 （0,1]
	defaultLenTickerDelay    = time.Second      // 默认长度检测时间间隔
	defaultHandleTickerDelay = 10 * time.Second // 默认行为触发时间间隔
	defaultPerHandleMax      = 1000             // 每组处理的数量上限
)

// Option 设置参数
// 最大处理速度 每秒检测次数 * 最大缓冲区长度， (time.Second/Option.LenTickerDelay) * Option.BufferMax
type Option[T any] struct {
	BufferMax         int             // 缓冲区最大值
	BufferReportRatio float64         // 触发行为缓冲区长度系数,范围（0,1]，当缓冲区中中数据的长度到达 BufferMax 的一定比例时，触发事件行为
	LenTickerDelay    time.Duration   // 长度检查时间间隔
	HandleTickerDelay time.Duration   // 触发行为的时间间隔，当数据量没有达到 BufferMax*BufferReportRatio 时，也会触发事件行为
	PerHandleMax      int             // 每组处理的数量上限
	Filters           []func(*T) bool // 筛选器，筛选出不需要的数据
}

func (opt Option[T]) validate() error {
	if opt.BufferMax <= 0 {
		return errors.New("BufferMax must greater than 0")
	}
	if opt.BufferReportRatio > 1 {
		return errors.New("BufferReportRatio  must less then 1")
	}

	return nil
}

// DefaultOption 可以通过 DefaultOption 方法获取到默认的参数，然后重新赋值需要变更的参数
func DefaultOption[T any]() Option[T] {
	return Option[T]{
		BufferMax:         defaultBufferMax,
		BufferReportRatio: defaultBufferReportRatio,
		LenTickerDelay:    defaultLenTickerDelay,
		HandleTickerDelay: defaultHandleTickerDelay,
		PerHandleMax:      defaultPerHandleMax,
	}
}

type ShishiOdoshi[T any] struct {
	cancel    context.CancelFunc
	isRunning bool

	bufferStorage chan *T
	channel       <-chan *T
	option        Option[T]
	handler       func([]*T)
	filters       []func(*T) bool // 筛选器，筛选出不需要的数据

	// 数据统计
	receiveCount int64 // 接收数量
	storageCount int64 // 缓存数量
	dropCount    int64 // 丢弃数量
	handleCount  int64 // 处理数量
}

func New[T any](channel <-chan *T, handler func([]*T), opt Option[T]) (*ShishiOdoshi[T], error) {
	if err := opt.validate(); err != nil {
		return nil, err
	}

	if channel == nil {
		return nil, errors.New("invalid channel")
	}

	if handler == nil {
		return nil, errors.New("invalid handler")
	}
	bufferStorage := make(chan *T, opt.BufferMax)
	return &ShishiOdoshi[T]{
		bufferStorage: bufferStorage,
		channel:       channel,
		option:        opt,
		handler:       handler,
		filters:       opt.Filters,
	}, nil
}

func (bp *ShishiOdoshi[T]) Run() {
	if bp.isRunning {
		return
	}
	bp.isRunning = true
	defer func() { bp.isRunning = false }()
	lenTicker := time.NewTicker(bp.option.LenTickerDelay)
	defer lenTicker.Stop()
	handleTicker := time.NewTicker(bp.option.HandleTickerDelay)
	defer handleTicker.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	bp.cancel = cancel
	defer cancel()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-handleTicker.C:
				// 定时发送
				length := len(bp.bufferStorage)
				if length != 0 {
					bp.handleAsGroup(length)
				}
			case <-lenTicker.C:
				length := len(bp.bufferStorage)
				// 检查队列长度，到达缓冲区阈值的时候外发
				if length >= int(float64(bp.option.BufferMax)*bp.option.BufferReportRatio) {
					bp.handleAsGroup(length)
					// 重置定时发送时间
					handleTicker.Reset(bp.option.HandleTickerDelay)
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-bp.channel:
			if !ok {
				return
			}
			bp.receiveCount++
			for _, f := range bp.filters {
				if !f(v) {
					continue
				}
			}

			// 丢入缓冲区，防止发送时阻塞接收，如果缓冲区已满，则丢弃
			select {
			case bp.bufferStorage <- v:
				bp.storageCount++
			default:
				bp.dropCount++
			}
		}
	}
}

func (bp *ShishiOdoshi[T]) Stop() {
	defer func() { bp.isRunning = false }()
	if bp.isRunning && bp.cancel != nil {
		bp.cancel()
	}
}

func (bp *ShishiOdoshi[T]) handleAsGroup(length int) {
	for i := 0; i < length/bp.option.PerHandleMax; i++ {
		data := make([]*T, 0, bp.option.PerHandleMax)
		for j := 0; j < bp.option.PerHandleMax; j++ {
			bp.handleCount++
			data = append(data, <-bp.bufferStorage)
		}
		bp.handler(data)
	}
	if length%bp.option.PerHandleMax != 0 {
		data := make([]*T, 0, length%bp.option.PerHandleMax)
		for j := 0; j < length%bp.option.PerHandleMax; j++ {
			bp.handleCount++
			data = append(data, <-bp.bufferStorage)
		}
		bp.handler(data)
	}
}

func (bp *ShishiOdoshi[T]) RunningState() (receiveCount, storageCount, dropCount, handleCount int64) {
	return bp.receiveCount, bp.storageCount, bp.dropCount, bp.handleCount
}

func (bp *ShishiOdoshi[T]) IsRunning() bool {
	return bp.isRunning
}
