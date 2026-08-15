package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rand"
	"pgregory.net/rapid"

	"github.com/VKCOM/statshouse/internal/data_model"
)

func TestCache2BucketList(t *testing.T) {
	s := [100]cache2Bucket{}
	l := newCache2BucketList()
	require.Equal(t, 0, l.len())
	for i := 0; i < len(s); i++ {
		l.add(&s[i])
		require.Equal(t, i+1, l.len())
	}
	for i := 0; i < len(s); i++ {
		l.remove(&s[i])
		require.Equal(t, len(s)-i-1, l.len())
	}
}

func TestCache2TrimBucketHeapMaxSize(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := make(cache2TrimBucketHeap, 0, 10)
		for i := 0; i < 10; i++ {
			h = h.push(cache2TrimBucket{info: cache2BucketRuntimeInfo{
				size: rapid.Int().Draw(t, "sizeInBytes"),
			}})
		}
		sizeInBytes := h.min().info.size
		for h.len() > 1 {
			h = h.pop()
			require.GreaterOrEqual(t, sizeInBytes, h.min().info.size)
			sizeInBytes = h.min().info.size
		}
		h = h.pop()
		require.Zero(t, h.len())
	})
}

func TestCache2TrimBucketHeapMinAccessTime(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := make(cache2TrimBucketHeap, 0, 1000)
		for i := 0; i < 1000; i++ {
			h = h.push(cache2TrimBucket{info: cache2BucketRuntimeInfo{
				idlePeriod: time.Duration(rapid.Int64().Draw(t, "lastAccessTime")),
			}})
		}
		lastAccessTime := h.min().info.idlePeriod
		for h.len() > 1 {
			h = h.pop()
			require.GreaterOrEqual(t, lastAccessTime, h.min().info.idlePeriod)
			lastAccessTime = h.min().info.idlePeriod
		}
		h = h.pop()
		require.Zero(t, h.len())
	})
}

func TestCache2TrimBucketHeapMaxPlay(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := make(cache2TrimBucketHeap, 0, 1000)
		for i := 0; i < 1000; i++ {
			h = h.push(cache2TrimBucket{info: cache2BucketRuntimeInfo{
				playInterval: time.Duration(rapid.Int64().Draw(t, "play")),
			}})
		}
		play := h.min().info.playInterval
		for h.len() > 1 {
			h = h.pop()
			require.GreaterOrEqual(t, play, h.min().info.playInterval)
			play = h.min().info.playInterval
		}
		h = h.pop()
		require.Zero(t, h.len())
	})
}

func TestCache2AddRemoveChunks(t *testing.T) {
	h := &requestHandler{
		Handler: &Handler{
			HandlerOptions: HandlerOptions{
				location: time.Local,
			},
		},
	}
	q := &queryBuilder{}
	c := newCache2(h.Handler, 0, nil)
	shard := c.shards[time.Second]
	info := cache2UpdateInfo{}
	b := shard.getOrCreateLockedBucket(h, q, &info)
	b.mu.Unlock()
	c.updateRuntimeInfo(shard.stepS, b.fau, &info)
	rapid.Check(t, func(t *rapid.T) {
		// add chunks
		lenG := rapid.IntRange(0, 32)
		timeNow := time.Now()
		timeG := rapid.Int64Range(timeNow.Add(-24*time.Hour).Unix(), timeNow.Unix())
		for i := 0; i < 512; i++ {
			start := timeG.Draw(t, "from sec")
			l := cache2Loader{
				cache:   c,
				handler: h,
				query:   q,
				shard:   shard,
				bucket:  b,
				lod: data_model.LOD{
					FromSec: start,
					ToSec:   start + int64(lenG.Draw(t, "chunk length")),
				},
			}
			info := cache2UpdateInfo{}
			l.init(&info)
			c.updateRuntimeInfo(shard.stepS, b.fau, &info)
		}
		// acccess chunks
		for _, chunk := range b.chunks {
			chunk.lastAccessTime = timeG.Draw(t, "chunk last access time")
		}
		// remove chunks
		for n := len(b.chunks); n != 0; n = len(b.chunks) {
			i := rapid.IntRange(0, n-1).Draw(t, "chunk index")
			info := cache2UpdateInfo{}
			b.removeChunksNotUsedAfterUnlocked(b.chunks[i].lastAccessTime+1, &info)
			c.updateRuntimeInfo(shard.stepS, b.fau, &info)
			require.Less(t, len(b.chunks), n)
		}
	})
	c.shutdown().Wait()
	require.Zero(t, c.info.size())
}

func TestCache2Parallel(t *testing.T) {
	h := &requestHandler{
		Handler: &Handler{
			HandlerOptions: HandlerOptions{
				location: time.Local,
			},
		},
	}
	c := newCache2(h.Handler, 0, func(_ context.Context, _ *requestHandler, _ *queryBuilder, _ data_model.LOD, ret [][]tsSelectRow, _ int) (int, error) {
		for i := 0; i < len(ret); i++ {
			ret[i] = make([]tsSelectRow, rand.Intn(2))
		}
		return len(ret), nil
	})
	c.setLimits(cache2Limits{maxSize: 32})
	requireCache2Valid(t, c)
	t.Run("parent", func(t *testing.T) {
		for i := 0; i < 128; i++ {
			t.Run("child", func(t *testing.T) {
				t.Parallel()
				q := &queryBuilder{
					play: rand.Intn(15),
				}
				for i := 0; i < 512; i++ {
					lod := cache2TestDrawLOD(c)
					_, err := c.Get(context.Background(), h, q, lod, false)
					require.NoError(t, err)
				}
			})
		}
	})
	c.shutdown().Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	requireCache2Valid(t, c)
	c.info.normalizeWaterLevel()
	c.info.resetAccessInfo()
	require.Equal(t, c.info, cache2RuntimeInfo{minChunkAccessTime: c.info.minChunkAccessTime})
}

// TestCache2AwaiterCopyOffset reproduces the awaiter data-misplacement bug in
// loadChunks(): a request that awaits an in-flight chunk load must receive its
// data at the right buffer offsets. The awaiter copy loop writes destination
// indices [a.loadStart, a.loadEnd) sourcing from chunkData[a.chunkOffset, ...).
func TestCache2AwaiterCopyOffset(t *testing.T) {
	h := &requestHandler{
		Handler: &Handler{
			HandlerOptions: HandlerOptions{
				location: time.Local,
			},
		},
	}
	const chunkSlots = 60

	// deterministic loader: stamp every slot with its offset from the chunk start
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	loader := func(_ context.Context, _ *requestHandler, _ *queryBuilder, _ data_model.LOD, ret [][]tsSelectRow, _ int) (int, error) {
		started <- struct{}{}
		<-release
		for i := 0; i < len(ret); i++ {
			ret[i] = []tsSelectRow{{tsValues: tsValues{sum: float64(i)}}}
		}
		return len(ret), nil
	}
	c := newCache2(h.Handler, 0, loader)
	shard := c.shards[time.Second] // 1s step, 60-slot chunks
	q := &queryBuilder{}

	// chunk-aligned base second for the query range
	base := c.chunkStart(shard, int64(1_700_000_000)*int64(time.Second))
	baseSec := base / int64(time.Second)

	// Request A loads the whole chunk; it blocks inside the loader.
	lodA := data_model.LOD{Version: Version6, StepSec: 1, FromSec: baseSec, ToSec: baseSec + chunkSlots}
	type aResult struct {
		res cache2Data
		err error
	}
	aDone := make(chan aResult, 1)
	go func() {
		res, err := c.Get(context.Background(), h, q, lodA, false)
		aDone <- aResult{res, err}
	}()
	<-started // A is now blocked inside the loader; chunk.loading == 1, chunk.data == nil

	// Request B needs a mid-chunk sub-range -> it must await A's in-flight load.
	// Building the loader (without running it) synchronously registers B as an awaiter.
	const off, tail = 10, 10
	lodB := data_model.LOD{Version: Version6, StepSec: 1, FromSec: baseSec + off, ToSec: baseSec + chunkSlots - tail}
	bL := c.newLoader(h, q, lodB, chunkSlots-off-tail, false, shard)
	require.Equal(t, off, bL.loadStart)
	require.Equal(t, chunkSlots-tail, bL.loadEnd)
	bWaitDone := make(chan error, 1)
	go func() { bWaitDone <- bL.wait(context.Background()) }()

	close(release) // let A finish loading; loadChunks copies data into the awaiter (B)
	<-bWaitDone    // B's awaiter copy + send completed -> bL.data is populated
	a := <-aDone
	require.NoError(t, a.err)

	// A sees the whole chunk, in order.
	require.Len(t, a.res, chunkSlots)
	for i := 0; i < len(a.res); i++ {
		require.Len(t, a.res[i], 1, "A slot %d", i)
		require.Equal(t, float64(i), a.res[i][0].sum, "A slot %d", i)
	}

	// B must see its sub-range [off, chunkSlots-tail) at the right offsets and values.
	bRes := bL.data[bL.loadStart:bL.loadEnd]
	require.Len(t, bRes, chunkSlots-off-tail)
	for m := 0; m < len(bRes); m++ {
		idx := bL.loadStart + m
		require.Len(t, bRes[m], 1, "B buffer idx %d should have 1 row (gap?)", idx)
		require.Equal(t, float64(idx), bRes[m][0].sum,
			"B buffer idx %d (awaiter slot %d): wrong value/shifted timestamp", idx, m)
	}
	c.shutdown().Wait()
}

func cache2TestDrawLOD(c *cache2) data_model.LOD {
	n := rand.Intn(len(c.shards) - 1)
	var shard *cache2Shard
	for _, v := range c.shards {
		if v.step != timeMonth && n <= 0 {
			shard = v
			break
		}
		n--
	}
	start := rand.Int63n(time.Now().UnixNano())
	start = (start / int64(shard.step)) * int64(shard.step) // align
	end := start + int64(shard.step)*rand.Int63n(10)
	return data_model.LOD{
		Version: Version6,
		StepSec: int64(shard.step / time.Second),
		FromSec: start / int64(time.Second),
		ToSec:   end / int64(time.Second),
	}
}

func requireCache2Valid(t *testing.T, c *cache2) {
	for _, shard := range c.shards {
		requireCache2ShardValid(t, shard)
	}
}

func requireCache2ShardValid(t *testing.T, shard *cache2Shard) {
	for _, b := range shard.bucketM {
		requireCache2BucketValid(t, b)
	}
}

func requireCache2BucketValid(t *testing.T, b *cache2Bucket) {
	require.Equal(t, len(b.times), len(b.chunks))
	if len(b.times) == 0 {
		return
	}
	require.Equal(t, b.times[0], b.chunks[0].start)
	for i := 1; i < len(b.times); i++ {
		require.Less(t, b.times[i-1], b.times[i])
		require.Equal(t, b.times[i], b.chunks[i].start)
	}
}
