package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/gorilla/websocket"
	"github.com/wcharczuk/go-diskq"
)

var (
	blueskyFeedURL   = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
	commitDataPath   = os.ExpandEnv("$HOME/data/bs-feed/commits")
	identityDataPath = os.ExpandEnv("$HOME/data/bs-feed/identity")
)

func main() {
	runRecover(func() {
		ctx := context.Background()

		slog.Info("dialing feed url", "url", blueskyFeedURL)
		con, _ := must2(websocket.DefaultDialer.Dial(blueskyFeedURL, http.Header{}))

		slog.Info("opening diskq buffer for commits", "path", commitDataPath)
		commits := must(diskq.New(commitDataPath, diskq.Options{
			SegmentSizeBytes: 32 * 1024 * 1024, // 32mb
			PartitionCount:   256,
			RetentionMaxAge:  24 * time.Hour,
		}))
		defer commits.Close()

		slog.Info("opening diskq buffer for identity", "path", identityDataPath)
		identity := must(diskq.New(identityDataPath, diskq.Options{
			SegmentSizeBytes: 32 * 1024 * 1024, // 32mb
			PartitionCount:   1,                // there are hilariously infrequent events here
			RetentionMaxAge:  24 * time.Hour,
		}))
		defer identity.Close()

		go func() {
			for range time.Tick(30 * time.Second) {
				if err := commits.Vacuum(); err != nil {
					slog.Error("vacuum error for commits diskq", "err", err)
					continue
				}
				stats, _ := diskq.GetStats(commitDataPath)
				slog.Info("commits vacuumed successfully", "size", formatSizeBytes(stats.SizeBytes), "offsets", stats.TotalOffsets, "age", stats.Age.Round(time.Second))
			}
		}()
		go func() {
			for range time.Tick(60 * time.Second) {
				if err := identity.Vacuum(); err != nil {
					slog.Error("vacuum error for identity diskq", "err", err)
					continue
				}
				stats, _ := diskq.GetStats(identityDataPath)
				slog.Info("identity vacuumed successfully", "size", formatSizeBytes(stats.SizeBytes), "offsets", stats.TotalOffsets, "age", stats.Age.Round(time.Second))
			}
		}()

		rsc := &events.RepoStreamCallbacks{
			RepoIdentity: func(evt *atproto.SyncSubscribeRepos_Identity) error {
				_, _, pushErr := identity.Push(diskq.Message{
					PartitionKey: evt.Did,
					Data:         marshal(evt),
				})
				return pushErr
			},
			RepoCommit: func(evt *atproto.SyncSubscribeRepos_Commit) error {
				_, _, pushErr := commits.Push(diskq.Message{
					PartitionKey: evt.Repo,
					Data:         marshal(evt),
				})
				return pushErr
			},
		}
		sched := sequential.NewScheduler("myfirehose", rsc.EventHandler)
		void(events.HandleRepoStream(ctx, con, sched))
	})
}

var gigabyte = 1 << 30
var megabyte = 1 << 20
var kilobyte = 1 << 10

func formatSizeBytes(size uint64) string {
	if size > uint64(gigabyte) {
		return fmt.Sprintf("%dGiB", size/uint64(gigabyte))
	}
	if size > uint64(megabyte) {
		return fmt.Sprintf("%dMiB", size/uint64(megabyte))
	}
	if size > uint64(kilobyte) {
		return fmt.Sprintf("%dKiB", size/uint64(kilobyte))
	}
	return fmt.Sprintf("%dB", size)
}

func marshal(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

func runRecover(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "%+v", r)
			os.Exit(1)
		}
	}()
	fn()
}

func void(err error) {
	if err != nil {
		panic(err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func must2[T0, T1 any](v0 T0, v1 T1, err error) (T0, T1) {
	if err != nil {
		panic(err)
	}
	return v0, v1
}
