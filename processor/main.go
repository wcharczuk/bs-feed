package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/wcharczuk/go-diskq"
)

var (
	commitsDataPath  = os.ExpandEnv("$HOME/data/bs-feed/commits")
	identityDataPath = os.ExpandEnv("$HOME/data/bs-feed/identity")
)

func main() {
	runRecover(func() {
		commits := ret(diskq.OpenConsumerGroup(commitsDataPath, diskq.ConsumerGroupOptions{
			OptionsForConsumer: func(_ uint32) (diskq.ConsumerOptions, error) {
				return diskq.ConsumerOptions{
					StartBehavior: diskq.ConsumerStartBehaviorOldest,
					EndBehavior:   diskq.ConsumerEndBehaviorWait,
				}, nil
			},
		}))
		defer commits.Close()

		identity := ret(diskq.OpenConsumerGroup(identityDataPath, diskq.ConsumerGroupOptions{
			OptionsForConsumer: func(_ uint32) (diskq.ConsumerOptions, error) {
				return diskq.ConsumerOptions{
					StartBehavior: diskq.ConsumerStartBehaviorOldest,
					EndBehavior:   diskq.ConsumerEndBehaviorWait,
				}, nil
			},
		}))
		defer identity.Close()

		ctx := context.Background()
		for {
			select {
			case msg, ok := <-commits.Messages():
				if !ok {
					return
				}
				evt := decode[atproto.SyncSubscribeRepos_Commit](msg.Data)
				handleEvent(ctx, evt)
			case err, ok := <-commits.Errors():
				if !ok {
					return
				}
				slog.Error("commits error", "err", err)
			case msg, ok := <-identity.Messages():
				if !ok {
					return
				}
				evt := decode[atproto.SyncSubscribeRepos_Identity](msg.Data)
				fmt.Printf("%#v\n", evt)
			case err, ok := <-identity.Errors():
				if !ok {
					return
				}
				slog.Error("identity error", "err", err)
			}
		}
	})
}

func handleEvent(ctx context.Context, evt atproto.SyncSubscribeRepos_Commit) {
	rr := saferet(repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks)))
	if rr == nil {
		return
	}
	for _, op := range evt.Ops {
		ek := repomgr.EventKind(op.Action)
		switch ek {
		case repomgr.EvtKindCreateRecord, repomgr.EvtKindUpdateRecord:
			_, rec := saferet2(rr.GetRecord(ctx, op.Path))
			if rec == nil {
				return
			}
			banana := lexutil.LexiconTypeDecoder{
				Val: rec,
			}
			var post bsky.FeedPost
			b := ret(banana.MarshalJSON())
			void(json.Unmarshal(b, &post))
			if post.LexiconTypeID == "app.bsky.feed.post" {
				fmt.Printf("%v: %v\n", evt.Repo, post.Text)
			}
		}
	}
}

func decode[T any](data []byte) (out T) {
	_ = json.Unmarshal(data, &out)
	return
}

func void(err error) {
	if err != nil {
		panic(err)
	}
}

func safe(_ error) {}

func ret[A any](v A, err error) A {
	if err != nil {
		panic(err)
	}
	return v
}

func saferet[A any](v A, _ error) A {
	return v
}

func ret2[A, B any](v0 A, v1 B, err error) (A, B) {
	if err != nil {
		panic(err)
	}
	return v0, v1
}

func saferet2[A, B any](v0 A, v1 B, _ error) (A, B) {
	return v0, v1
}

func runRecover(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "%+v\n", r)
			os.Exit(1)
		}
	}()
	fn()
}
