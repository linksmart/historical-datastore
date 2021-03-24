package sync

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/data"
)

type backfillThread struct {
	// running is set to true if the backfill is running
	running bool
	// mutex to protect the "running" variable
	mutex *sync.Mutex
}
type Src struct {
	// srcLastTS is the time corresponding to the latest record in the source
	lastTS time.Time
	// ctx is the context passed to gRPC Calls
	// client is the connection to the source host
	client *data.GrpcClient
}
type Dst struct {
	// dstLastTS is the time corresponding to the latest record in the destionation
	lastTS time.Time
	// client is the connection to the destination host
	client *data.GrpcClient
}
type Synchronization struct {
	// series to by synced
	series string
	// firstTS holds the starting time from which sync needs to start
	firstTS time.Time
	// interval in which the synchronization should happen. when 0, the synchronization will be continuous
	interval time.Duration
	// src holds the information related to the source series
	src Src
	//dst holds the information related to the destination series
	dst Dst
	// backfill holds the information related to the backfill thread
	backfillThread backfillThread

	// ctx is the context passed to gRPC Calls
	ctx context.Context
	// cancel function to cancel any of the running gRPC communication whenever the synchronization needs to be stopped
	cancel context.CancelFunc
}

func newSynchronization(series string, srcClient *data.GrpcClient, dstClient *data.GrpcClient, interval time.Duration) (s *Synchronization) {

	zeroTime := time.Time{}

	s = &Synchronization{
		series:   series,
		firstTS:  zeroTime, //TODO: This should come as an argument. But for the future
		interval: interval,
		src: Src{
			lastTS: zeroTime,
			client: srcClient,
		},
		dst: Dst{
			lastTS: zeroTime,
			client: dstClient,
		},
		backfillThread: backfillThread{
			running: false,
			mutex:   &sync.Mutex{},
		},
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	go s.synchronize()
	return s
}

// clear ensures graceful shutdown of the synchronization related to the series
func (s *Synchronization) clear() {
	s.cancel()
}

func (s *Synchronization) synchronize() {
	canceled := false
	if s.interval == 0 {
		for !canceled {
			s.subscribeAndPublish()
			canceled = sleepContext(s.ctx, time.Second)
		}
	} else {
		for !canceled {
			s.periodicSynchronization()
			canceled = sleepContext(s.ctx, s.interval)
		}
	}
}

func (s *Synchronization) subscribeAndPublish() {
	// get the latest measurement from source
	var err error
	s.src.lastTS, err = getLastTime(s.ctx, s.src.client, s.series, time.Time{}, time.Now())
	if err != nil {
		log.Printf("%s: error getting latest measurement from source:%v", s.series, err)
		return
	}

	//subscribe to source HDS
	responseCh, err := s.src.client.Subscribe(s.ctx, s.series)
	log.Printf("Success subscribing to source %s", s.series)
	if err != nil {
		log.Printf("%s: error subscribing to source: %v", s.series, err)
		return
	}

	for response := range responseCh {
		if response.Err != nil {
			log.Printf("%s: error while recieving stream: %v", s.series, response.Err)
			return
		}
		pack := response.Pack
		latestInPack := getLatestInPack(pack)
		if s.dst.lastTS.After(s.src.lastTS) == true {
			log.Printf("%s: src and destination time (%v vs %v) do not match. starting backfill until %v", s.series, s.src.lastTS, s.dst.lastTS, latestInPack)
			go s.backfill(s.dst.lastTS, latestInPack)
			continue
		}
		err = s.dst.client.Submit(s.ctx, pack)
		if err != nil {
			log.Printf("%s: error copying entries : %v", s.series, err)
		} else {
			log.Printf("%s: copied SenML pack of len %d to destination", s.series, len(pack))
			s.dst.lastTS = latestInPack
		}

		s.src.lastTS = latestInPack

	}

}

func (s *Synchronization) periodicSynchronization() {
	var err error
	s.src.lastTS, err = getLastTime(s.ctx, s.src.client, s.series, time.Time{}, time.Now())
	if err != nil {
		log.Printf("%s: unable to get latest source measurement%v", s.series, err)
		return
	}

	s.dst.lastTS, err = getLastTime(s.ctx, s.dst.client, s.series, time.Time{}, time.Now())
	if err != nil {
		log.Printf("%s: error getting latest destination measurement :%v", s.series, err)
		return
	}

	if s.dst.lastTS.Equal(s.src.lastTS) == false {
		go s.backfill(s.dst.lastTS, s.src.lastTS)
	}

}

func getLastTime(ctx context.Context, client *data.GrpcClient, series string, from time.Time, to time.Time) (time.Time, error) {
	pack, err := client.Query(ctx, []string{series}, data.Query{From: from, To: to, Limit: 1, SortAsc: false})
	if err != nil {
		return time.Time{}, fmt.Errorf("series:%s, error:%s", series, err)
	}
	if len(pack) != 1 {
		return to, nil
	}
	return data.FromSenmlTime(pack[0].Time), err
}

func (s *Synchronization) backfill(from time.Time, to time.Time) {
	//backfill is supposed to run only once
	s.backfillThread.mutex.Lock()
	if s.backfillThread.running {
		s.backfillThread.mutex.Unlock()
		return
	}
	s.backfillThread.running = true
	s.backfillThread.mutex.Unlock()

	defer func() {
		s.backfillThread.mutex.Lock()
		s.backfillThread.running = false
		s.backfillThread.mutex.Unlock()
	}()

	destLatest, err := getLastTime(s.ctx, s.dst.client, s.series, from, to)
	if err != nil {
		log.Printf("%s: error getting the last timestamp from dest: %s", s.series, err)
		return
	}
	log.Printf("%s: destLatest : %s", s.series, destLatest)

	if to.Equal(destLatest) {
		log.Printf("%s: skipping backfill as the destination is already updated", s.series)
	} else if to.Before(destLatest) {
		log.Printf("%s: destination is ahead of source. Should not have happened!!", s.series)
	} else {
		log.Printf("%s: starting backfill for destination, dest latest: %v, to:%v", s.series, destLatest, to)
	}
	ctx := s.ctx
	destStream, err := s.dst.client.CreateSubmitStream(ctx)
	if err != nil {
		log.Printf("%s: Error getting the stream: %v", s.series, err)
	}

	defer s.dst.client.CloseSubmitStream(destStream)
	//get last time from Src HDS
	q := data.Query{
		Denormalize: data.DenormMaskName | data.DenormMaskTime | data.DenormMaskUnit,
		SortAsc:     true,
		From:        destLatest,
		To:          to.Add(time.Second),
	}
	sourceChannel, err := s.src.client.QueryStream(ctx, []string{s.series}, q)
	if err != nil {
		log.Printf("%s: error querying the source: %v", s.series, err)
		return
	}
	totalSynced := 0
	for response := range sourceChannel {
		if response.Err != nil {
			log.Printf("%s: breaking backfill as there was error while recieving stream : %v", s.series, response.Err)
			break
		}
		err = s.dst.client.SubmitToStream(destStream, response.Pack)
		if err != nil {
			log.Printf("%s: breaking backfill as there was error while submitting stream %s: %v", s.series, err)
			break
		}
		s.dst.lastTS = getLatestInPack(response.Pack)
		totalSynced += len(response.Pack)

	}
	log.Printf("%s: migrated %d entries destination latest: %v", s.series, totalSynced, s.dst.lastTS)

}

func getLatestInPack(pack senml.Pack) time.Time {
	//Since it is not assured that the pack will be sorted, we search exhaustively to find the latest
	bt := pack[0].BaseTime
	latestInPack := bt + pack[0].Time
	for _, r := range pack {
		t := bt + r.Time
		if t > latestInPack {
			latestInPack = t
		}
	}
	return data.FromSenmlTime(latestInPack)
}

func sleepContext(ctx context.Context, delay time.Duration) (cancelled bool) {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(delay):
		return false
	}
}
