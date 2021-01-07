package sync

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/data"
)

type fallbackThread struct {
	// running is set to true if the fallback is running
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
	ctx    context.Context
	// cancel function to cancel any of the running gRPC communication whenever the destination needs to be deleted
	cancel context.CancelFunc
}
type Dst struct {
	// dstLastTS is the time corresponding to the latest record in the destionation
	lastTS time.Time
	// ctx is the context passed to gRPC Calls
	ctx context.Context
	// cancel function to cancel any of the running gRPC communication whenever the destination needs to be deleted
	cancel context.CancelFunc
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
	// fallback holds the information related to the fallback thread
	fallbackThread fallbackThread
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
		fallbackThread: fallbackThread{
			running: false,
			mutex:   &sync.Mutex{},
		},
	}
	s.src.ctx, s.src.cancel = context.WithCancel(context.Background())
	s.dst.ctx, s.dst.cancel = context.WithCancel(context.Background())
	go s.synchronize()
	return s
}

func (s Synchronization) synchronize() {
	if s.interval == 0 {
		for {
			s.subscribeAndPublish()
			time.Sleep(time.Second)
			log.Println("retrying subscription")
		}
	} else {
		for {
			s.periodicSynchronization()
			time.Sleep(s.interval)
		}
	}

}

func (s Synchronization) subscribeAndPublish() {
	// get the latest measurement from source
	var err error
	s.src.lastTS, err = getLastTime(s.src.client, s.series, time.Time{}, time.Now())
	if err != nil {
		log.Printf("error getting latest measurement:%v", err)
		return
	}

	//subscribe to source HDS
	responseCh, err := s.src.client.Subscribe(s.src.ctx, s.series)
	log.Printf("Success subscribing to source")
	if err != nil {
		log.Printf("error subscribing:%v", err)
		return
	}

	for response := range responseCh {
		if response.Err != nil {
			log.Printf("error while recieving stream: %v", response.Err)
			return
		}
		pack := response.Pack
		latestInPack := getLatestInPack(pack)
		if s.dst.lastTS.Equal(s.src.lastTS) == false {
			log.Printf("src and destination time (%v vs %v) do not match. starting fallback until %v", s.src.lastTS, s.dst.lastTS, latestInPack)
			go s.fallback(s.dst.lastTS, latestInPack)
			continue
		}
		log.Printf("copying %d entries to destination", len(pack))
		err = s.dst.client.Submit(s.dst.ctx, pack)
		if err != nil {
			log.Printf("Error copying entries : %v", err)
		} else {
			s.dst.lastTS = latestInPack
		}

		s.src.lastTS = latestInPack

	}

}

func (s Synchronization) periodicSynchronization() {
	var err error
	s.src.lastTS, err = getLastTime(s.src.client, s.series, time.Time{}, time.Now())
	if err != nil {
		log.Printf("error getting latest measurement:%v", err)
		return
	}
	//get last time from Src HDS
	q := data.Query{
		Denormalize: data.DenormMaskName | data.DenormMaskTime | data.DenormMaskUnit,
		SortAsc:     false,
		From:        s.dst.lastTS,
	}
	inputSeries := []string{s.series}
	pack, err := s.src.client.Query(s.src.ctx, inputSeries, q)
	if err != nil {
		log.Printf("error subscribing:%v", err)
		return
	}
	latestInPack := pack[0].Time
	log.Printf("copying %d entries to destination", len(pack))
	err = s.dst.client.Submit(s.dst.ctx, pack)
	if err != nil {
		log.Printf("Error copying entries : %v", err)
	} else {
		s.dst.lastTS = data.FromSenmlTime(latestInPack)
	}

}

func getLastTime(client *data.GrpcClient, series string, from time.Time, to time.Time) (time.Time, error) {
	pack, err := client.Query(context.Background(), []string{series}, data.Query{From: from, To: to, Limit: 1, SortAsc: false})
	if err != nil {
		return time.Time{}, err
	}
	if len(pack) != 1 {
		return to, nil
	}
	return data.FromSenmlTime(pack[0].Time), err
}

func (s Synchronization) fallback(from time.Time, to time.Time) {
	//fallback is supposed to run only once
	s.fallbackThread.mutex.Lock()
	if s.fallbackThread.running {
		s.fallbackThread.mutex.Unlock()
		return
	}
	s.fallbackThread.running = true
	s.fallbackThread.mutex.Unlock()

	defer func() {
		s.fallbackThread.mutex.Lock()
		s.fallbackThread.running = false
		s.fallbackThread.mutex.Unlock()
	}()

	destLatest, err := getLastTime(s.dst.client, s.series, from, to)
	if err != nil {
		log.Printf("Error getting the last timestamp: %v", err)
		return
	}
	log.Printf("Last timestamp: %s", destLatest)

	if to.Equal(destLatest) {
		log.Printf("Skipping fallback as the destination is already updated for stream %s", s.series)
	} else if to.Before(destLatest) {
		log.Println("destination is ahead of source. Should not have happened!!")
	} else {
		log.Printf("Starting fallback for destination, series: %s, dest latest: %v, to:%v", s.series, destLatest, to)
	}
	ctx := s.dst.ctx
	destStream, err := s.dst.client.CreateSubmitStream(ctx)
	if err != nil {
		log.Printf("Error getting the stream: %v", err)
		return
	}

	defer s.dst.client.CloseSubmitStream(destStream)

	sourceChannel, err := s.src.client.QueryStream(ctx, []string{s.series}, data.Query{From: destLatest, To: to.Add(time.Second), SortAsc: true})
	if err != nil {
		log.Printf("Error querying the source: %v", err)
		return
	}
	for response := range sourceChannel {
		if response.Err != nil {
			log.Printf("Breaking as there was error while recieving stream: %v", response.Err)
			break
		}
		err = s.dst.client.SubmitToStream(destStream, response.Pack)
		if err != nil {
			log.Printf("Breaking as there was error while submitting stream: %v", err)
			break
		}
		s.dst.lastTS = getLatestInPack(response.Pack)

	}
	log.Printf("done with fallback. destination latest: %v", s.dst.lastTS)

}

func getLatestInPack(pack senml.Pack) time.Time {
	//Since it is not assured that the pack will be sorted, we search exhaustively to find the latest
	latestInPack := pack[0].Time
	for _, r := range pack {
		if r.Time > latestInPack {
			latestInPack = r.Time
		}
	}
	return data.FromSenmlTime(latestInPack)
}
