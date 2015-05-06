package common

// Supported notification types
const (
	CREATE uint8 = iota
	DELETE
	UPDATE_DATA
	UPDATE_AGGR
)

// A notification message
type Notification struct {
	TYPE uint8
	DS   interface{}
}

type Notifier struct {
	Sender  chan Notification
	readers []chan Notification
}

// Constructs notifier and starts multicasting
func SetupNotifier() *Notifier {
	nt := &Notifier{
		Sender: make(chan Notification), // send-only unbuffered channel
	}

	go nt.multicaster()

	return nt
}

// Multicasts sender messages to receivers
func (nt *Notifier) multicaster() {
	for n := range nt.Sender {
		// forward notification to all readers
		for _, r := range nt.readers {
			r <- n
		}
	}
}

// Create a new reader channel
func (nt *Notifier) NewReader() chan Notification {
	newReader := make(chan Notification) // receive-only unbuffered channel
	nt.readers = append(nt.readers, newReader)
	return newReader
}
