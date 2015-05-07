package common

// Supported notification types
type NotificationTYPE uint8

const (
	CREATE NotificationTYPE = iota
	DELETE
	UPDATE_DATA
	UPDATE_AGGR
)

// A notification message
type Notification struct {
	TYPE NotificationTYPE
	DS   interface{}
}

type Notifier struct {
	sender  chan Notification
	readers []chan Notification
}

// Constructs notifier and starts multicasting
func NewNotifier(in chan Notification, out ...chan Notification) *Notifier {
	nt := &Notifier{
		sender:  in,
		readers: out,
	}
	go nt.multicaster()
	return nt
}

// Multicasts sender messages to receivers
func (nt *Notifier) multicaster() {
	for n := range nt.sender {
		// forward notification to all readers
		for _, r := range nt.readers {
			r <- n
		}
	}
}

// // Get sender channel as write only
// func (nt *Notifier) Sender() chan<- Notification {
// 	return nt.sender
// }

// // Create a new reader channel as receive-only
// func (nt *Notifier) NewReader() <-chan Notification {
// 	newReader := make(chan Notification) // unbuffered channel
// 	nt.readers = append(nt.readers, newReader)
// 	return newReader
// }
