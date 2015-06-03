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

// Constructs notifier and starts multicasting
func StartNotifier(in *chan Notification, out ...chan<- Notification) {

	// Multicasts sender messages to receivers
	go func() {
		for n := range *in {
			// forward notification to all readers
			for _, r := range out {
				r <- n
			}
		}
	}()

	// Open the sender
	*in = make(chan Notification)
}
