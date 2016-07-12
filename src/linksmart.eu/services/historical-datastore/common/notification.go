// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package common

// Supported notification types
type NotificationType uint8

const (
	CREATE NotificationType = iota
	DELETE
	UPDATE
)

// A notification message
type Notification struct {
	Type     NotificationType
	Payload  interface{}
	Callback chan error
}

// Constructs notifier and starts multicasting
func StartNotifier(in *chan Notification, out ...chan<- Notification) {
	subscribers = len(out)

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

// Number of subscribers (= expected number of callbacks)
var subscribers int

func Subscribers() int {
	return subscribers
}
