package common

// Inteface for notification system: go-channel, rpc, etc
type Notifier interface {
	// Send a notification
	Send(nt Notification)
	// Blocking receive of norification
	Receive() Notification
}

// Operations that are notified
const (
	CREATED uint8 = iota
	UPDATED
	DELETED
)

// A single notification message
type Notification struct {
	ID string
	OP uint8 // the operation that is done
}
