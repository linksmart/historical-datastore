package common

// Channel-based Notification System

type ChanNotifier struct {
	nChan chan Notification
}

// Notifier: Notification Channel
func NewNotifier() Notifier {
	return ChanNotifier{
		nChan: make(chan Notification), // unbuffered channel
	}
}

func (self ChanNotifier) Send(nt Notification) {
	self.nChan <- nt
}

func (self ChanNotifier) Receive() Notification {
	return <-self.nChan
}
