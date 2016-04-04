package registry

import (
	"fmt"

	"linksmart.eu/services/historical-datastore/common"
)

// Sends a Notification{} to channel
func sendNotification(payload interface{}, ntType common.NotificationType, ntChan chan<- common.Notification) error {
	if ntChan == nil {
		fmt.Println("WARNING: Notification channel not initialized! Notification will be ignored.")
		return nil
	}
	clbk := make(chan error, 2)
	ntChan <- common.Notification{Type: ntType, Payload: payload, Callback: clbk}
	for c := 0; c < common.Subscribers(); c++ {
		if err := <-clbk; err != nil {
			return err
		}
	}
	close(clbk)

	return nil
}
