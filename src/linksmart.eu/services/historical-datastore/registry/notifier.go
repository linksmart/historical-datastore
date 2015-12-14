package registry

import (
	"log"

	"linksmart.eu/services/historical-datastore/common"
)

// Sends a Notification{} to channel
func sendNotification(payload interface{}, ntType common.NotificationType, ntChan chan<- common.Notification) chan error {
	if ntChan == nil {
		log.Panicln("Notification channel not initialized!")
	}
	clbk := make(chan error)
	ntChan <- common.Notification{Type: ntType, Payload: payload, Callback: clbk}
	return clbk
}
