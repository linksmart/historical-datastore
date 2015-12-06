package data

import (
	"fmt"

	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"
)

// Handles an incoming notification
func (s *influxStorage) ntListener(ntChan <-chan common.Notification) {
	for ntf := range ntChan {
		ds, ok := ntf.DS.(registry.DataSource)
		if !ok {
			fmt.Println("ntListener(): Bad notification!", ds)
			continue
		}
		switch ntf.TYPE {
		case common.CREATE:
			s.ntfCreated(&ds)
		case common.UPDATE_DATA:
			s.ntfUpdated(&ds)
		case common.DELETE:
			s.ntfDeleted(&ds)
		default:
			// other notifications
		}
	}
}
