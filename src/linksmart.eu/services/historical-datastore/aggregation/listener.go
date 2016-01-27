package aggregation

import (
	"fmt"

	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"
)

// Handles an incoming notification
func ntListener(a Aggr, ntChan <-chan common.Notification) {
	for ntf := range ntChan {
		switch ntf.Type {
		case common.CREATE:
			ds, ok := ntf.Payload.(registry.DataSource)
			if !ok {
				fmt.Println("ntListener() create: Bad notification!", ds)
				continue
			}
			a.ntfCreated(ds, ntf.Callback)
		case common.UPDATE:
			dss, ok := ntf.Payload.([]registry.DataSource)
			if !ok || len(dss) < 2 {
				fmt.Println("ntListener() update: Bad notification!", dss)
				continue
			}
			a.ntfUpdated(dss[0], dss[1], ntf.Callback)
		case common.DELETE:
			ds, ok := ntf.Payload.(registry.DataSource)
			if !ok {
				fmt.Println("ntListener() delete: Bad notification!", ds)
				continue
			}
			a.ntfDeleted(ds, ntf.Callback)
		default:
			// other notifications
		}
	}
}
