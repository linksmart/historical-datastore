package data

import (
	"fmt"

	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"
)

// Handles an incoming notification
func (d *ReadableAPI) ntListener(ntChan <-chan common.Notification) {
	for ntf := range ntChan {
		ds, ok := ntf.DS.(registry.DataSource)
		if !ok {
			fmt.Println("ntListener(): Bad notification!", ds)
			continue
		}
		switch ntf.TYPE {
		case common.CREATE:
			d.ntfCreated(&ds)
		case common.UPDATE_DATA:
			d.ntfUpdated(&ds)
		case common.DELETE:
			d.ntfDeleted(&ds)
		default:
			// other notifications
		}
	}
}

// Handles the creation of a new data source
func (d *ReadableAPI) ntfCreated(ds *registry.DataSource) {
	fmt.Println("created: ", ds.ID)
}

// Handles updates of a data source
func (d *ReadableAPI) ntfUpdated(ds *registry.DataSource) {
	fmt.Println("updated: ", ds.ID)
}

// Handles deletion of a data source
func (d *ReadableAPI) ntfDeleted(ds *registry.DataSource) {
	fmt.Println("deleted: ", ds.ID)
}
