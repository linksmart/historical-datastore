package data

import (
	"fmt"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"
)

func (d *DataAPI) ntListener(ntChan chan common.Notification) {
	for ntf := range ntChan { // blocks unless there is a notification
		switch ntf.TYPE {
		case common.CREATE:
			d.ntfCreated(ntf.DS.(*registry.DataSource))

		case common.UPDATE_DATA:
			d.ntfUpdated(ntf.DS.(*registry.DataSource))

		case common.DELETE:
			d.ntfDeleted(ntf.DS.(*string))

		default:
			fmt.Println("ntListener(): Invalid nt operation!")

		}
	}
}

// Handles the creation of a new data source
func (d *DataAPI) ntfCreated(DS *registry.DataSource) {
	fmt.Println("ds created...")
}

// Handles updates of a data source
func (d *DataAPI) ntfUpdated(DS *registry.DataSource) {
	fmt.Println("ds updated...")
}

// Handles deletion of a data source
func (d *DataAPI) ntfDeleted(ID *string) {
	fmt.Println("ds deleted...")
}
