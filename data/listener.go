package data

import (
	"fmt"
	"linksmart.eu/services/historical-datastore/common"
)

func (d *DataAPI) ntListener() {
	for {
		nt := d.notifier.Receive() // blocks unless there is a notification
		switch nt.OP {
		case common.CREATED:
			d.ntCreated(nt.ID)
		case common.UPDATED:
			d.ntUpdated(nt.ID)
		case common.DELETED:
			d.ntDeleted(nt.ID)
		default:
			fmt.Println("ntListener(): Invalid nt operation!")
		}
	}
}

// Handles the creation of a new data source
func (d *DataAPI) ntCreated(id string) {
	fmt.Println("ds created...")
}

// Handles updates of a data source
func (d *DataAPI) ntUpdated(id string) {
	fmt.Println("ds updated...")
}

// Handles deletion of a data source
func (d *DataAPI) ntDeleted(id string) {
	fmt.Println("ds deleted...")
}
