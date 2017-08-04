// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"runtime/debug"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/registry"
)

// Handles an incoming notification
func NtfListener(s Storage, ntChan <-chan common.Notification) {
	for ntf := range ntChan {
		defer func() {
			if r := recover(); r != nil {
				logger.Printf("Recovered from panic: %v\n%v", r, string(debug.Stack()))
				ntf.Callback <- logger.Errorf("panic: %v", r)
			}
		}()
		switch ntf.Type {
		case common.CREATE:
			ds, ok := ntf.Payload.(registry.DataSource)
			if !ok {
				logger.Println("ntListener() create: Bad notification!", ds)
				continue
			}
			s.NtfCreated(ds, ntf.Callback)
		case common.UPDATE:
			dss, ok := ntf.Payload.([]registry.DataSource)
			if !ok || len(dss) < 2 {
				logger.Println("ntListener() update: Bad notification!", dss)
				continue
			}
			s.NtfUpdated(dss[0], dss[1], ntf.Callback)
		case common.DELETE:
			ds, ok := ntf.Payload.(registry.DataSource)
			if !ok {
				logger.Println("ntListener() delete: Bad notification!", ds)
				continue
			}
			s.NtfDeleted(ds, ntf.Callback)
		default:
			// other notifications
		}
	}
}
