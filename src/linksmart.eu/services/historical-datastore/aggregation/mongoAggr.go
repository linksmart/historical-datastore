package aggregation

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	 mgo "gopkg.in/mgo.v2"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/data"
	"linksmart.eu/services/historical-datastore/registry"
)
