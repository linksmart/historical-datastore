module code.linksmart.eu/hds/historical-datastore

require (
	code.linksmart.eu/com/go-sec v1.0.0
	code.linksmart.eu/sc/service-catalog v2.3.4+incompatible
	github.com/ancientlore/go-avltree v1.0.1 // indirect
	github.com/codegangsta/negroni v0.2.0
	github.com/dgrijalva/jwt-go v3.1.0+incompatible // indirect
	github.com/dschowta/lite.tsdb v0.0.0-20190315103504-66f54648e8e2 // indirect
	github.com/dschowta/senml.datastore v0.0.0-20190315112405-1580e769cb84
	github.com/eclipse/paho.mqtt.golang v1.1.1
	github.com/farshidtz/elog v0.9.0 // indirect
	github.com/farshidtz/mqtt-match v1.0.1 // indirect
	github.com/farshidtz/senml v1.0.2
	github.com/gorilla/context v1.1.1
	github.com/gorilla/mux v1.4.0
	github.com/justinas/alice v0.0.0-20171023064455-03f45bd4b7da
	github.com/onsi/ginkgo v1.8.0 // indirect
	github.com/onsi/gomega v1.5.0 // indirect
	github.com/rs/cors v1.6.0
	github.com/satori/go.uuid v1.1.0
	github.com/syndtr/goleveldb v1.0.0
	golang.org/x/sys v0.0.0-20190322080309-f49334f85ddc // indirect
)

replace (
	github.com/dschowta/lite.tsdb => ../lite.tsdb
	github.com/dschowta/senml.datastore => ../senml.datastore
)
