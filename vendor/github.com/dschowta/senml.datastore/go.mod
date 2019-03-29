module github.com/dschowta/senml.datastore

go 1.12

require (
	github.com/boltdb/bolt v1.3.1
	github.com/dschowta/lite.tsdb v0.0.0-20190117083202-b0a5ea1c6099
	github.com/farshidtz/senml v1.0.2
	github.com/ugorji/go v1.1.2
	github.com/ugorji/go/codec v0.0.0-20190320090025-2dc34c0b8780 // indirect
	golang.org/x/sys v0.0.0-20190204203706-41f3e6584952
)

replace github.com/dschowta/lite.tsdb => ../lite.tsdb
