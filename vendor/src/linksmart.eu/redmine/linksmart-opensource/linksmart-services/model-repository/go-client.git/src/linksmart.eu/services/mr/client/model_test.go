package client

import (
	"encoding/json"
	"testing"
)

var validSample = `
{
  "name": "string",
  "version": "string",
  "model": [
    {
      "ls_id": "12345",
      "ls_name": "DeviceName",
      "ls_stereotype": "Device",
      "ls_attributes": {
        "description": "Device description",
        "ttl": 60
      },
      "domain_class": "Object",
      "domain_attributes": {},
      "children": [
        {
          "ls_id": "67890",
          "ls_name": "SensorName",
          "ls_stereotype": "Resource",
          "ls_attributes": {
            "ext_protocol": {
              "type": "MQTT",
              "endpoint": {
                "url": "tcp://mqtt-broker.local:1883",
                "pub_topic": "/sortline/raw/DeviceName/SensorName"
              },
              "methods": [
                "PUB"
              ],
              "content-types": [
                "application/senml+json"
              ]
            }
          },
          "domain_class": "string",
          "domain_attributes": {}
        }
      ]
    },
    {
      "ls_id": "54321",
      "ls_name": "DeviceName2",
      "ls_stereotype": "Device",
      "ls_attributes": {
        "description": "Device description 2",
        "ttl": 120
      },
      "domain_class": "Object",
      "domain_attributes": {},
      "children": [
        {
          "ls_id": "09876",
          "ls_name": "SensorName2",
          "ls_stereotype": "Resource",
          "ls_attributes": {
            "ext_protocol": {
              "type": "REST",
              "endpoint": {
                "url": "http://localhost/sensor2"
              },
              "methods": [
                "GET"
              ],
              "content-types": [
                "application/senml+json"
              ]
            }
          },
          "domain_class": "string",
          "domain_attributes": {}
        }
      ]
    }
  ]
}
`

func TestParseModel(t *testing.T) {
	var model Model
	err := json.Unmarshal([]byte(validSample), &model)
	if err != nil {
		t.Fatalf("Error unmarshalling json: %v\n", err.Error())
	}

	devices, err := model.ParseDevices()
	if err != nil {
		t.Fatalf("Error parsing devices: %v\n", err.Error())
	}

	out, _ := json.Marshal(devices)
	t.Log(string(out))

	if len(devices) < 2 {
		t.Fatalf("Should be 2 devices, but got only %v\n", len(devices))
	}

	d2 := devices[1]
	if d2.Name != "DeviceName2" {
		t.Fatalf("Wrong name of device 2: %v\n", d2)
	}
	if len(d2.Resources) != 1 {
		t.Fatalf("Device 2 should have exactly 1 resource, but has %v\n", len(d2.Resources))
	}

	d2r1 := d2.Resources[0]

	if d2r1.Name != "SensorName2" {
		t.Fatalf("Wrong name of d2r1: %v\n", d2r1)
	}

	if d2r1.ExtProtocol == nil {
		t.Fatalf("The ExtProtocol of d2r1 is nil, but it should not be: %v\n", d2r1)
	}
}
