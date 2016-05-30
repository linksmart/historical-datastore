package client

// ModelRepository models

const (
	StereotypeDevice   = "Device"
	StereotypeResource = "Resource"
)

// Model is the top-most object
type Model struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Nodes   []Node `json:"model"`
}

// Node is the generic represenatation of an object in the model
type Node struct {
	LSID             string                 `json:"ls_id"`
	LSName           string                 `json:"ls_name"`
	LSStereoptype    string                 `json:"ls_stereotype"`
	LSAttributes     map[string]interface{} `json:"ls_attributes"`
	DomainClass      string                 `json:"domain_class"`
	DomainAttributes map[string]interface{} `json:"domain_attributes"`
	Children         []Node                 `json:"children"`
}

// parases the model to []Device
func (m *Model) ParseDevices() ([]Device, error) {
	var devices []Device
	// go through the list of all nodes
	for _, node := range m.Nodes {
		if node.LSStereoptype == StereotypeDevice {
			d, err := node.ParseDevice()
			if err != nil {
				return nil, err
			}
			devices = append(devices, *d)
		}
	}

	return devices, nil
}

func (n *Node) ParseDevice() (*Device, error) {
	var d Device
	d.ID = n.LSID
	d.Name = n.LSName

	if n.LSAttributes["description"] != nil {
		d.Description = n.LSAttributes["description"].(string)
	}

	if n.LSAttributes["meta"] != nil {
		d.Meta = n.LSAttributes["meta"].(map[string]interface{})
	}

	// parse TTL
	if n.LSAttributes["ttl"] != nil {
		d.TTL = int(n.LSAttributes["ttl"].(float64))

	}

	// Parse children resources
	d.Resources = []Resource{}
	for _, node := range n.Children {
		if node.LSStereoptype == StereotypeResource {
			r, err := node.ParseResource()
			if err != nil {
				return nil, err
			}
			r.ID = node.LSID
			r.Name = node.LSName
			d.Resources = append(d.Resources, *r)
		}
	}

	return &d, nil
}

func (n *Node) ParseResource() (*Resource, error) {
	var r Resource
	if n.LSAttributes["meta"] != nil {
		r.Meta = n.LSAttributes["meta"].(map[string]interface{})
	}

	if n.LSAttributes["representation"] != nil {
		r.Representation = n.LSAttributes["representation"].(map[string]interface{})
	}

	// parse protocols
	if n.LSAttributes["int_protocol"] != nil {
		r.IntProtocol = ParseProtocol(n.LSAttributes["int_protocol"].(map[string]interface{}))
	}
	if n.LSAttributes["ext_protocol"] != nil {
		r.ExtProtocol = ParseProtocol(n.LSAttributes["ext_protocol"].(map[string]interface{}))
	}
	return &r, nil
}

func ParseProtocol(data map[string]interface{}) interface{} {
	// fmt.Printf("%T", data["methods"])
	// type assert to Protocol
	var contentTypes []string
	for _, ct := range data["content-types"].([]interface{}) {
		contentTypes = append(contentTypes, ct.(string))
	}
	protocol := Protocol{
		Type:         data["type"].(string),
		Endpoint:     data["endpoint"].(map[string]interface{}),
		ContentTypes: contentTypes,
	}

	for _, m := range data["methods"].([]interface{}) {
		protocol.Methods = append(protocol.Methods, m.(string))
	}
	// Methods:      data["methods"],
	// ContentTypes: data["content-types"]

	switch protocol.Type {
	case ProtoREST:
		restProtocol := RESTProtocol{
			&protocol,
			RESTEndpoint{},
		}
		_, ok := protocol.Endpoint["url"]
		if ok {
			restProtocol.Endpoint.URL = protocol.Endpoint["url"].(string)
		}
		return &restProtocol
	case ProtoMQTT:
		mqttProtocol := MQTTProtocol{
			&protocol,
			MQTTEndpoint{},
		}
		_, ok := protocol.Endpoint["url"]
		if ok {
			mqttProtocol.Endpoint.URL = protocol.Endpoint["url"].(string)
		}

		_, ok = protocol.Endpoint["pub_topic"]
		if ok {
			mqttProtocol.Endpoint.PubTopic = protocol.Endpoint["pub_topic"].(string)
		}

		_, ok = protocol.Endpoint["sub_topic"]
		if ok {
			mqttProtocol.Endpoint.SubTopic = protocol.Endpoint["sub_topic"].(string)
		}
		return &mqttProtocol
	default:
		return &protocol
	}

}
