package registry

// EventListener is implemented by storage modules and connectors which need to react to changes in the registry
type EventListener interface {
	CreateHandler(new DataSource) error
	UpdateHandler(old DataSource, new DataSource) error
	DeleteHandler(old DataSource) error
}

// eventHandler implements sequential fav-out/fan-in of events from registry
type eventHandler []EventListener

func (h eventHandler) created(new DataSource) error {
	for i := range h {
		err := h[i].CreateHandler(new)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h eventHandler) updated(old DataSource, new DataSource) error {
	for i := range h {
		err := h[i].UpdateHandler(old, new)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h eventHandler) deleted(old DataSource) error {
	for i := range h {
		err := h[i].DeleteHandler(old)
		if err != nil {
			return err
		}
	}
	return nil
}
