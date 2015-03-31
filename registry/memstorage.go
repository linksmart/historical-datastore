package registry

func add(ds DataSource) error {
	// TODO
	return nil
}

func update(id string, ds DataSource) error {
	// TODO
	return nil
}

func delete(id string) error {
	// TODO
	return nil
}

func get(id string) (DataSource, error) {
	// TODO
	return DataSource{}, nil
}

func getMany(page, perPage int) ([]DataSource, int, error) {
	// TODO
	return []DataSource{}, 0, nil
}

func getCount() int {
	// TODO
	return 0
}

func pathFilterOne(path, op, value string) (DataSource, error) {
	// TODO
	return DataSource{}, nil
}

func pathFilter(path, op, value string, page, perPage int) ([]DataSource, int, error) {
	// TODO
	return []DataSource{}, 0, nil
}
