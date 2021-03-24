package registry

import (
	"context"
	"fmt"

	_go "github.com/linksmart/historical-datastore/protobuf/go"
	"google.golang.org/grpc"
)

type GrpcClient struct {
	Client _go.RegistryClient
}

func NewGrpcClient(serverEndpoint string, opts ...grpc.DialOption) (*GrpcClient, error) {
	conn, err := grpc.Dial(serverEndpoint, opts...)
	if err != nil {
		return nil, err
	}
	client := _go.NewRegistryClient(conn)
	return &GrpcClient{Client: client}, nil
}

func (c GrpcClient) Add(ts TimeSeries) error {
	s, err := marshalSeries(ts)
	if err != nil {
		return err
	}
	_, err = c.Client.Add(context.Background(), &s)
	if err != nil {
		return err
	}
	return nil
}
func (c GrpcClient) Update(ts TimeSeries) error {
	s, err := marshalSeries(ts)
	if err != nil {
		return err
	}
	_, err = c.Client.Update(context.Background(), &s)
	if err != nil {
		return err
	}
	return nil
}
func (c GrpcClient) Get(name string) (*TimeSeries, error) {
	sName := &_go.SeriesName{Series: name}
	series, err := c.Client.Get(context.Background(), sName)
	if err != nil {
		return nil, err
	}
	if series == nil {
		return nil, fmt.Errorf("got empty reponse")
	}
	ts, err := UnmarshalSeries(*series)
	return &ts, err
}
func (c GrpcClient) Delete(name string) error {
	sName := &_go.SeriesName{Series: name}
	_, err := c.Client.Delete(context.Background(), sName)
	return err
}

func (c GrpcClient) GetMany(page, perPage int) ([]TimeSeries, int, error) {
	pageParams := _go.PageParams{
		Page:    int32(page),
		PerPage: int32(perPage),
	}
	registrations, err := c.Client.GetAll(context.Background(), &pageParams)
	if err != nil {
		return nil, 0, err
	}

	ts, err := unmarshalSeriesList(registrations.SeriesList)

	return ts, int(registrations.Total), nil
}
func (c GrpcClient) FilterOne(path, op, value string) (*TimeSeries, error) {
	filterPath := &_go.Filterpath{
		Path:  path,
		Op:    op,
		Value: value,
	}
	series, err := c.Client.FilterOne(context.Background(), filterPath)
	if err != nil {
		return nil, err
	}
	if series == nil {
		return nil, fmt.Errorf("no series returned")
	}
	ts, err := UnmarshalSeries(*series)
	return &ts, err
}
func (c GrpcClient) Filter(path, op, value string, page, perPage int) ([]TimeSeries, int, error) {
	filterManyRequest := _go.FilterManyRequest{
		FilterPath: &_go.Filterpath{
			Path:  path,
			Op:    op,
			Value: value},
		PageParams: &_go.PageParams{
			Page:    int32(page),
			PerPage: int32(perPage),
		},
	}
	registrations, err := c.Client.Filter(context.Background(), &filterManyRequest)
	if err != nil {
		return nil, 0, err
	}

	ts, err := unmarshalSeriesList(registrations.SeriesList)

	return ts, int(registrations.Total), nil
}
