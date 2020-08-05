package registry

import (
	"context"
	"encoding/json"
	"net"
	"reflect"

	"github.com/containerd/typeurl"
	"github.com/gogo/protobuf/types"
	"github.com/linksmart/historical-datastore/common"
	_go "github.com/linksmart/historical-datastore/protobuf/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// API describes the RESTful HTTP data API
type GrpcAPI struct {
	c      *Controller
	server *grpc.Server
}

func marshalSeries(t TimeSeries) (_go.Series, error) {
	s := _go.Series{
		Name: t.Name,
		Type: _go.Series_ValueType(t.Type),
		Unit: t.Unit,
	}

	if t.Meta != nil {
		s.Meta = make(map[string]*types.Any)
		for k, v := range t.Meta {
			bytes, err := json.Marshal(v)
			if err != nil {
				return _go.Series{}, err
			}
			s.Meta[k] = &types.Any{
				TypeUrl: reflect.TypeOf(v).Name(),
				Value:   bytes,
			}

		}
	}
	return s, nil

}
func UnmarshalSeries(s _go.Series) (TimeSeries, error) {
	ts := TimeSeries{
		Name: s.Name,
		Type: ValueType(s.Type),
		Unit: s.Unit,
	}
	if s.Meta != nil {
		ts.Meta = make(map[string]interface{})
		for k, v := range s.Meta {
			var err error
			ts.Meta[k], err = typeurl.UnmarshalAny(v)
			if err != nil {
				return ts, err
			}
		}
	}
	return ts, nil
}
func marshalSeriesList(ts []TimeSeries) (seriesList []*_go.Series, err error) {
	seriesList = make([]*_go.Series, len(ts))
	for i, t := range ts {
		s, err := marshalSeries(t)
		if err != nil {
			return nil, err
		}
		seriesList[i] = &s
	}
	return seriesList, nil
}

func unmarshalSeriesList(seriesList []*_go.Series) (ts []TimeSeries, err error) {
	ts = make([]TimeSeries, len(seriesList))
	for i, s := range seriesList {
		t, err := UnmarshalSeries(*s)
		if err != nil {
			return nil, err
		}
		ts[i] = t
	}
	return ts, nil
}

func (a GrpcAPI) Add(ctx context.Context, series *_go.Series) (*_go.Void, error) {
	ts, err := UnmarshalSeries(*series)
	if err != nil {
		return &_go.Void{}, status.Errorf(codes.InvalidArgument, err.Error())
	}
	_, addErr := a.c.Add(ts)
	if addErr != nil {
		return &_go.Void{}, status.Errorf(addErr.GrpcStatus(), addErr.Error())
	}
	return &_go.Void{}, nil
}

func (a GrpcAPI) GetAll(ctx context.Context, req *_go.PageParams) (*_go.Registrations, error) {
	page := int(req.Page)
	perPage := int(req.PerPage)
	err := common.ValidatePagingParams(page, perPage, MaxPerPage)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	ts, total, getErr := a.c.GetMany(page, perPage)
	if getErr != nil {
		return nil, status.Errorf(getErr.GrpcStatus(), err.Error())
	}
	reg := &_go.Registrations{
		PerPage: int32(perPage),
		Page:    int32(page),
		Total:   int32(total),
	}
	reg.SeriesList, err = marshalSeriesList(ts)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "Error marshalling the time series registrations: %v", err)
	}
	return reg, nil
}

func (a GrpcAPI) Get(ctx context.Context, name *_go.SeriesName) (*_go.Series, error) {
	ts, getErr := a.c.Get(name.Series)
	if getErr != nil {
		return nil, status.Errorf(getErr.GrpcStatus(), getErr.Error())
	}

	s, err := marshalSeries(*ts)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "Error marshalling the time series registration: %v", err)
	}
	return &s, nil
}

func (a GrpcAPI) FilterOne(ctx context.Context, req *_go.Filterpath) (*_go.Series, error) {
	ts, filterErr := a.c.FilterOne(req.Path, req.Op, req.Value)
	if filterErr != nil {
		return nil, status.Errorf(filterErr.GrpcStatus(), filterErr.Error())
	}

	if ts == nil { //nothing matched
		return nil, nil
	}
	s, err := marshalSeries(*ts)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "Error marshalling the time series registration: %v", err)
	}
	return &s, nil
}

func (a GrpcAPI) Filter(ctx context.Context, req *_go.FilterManyRequest) (*_go.Registrations, error) {
	page := int(req.PageParams.Page)
	perPage := int(req.PageParams.PerPage)
	err := common.ValidatePagingParams(page, perPage, MaxPerPage)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	ts, total, filterErr := a.c.Filter(req.FilterPath.Path, req.FilterPath.Op, req.FilterPath.Value, page, perPage)
	if filterErr != nil {
		return nil, status.Errorf(filterErr.GrpcStatus(), err.Error())
	}
	reg := &_go.Registrations{
		PerPage: int32(perPage),
		Page:    int32(page),
		Total:   int32(total),
	}
	reg.SeriesList, err = marshalSeriesList(ts)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "Error marshalling the time series registrations: %v", err)
	}
	return reg, nil
}

func (a GrpcAPI) Update(ctx context.Context, series *_go.Series) (*_go.Void, error) {
	ts, err := UnmarshalSeries(*series)
	if err != nil {
		return &_go.Void{}, status.Errorf(codes.InvalidArgument, err.Error())
	}
	_, updatErr := a.c.Update(series.Name, ts)
	if updatErr != nil {
		return &_go.Void{}, status.Errorf(updatErr.GrpcStatus(), updatErr.Error())
	}
	return &_go.Void{}, nil
}

func (a GrpcAPI) Delete(ctx context.Context, name *_go.SeriesName) (*_go.Void, error) {
	deleteErr := a.c.Delete(name.Series)
	if deleteErr != nil {
		return &_go.Void{}, status.Errorf(deleteErr.GrpcStatus(), deleteErr.Error())
	}
	return &_go.Void{}, nil
}

// NewAPI returns the configured Data API
func NewGrpcAPI(storage Storage) *GrpcAPI {
	srv := grpc.NewServer()
	grpcAPI := &GrpcAPI{&Controller{storage}, srv} //TODO: Sharing controller between HTTP and Grpc instead of creating one for both
	_go.RegisterRegistryServer(srv, grpcAPI)
	return grpcAPI
}

func (a GrpcAPI) StartGrpcServer(l net.Listener) error {
	return a.server.Serve(l)
}

func (a GrpcAPI) StopGrpcServer() {
	a.server.Stop()
}
