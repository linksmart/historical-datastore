FROM golang:1.8-alpine as builder

# copy code
COPY . /home/src/code.linksmart.eu/hds/historical-datastore

# build
ENV GOPATH /home
RUN go install code.linksmart.eu/hds/historical-datastore

###########
FROM alpine

WORKDIR /home
COPY --from=builder /home/bin/* .

VOLUME /conf /data
EXPOSE 8085 4000

COPY sample_conf/* /conf/
ENTRYPOINT ["./historical-datastore"]
CMD ["-conf", "/conf/docker.json"]
