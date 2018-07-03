FROM golang:1.10-alpine as builder

ENV PACKAGE code.linksmart.eu/hds/historical-datastore

# copy code
COPY . /home/src/${PACKAGE}

# build
ENV GOPATH /home
RUN go install ${PACKAGE}

###########
FROM alpine

RUN apk --no-cache add ca-certificates

WORKDIR /home
COPY --from=builder /home/bin/* .
COPY sample_conf/* /conf/

VOLUME /conf /data
EXPOSE 8085 4000

ENTRYPOINT ["./historical-datastore"]
CMD ["-conf", "/conf/docker.json"]
