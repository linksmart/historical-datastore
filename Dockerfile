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
EXPOSE 8085
# HEALTHCHECK --interval=1m CMD wget localhost:8085/health -q -O - > /dev/null 2>&1

ENTRYPOINT ["./historical-datastore"]
CMD ["-conf", "/conf/docker.json"]
