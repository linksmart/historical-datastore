FROM golang:1.12-alpine as builder

RUN apk add --no-cache build-base git

# copy code
COPY . /home

# build
WORKDIR /home
RUN go build -mod=vendor -o historical-datastore

###########
FROM alpine

RUN apk --no-cache add ca-certificates

WORKDIR /home
COPY --from=builder /home/historical-datastore .
COPY sample_conf/* /conf/

VOLUME /conf /data
EXPOSE 8085
# HEALTHCHECK --interval=1m CMD wget localhost:8085/health -q -O - > /dev/null 2>&1

ENTRYPOINT ["./historical-datastore"]
CMD ["-conf", "/conf/docker.json"]
