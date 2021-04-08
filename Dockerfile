FROM golang:1.14-alpine AS builder

RUN apk add --no-cache build-base 

COPY . /home
WORKDIR /home

ARG version
ARG buildnum

RUN go build -v -mod=vendor -o historical-datastore \
		-ldflags "-X main.Version=$version -X main.BuildNumber=$buildnum"

###########
FROM alpine

RUN apk --no-cache add ca-certificates

WORKDIR /home

ARG version
ARG buildnum

LABEL NAME="LinkSmart Historical Datastore"
LABEL VERSION=${version}
LABEL BUILD=${buildnum}
ENV DISABLE_LOG_TIME=1

COPY --from=builder /home/historical-datastore .
COPY sample_conf/historical-datastore.json /home/conf/

ENV HDS_DATA_BACKEND_DSN="/data/data.db?cache=shared&_journal=WAL"
ENV HDS_REGISTRY_BACKEND_DSN=/data/registry

VOLUME /data
VOLUME /pki

EXPOSE 8085
EXPOSE 8088
# HEALTHCHECK --interval=1m CMD wget localhost:8085/health -q -O - > /dev/null 2>&1

ENTRYPOINT ["./historical-datastore"]
# Note: this loads the default config files from /home/conf/. Use --help to to learn about CLI arguments.
