FROM golang:1.8-alpine

ENV REFRESHED_AT 2017-04-25

# get dependencies
RUN apk add --no-cache git

# install go tools
RUN go get github.com/constabulary/gb/...

# setup hds home
RUN mkdir /home/hds
ENV HDS_HOME /home/hds

# copy default config file and code
COPY sample_conf/* /conf/
COPY . ${HDS_HOME}

WORKDIR ${HDS_HOME}

# build code
RUN gb build all

VOLUME /conf /data

EXPOSE 8085 4000

ENTRYPOINT ["./bin/historical-datastore"]
CMD ["-conf", "/conf/docker.json"]
