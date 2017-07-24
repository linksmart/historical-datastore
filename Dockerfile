FROM golang:1.8-alpine

ENV REFRESHED_AT 2017-05-31

# setup hds home
RUN mkdir /home/hds
ENV HDS_HOME /home/hds

# copy default config file and code
COPY sample_conf/* /conf/
COPY . ${HDS_HOME}/src/code.linksmart.eu/hds/historical-datastore

WORKDIR ${HDS_HOME}

# build code	
ENV GOPATH ${HDS_HOME}
RUN go install code.linksmart.eu/hds/historical-datastore

VOLUME /conf /data

EXPOSE 8085 4000

ENTRYPOINT ["./bin/historical-datastore"]
CMD ["-conf", "/conf/docker.json"]
