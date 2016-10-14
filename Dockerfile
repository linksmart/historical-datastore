FROM golang
MAINTAINER Alexandr Krylovskiy "alexandr.krylovskiy@fit.fraunhofer.de"
ENV REFRESHED_AT 2016-01-18

# update system
RUN apt-get update
RUN apt-get install -y wget git

# install dockerize
ENV DOCKERIZE_VERSION v0.2.0
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

# install the fraunhofer certificate
RUN wget http://cdp1.pca.dfn.de/fraunhofer-ca/pub/cacert/cacert.pem -O /usr/local/share/ca-certificates/fhg.crt
RUN update-ca-certificates

# install go tools
RUN go get github.com/constabulary/gb/...

# setup hds home
RUN mkdir /opt/hds
ENV HDS_HOME /opt/hds

# copy default config file and code
COPY sample_conf/* /conf/
COPY . ${HDS_HOME}

WORKDIR ${HDS_HOME}

# build code
RUN gb build all

VOLUME /conf /data

EXPOSE 8085 4000

ENTRYPOINT ["./bin/historical-datastore"]
CMD ["-conf", "/conf/default.json"]
