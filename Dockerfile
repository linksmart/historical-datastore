FROM golang
MAINTAINER Alexandr Krylovskiy "alexandr.krylovskiy@fit.fraunhofer.de"
ENV REFRESHED_AT 2016-01-18

# update system
RUN apt-get update
RUN apt-get install -y wget git

# install the fraunhofer certificate
RUN wget http://cdp1.pca.dfn.de/fraunhofer-ca/pub/cacert/cacert.pem -O /usr/local/share/ca-certificates/fhg.crt
RUN update-ca-certificates

# install go tools
RUN go get github.com/constabulary/gb/...

# setup local connect home
RUN mkdir /opt/hds
ENV HDS_HOME /opt/hds
WORKDIR ${HDS_HOME}

# copy code & build
COPY . ${HDS_HOME}
RUN gb build all

VOLUME conf

EXPOSE 8085
