# Force to use bash
SHELL = /bin/bash

export GOPATH=$(PWD)/temp

# Shortcuts for gb vendoring tool
GETGB=go get github.com/constabulary/gb/...
REMOVEGB=rm -rf $(GOPATH)
GB=$(GOPATH)/bin/gb

default: install

install: 
	@ $(GETGB) && $(GB) build && $(REMOVEGB)
build:
	@ $(GETGB) && $(GB) build
test:
	@ $(GETGB) && $(GB) test
clean:
	@ rm -rf bin pkg && $(REMOVEGB)
