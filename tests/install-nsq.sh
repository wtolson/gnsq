#!/bin/bash

if [ -z "$NSQ_VERSION" ]; then
    exit 0
fi


MIRROR="https://s3.amazonaws.com/bitly-downloads/nsq"
FILENAME="nsq-$NSQ_VERSION.linux-amd64.go$GO_VERSION.tar.gz"

curl "$MIRROR/$FILENAME" | sudo tar --strip-components 1 -C /usr/local -zxvf -
