#!/bin/bash

docker network create testing_net

docker run -d --rm --name server --network testing_net server:latest

RESULT=$(echo "hola" | docker run --rm --network testing_net busybox nc server 12345)

if [ "$RESULT" == "hola" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: failed"
fi

docker stop server

docker network rm testing_net

