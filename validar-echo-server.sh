#!/bin/bash

RESULT=$(echo "hola" | docker run --rm --network testing_net busybox nc server 12345)

if [ "$RESULT" = "hola" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: failed"
fi

