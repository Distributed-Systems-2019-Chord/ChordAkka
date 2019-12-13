#!/bin/bash
docker run -e CHORD_NODE_TYPE=regular -e CHORD_CENTRAL_NODE=centralnode -e CHORD_CENTRAL_NODE_PORT=25520 -e NODE_ID=$1 -e NOD\
    --network=chord_network \
    --rm \
    -it \
    chord:v1