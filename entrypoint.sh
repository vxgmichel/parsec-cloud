#!/bin/sh

if [ "x$PARSEC_DB_URL" == "x" ]
then
    echo "Missing database URL. Is $$PARSEC_DB_URL set?"
    exit 1
fi

if [ "x$PARSEC_BLOCK_STORE_URL" == "x" ]
then
    echo "Missing block store URL. Is $$PARSEC_BLOCK_STORE_URL set?"
    exit 1
fi

parsec backend --host=0.0.0.0 --port=8000 --store=$PARSEC_DB_URL --block-store=$PARSEC_BLOCK_STORE_URL --debug
