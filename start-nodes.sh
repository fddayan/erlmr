#!/bin/sh

if [ ! -f ~/.hosts.erlang ]
then
    echo "'`hostname`'." > ~/.hosts.erlang
fi

erlc *.erl

erl -sname tt1 -s dmr -noshell -detached
erl -sname tt2 -s dmr -noshell -detached
erl -sname tt3 -s dmr -noshell -detached
exec erl -sname tt4 -s dmr
