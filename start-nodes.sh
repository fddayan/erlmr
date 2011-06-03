#!/bin/bash

set -u
set -e

#if [ ! -f ~/.hosts.erlang ]
#then
    echo "'`hostname`'." > ./.hosts.erlang
#fi

erlc *.erl

i=1;
while [ $i -le $1 ];
  do 
#	echo Starting Trask Tracker $i;
	erl -sname tt$i -s dmr -noshell -detached
  	let i=$i+1;
done
echo Nodes started
#erl -sname tt1 -s dmr -noshell -detached
#erl -sname tt2 -s dmr -noshell -detached
#erl -sname tt3 -s dmr -noshell -detached
#exec erl -sname tt4 -s dmr
