-module(job_tracker).

-export([start_link/0,stop/1,loop/0]).

start_link()-> 
    spawn(?MODULE,loop,[]).

stop(Pid)->
    Pid ! shutdown.

loop()->
    receive
	{progress,{Message,Data}} ->
	    io:format(Message,Data),
	    io:nl(),
	    loop();
	{error,{Message,Data}} ->
	    io:format("ERROR:"++Message,[Data]),
	    loop();
	shutdown -> ok
    end.
    

