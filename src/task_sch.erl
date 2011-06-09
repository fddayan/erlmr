%%%-------------------------------------------------------------------
%%% @author FEDERICO DAYAN <>
%%% @copyright (C) 2011, FEDERICO DAYAN
%%% @doc
%%%
%%% @end
%%% Created :  8 Jun 2011 by FEDERICO DAYAN <>
%%%-------------------------------------------------------------------
-module(task_sch).

-export([start/1, init/1,submit/4,get_pending/1]).

-export([test/0,wait_and_print/1]).

-define(MAX,3).


%%% API----------------------------------------------------------------

start(Max) ->
    spawn(task_sch, init, [{self(),queue:new(),sets:new(),Max}]).

submit(Pid,Module,Fun,Args)->
    Pid ! {submit,Module,Fun,Args}.

get_pending(Pid)->
    Pid ! {get_pending,self()},
    receive
        Pending->
            Pending
    end.

%% SERVER -------------------------------------------------------------

init(State) ->
    loop(State).

loop({From,Pending,Running,Max}) ->
    receive
        {'DOWN', MonitorRef, _Type, _Pid, _Info} ->
            UpdatedRunning = sets:del_element(MonitorRef,Running),
            {NewPending,NewRunning} = try_to_run_next(Pending,UpdatedRunning,Max),

            loop({From,NewPending,NewRunning,Max});
        {submit,Module,Fun,Args} ->
            UpdatedPending = queue:in({Module,Fun,Args},Pending),
            {NewPending,NewRunning} = try_to_run_next(UpdatedPending,Running,Max),

            loop({From,NewPending,NewRunning,Max});
        {get_pending,From} ->
            From ! Pending,
            loop({From,Pending,Running,Max})
    end.

%%% INTERNAL ------------------------------------------------------------

try_to_run_next(Pending,Running,Max) ->
    case queue:is_queue(Pending) and not queue:is_empty(Pending) of
        true ->
            case is_av(Running,Max) of
                true->
                    run_next(Pending,Running);
                _ ->
                    {Pending,Running}
            end;
       false ->
            {Pending,Running}
    end.

is_av(Running,Max) ->
    Len = sets:size(Running),
    if
        Len < Max ->
            true;
        true ->
            false
    end.

run_next(Pending,Running) ->
    {{value,{Module,Fun,Args}},NewQ} = queue:out(Pending),
    Pid = spawn(fun()-> erlang:apply(Module,Fun,Args) end),
    Ref = erlang:monitor(process,Pid),
    NewSet = sets:add_element(Ref,Running),
    {NewQ,NewSet}.

%%% TEST ---------------------------------------------------------------

test()->
    P = start(?MAX),
    [submit(P,task_sch,wait_and_print,[5000]) || _ <- lists:seq(0,6)],
    Pending = get_pending(P),
    io:format("Pending ~p~n",[Pending]),
    ok.

wait_and_print(Time)->
    timer:sleep(Time),
    io:format("Hello~n").
