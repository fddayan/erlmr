%%%-------------------------------------------------------------------
%%% @author FEDERICO DAYAN <>
%%% @copyright (C) 2011, FEDERICO DAYAN
%%% @doc
%%%
%%% @end
%%% Created : 23 May 2011 by FEDERICO DAYAN <>
%%%-------------------------------------------------------------------
-module(reducer_server).

-behaviour(gen_server).

%% API
-export([start_link/3,create_reducers/4,start_reducing/1,gather/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link(JobTracker,ContextPid,ReduceFunction) -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------

start_link(JobTracker,ContextPid,ReduceFunction) ->
    gen_server:start_link(?MODULE, [{gb_trees:empty(),ReduceFunction,ContextPid,JobTracker}], []).

create_reducers(JobTracker,ContextPid,N,ReduceFunction)->
    lists:map(fun(_)->
		     {ok,Pid} = start_link(JobTracker,ContextPid,ReduceFunction),
		     Pid
	      end
	      ,lists:seq(1,N)).

start_reducing(Reducers)->
    lists:foreach(fun(Reducer)-> spawn(fun()-> gen_server:call(Reducer,{reduce}) end) end, Reducers).

gather(0,L,_) -> L;
gather(N, L,JobTracker) ->
    receive
	{reducer_out,Out}->
	    reporter:report_progress(JobTracker,"~p reducers left",[N-1]),
	    gather(N-1,[Out|L],JobTracker)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(Args) ->
    {ok, Args}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({collect,FromMapperPid,List},_From,State) -> % List = [{key,value}]
    [{Tree,Reduce,Context,JobTracker}] = State,
    NewTree = utils:tree_group_by(List,fun({K,V})-> {K,V} end,Tree),
    FromMapperPid ! ok,
    {reply,ok,[{NewTree,Reduce,Context,JobTracker}]};

handle_call({reduce},_From,State) ->
    [{Tree,Reduce,Context,JobTracker}] = State,

    reporter:report_progress(JobTracker,"~p Reducing",[self()]),

    ReducerOut = utils:tree_collect(Reduce,Tree),
    Context ! {reducer_out,ReducerOut}, % ReducerOut = [{Key,Value},...]
    {reply,ok,State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

