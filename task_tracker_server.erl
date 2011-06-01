%%%-------------------------------------------------------------------
%%% @author FEDERICO DAYAN <>
%%% @copyright (C) 2011, FEDERICO DAYAN
%%% @doc
%%%
%%% @end
%%% Created : 24 May 2011 by FEDERICO DAYAN <>
%%%-------------------------------------------------------------------
-module(task_tracker_server).

-behaviour(gen_server).

%% API
-export([start_link/0,start_link/1,create_mapper/5,create_reducer/3,distributed_mr/6,mapreduce/3,mapreduce/4,get_nodes/0,print_history/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,terminate/2, code_change/3]).

-define(SERVER, ?MODULE).  

-import(utils).

-include("records.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link()->
    start_link({verbose,false}).

start_link(Options) ->
    {verbose,Verbose} = Options,
    case gen_server:start_link({local, ?SERVER}, ?MODULE, [Options], []) of 
	{ok,Pid}->
	    utils:log_if(Verbose,"Task Tracker ~p started. Nodes: ~p",[Pid,get_nodes()]),
	    Pid;
	{error,{already_started,Pid}}->
	    utils:log_if(Verbose,"~p Task Tracker already running ~n",[Pid]),
	    Pid
    end.

print_history()->
    MapperTasks = task_tracker_db:find_mapper_tasks(),
    lists:foreach(fun(M)->
			  io:format("|~15w|~25s|~15w ms|~15w|~n",[M#mapper_task.id,iso_8601_fmt(calendar:now_to_datetime(M#mapper_task.time)),M#mapper_task.duration/1000,M#mapper_task.job_id])
		  end,MapperTasks).

create_mapper(Node,Context,Reducers,Map,RecordReader)->
       gen_server:call(Node,{create_mapper,Context,Reducers,Map,RecordReader}).
    
create_reducer(Node,Context,Reduce)->
    ReducerPid = gen_server:call(Node,{create_reducer,Context,Reduce}),
    ReducerPid.

mapreduce(Map,Reduce,RecordReaders) ->
    mapreduce(Map,Reduce,RecordReaders,{verbose,false}).

mapreduce(Map,Reduce,RecordReaders,Options)->
    Context = self(), 
    Nodes = get_nodes(),
    
    distributed_mr(Context,Nodes,Map,Reduce,RecordReaders,Options).

distributed_mr(Context,Nodes,Map,Reduce,RecordReaders,Options)->
    {verbose,Verbose} = Options,

    utils:log_if(Verbose,"Nodes ~p",[Nodes]),

    Reducers = lists:map(fun(Node)-> create_reducer(Node,Context,Reduce) end, Nodes),
    Mappers = utils:roundrobin_map(
		fun(RecordReader,Node)-> 
			create_mapper(Node,Context,Reducers,Map,RecordReader)
		end
		,RecordReaders,Nodes),

    utils:log_if(Verbose,"Reducers ~p",[Reducers]),
    utils:log_if(Verbose,"Mappers ~p",[Mappers]),

    Ts = erlang:now(),
    mapper_server:start_mapping(Mappers),
    mapper_server:gather(length(Mappers),[],Options),
    Te = erlang:now(),

    utils:log_if(Verbose,"~p Done Mapping in ~p ms",[Context,timer:now_diff(Te,Ts)/1000]),

    reducer_server:start_reducing(Reducers),
    Ret = reducer_server:gather(length(Reducers),[],Options),
    Ret.

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
    pg2:create(task_tracker),
    pg2:join(task_tracker, self()),
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
handle_call({create_mapper,Context,Reducers,Map,RecordReader}, _From, State) ->
    [{verbose,Verbose}] = State,
    {ok,Pid} = mapper_server:start_link(Context,Reducers,RecordReader,Map),
    utils:log_if(Verbose,"~p Creating mapper ~p ~n",[self(),Pid]),
    {reply, Pid, State};
handle_call({create_reducer,Context,Reduce}, _From, State) ->
    [{verbose,Verbose}] = State, 
    {ok,Pid} = reducer_server:start_link(Reduce,Context),
    utils:log_if(Verbose,"~p Creating reducer ~p ~n",[self(),Pid]),
    {reply, Pid, State}.

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

get_nodes()->
    case pg2:get_members(task_tracker) of 
	{error,{no_such_group,_Name}}-> throw({task_trakcer,{not_running}});
	Pids-> Pids
    end.



iso_8601_fmt(DateTime) ->
    {{Year,Month,Day},{Hour,Min,Sec}} = DateTime,
    io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
        [Year, Month, Day, Hour, Min, Sec]).
