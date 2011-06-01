-module(mapper_server).
-behaviour(gen_server).

-export([start_link/4,stop/0,create_mappers/4,start_mapping/1,gather/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,code_change/3]).

-import(utils).

%%%%%%%%%% API

start_link(ContextPid,ReducerPids,RecordReader,MapFunction)->
    start_link(ContextPid,ReducerPids,RecordReader,MapFunction,{verbose,false}).

start_link(ContextPid,ReducerPids,RecordReader,MapFunction,Options)->
     gen_server:start_link(?MODULE, [{ContextPid,ReducerPids,RecordReader,MapFunction,Options}], []).

stop()->
    ok.

create_mappers(ContextPid,ReducerPids,RecordReaders,MapFunction)->
    create_mappers(ContextPid,ReducerPids,RecordReaders,MapFunction,{verbose,false}).

create_mappers(ContextPid,ReducerPids,RecordReaders,MapFunction,Options)->
    lists:map(fun(RecordReader)->
		      {ok,Pid} = start_link(ContextPid,ReducerPids,RecordReader,MapFunction,Options),
		      Pid
	      end
	      ,RecordReaders).

start_mapping(Mappers)->
    lists:foreach(fun(Mapper)->spawn(fun()-> gen_server:call(Mapper,  {map}) end) end, Mappers).

gather(0,L,_) -> L;
gather(N,_,Options) ->
    {verbose,Verbose} = Options,
    receive 
	{done}->
	    utils:log_if(Verbose,"~p maps left",[N-1]),
	    gather(N-1,[],Options)
    end.

%%%%%%%%%%% GEN_ SERVER

init(Args) ->
    %% pg2:create(dmr),
    %% pg2:join(dmr, self()),
    {ok, Args}.

handle_call({map}, _From, State) ->
    [{ContextPid,ReducerPids,RecordReader,MapFunction,Options}] = State,
    map_task(ContextPid,ReducerPids,MapFunction,RecordReader,Options),
    {reply, ok,State}.

handle_cast(_Request, _State) ->
   ok.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%%%%%%%% Actual work
map_task(ContextPid,ReducerPids,MapFunction,RecordReader,Options)->
    {verbose,Verbose} = Options,

    utils:log_if(Verbose,"~p Mapping",[self()]),

    Ts = erlang:now(),
    RecordsMapped = RecordReader(MapFunction), 
    FlattenRecord = lists:flatten(RecordsMapped),
    GroupedByReducerId = utils:tree_group_by(
			   fun(V)-> 
				   {Key,Value} =V, 
				   {erlang:phash(Key, length(ReducerPids)),{Key,Value}} 
			   end
			   ,FlattenRecord),
    MapperPid = self(),	 

    lists:foreach(fun(Index)-> 
			  gen_server:call(lists:nth(Index, ReducerPids),{collect,MapperPid,gb_trees:get(Index,GroupedByReducerId)})
		  end,gb_trees:keys(GroupedByReducerId)),
    utils:wait_receives(length(gb_trees:keys(GroupedByReducerId)),ok),

%    task_tracker_db:save_mapper_task(MapperPid,Ts,timer:now_diff(erlang:now(),Ts),ContextPid),

    ContextPid ! {done}.

