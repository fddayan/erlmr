-module(mapper_server).
-behaviour(gen_server).

-export([start_link/5,stop/0,create_mappers/5,start_mapping/1,gather/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,code_change/3]).

-import(utils).

%%%%%%%%%% API

%start_link(ContextPid,ReducerPids,RecordReader,MapFunction)->
%    start_link(ContextPid,ReducerPids,RecordReader,MapFunction,_).

start_link(ContextPid,ReducerPids,RecordReader,MapFunction,JobTracker)->
     gen_server:start_link(?MODULE, [{ContextPid,ReducerPids,RecordReader,MapFunction,JobTracker}], []).

stop()->
    ok.

create_mappers(ContextPid,ReducerPids,RecordReaders,MapFunction,JobTracker)->
    lists:map(fun(RecordReader)->
		      {ok,Pid} = start_link(ContextPid,ReducerPids,RecordReader,MapFunction,JobTracker),
		      Pid
	      end
	      ,RecordReaders).

start_mapping(Mappers)->
    lists:foreach(fun(Mapper)->spawn(fun()-> gen_server:call(Mapper,  {map}) end) end, Mappers).

gather(0,L,_) -> L;
gather(N,_,JobTracker) ->
    receive 
	{done}->
	    reporter:report_progress(JobTracker,"~p maps left",[N-1]),
	    gather(N-1,[],JobTracker)
    end.

%%%%%%%%%%% GEN_ SERVER

init(Args) ->
    {ok, Args}.

handle_call({map}, _From, State) ->
    [{ContextPid,ReducerPids,RecordReader,MapFunction,JobTracker}] = State,
    map_task(ContextPid,ReducerPids,MapFunction,RecordReader,JobTracker),
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
map_task(ContextPid,ReducerPids,MapFunction,RecordReader,JobTracker)->
    reporter:report_progress(JobTracker,"~p Mapping",[self()]),

%    Ts = erlang:now(),
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

