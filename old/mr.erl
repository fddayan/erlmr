-module (mr).
-compile(export_all).
-import(utils,[log/2,log/1, partitionn/2]).
-import(record_reader,[list_record_reader/1,list_splitter/1,file_rr/1]).
-import(mapper_server).
-import(reducer_server).

print_progress(N) when N rem 1 =:= 0 -> 
    io:format(".");
print_progress(_) -> ok.

map_task(ContextPid,ReducerPids,MapFunction,RecordReader) ->
    receive
	{map}->
	    utils:log("~p Mapping",[self()]),
    
	    RecordsMapped = RecordReader(MapFunction), 
	    FlattenRecord = lists:flatten(RecordsMapped),
	    GroupedByReducerId = utils:tree_group_by(fun(V)-> 
					  {Key,Value} =V, 
					  {erlang:phash(Key, length(ReducerPids)),{Key,Value}} 
				  end
				  ,FlattenRecord),
	    MapperPid = self(),	 
	    lists:foreach(fun(Index)-> lists:nth(Index, ReducerPids) ! {collect,MapperPid,gb_trees:get(Index,GroupedByReducerId)}  end,gb_trees:keys(GroupedByReducerId)),
	    utils:wait_receives(length(gb_trees:keys(GroupedByReducerId)),ok),

	    ContextPid ! {done}
    end.

reducer_task(Tree,Reduce,Context)->
	receive 
	    {collect,FromMapperPid,List} -> % List = [{key,value}]
		NewTree = utils:tree_group_by(List,fun({K,V})-> {K,V} end,Tree),
	     	print_progress(gb_trees:size(NewTree)),
		FromMapperPid ! ok,
		reducer_task(NewTree,Reduce,Context);

	    {reduce}->
		io:format("~p Reducing ~n",[self()]),
		ReducerOut = utils:tree_collect(Reduce,Tree),

		Context ! {reducer_out,ReducerOut} % ReducerOut = [{Key,Value},...]
	end.

create_reducers(ContextPid,N,ReduceFunction)->
    lists:map(fun(_)->
		      spawn(mr,reducer_task,[gb_trees:empty(),ReduceFunction,ContextPid]) 
	      end
	      ,lists:seq(1,N)).

create_mappers(ContextPid,ReducerPids,RecordReaders,MapFunction)->
    lists:map(fun(RecordReader)->
		      spawn(mr,map_task,[ContextPid,ReducerPids,MapFunction,RecordReader]) 
	      end
	      ,RecordReaders).

gather_reducers(0,L) -> L;
gather_reducers(N, L) ->
	receive
	    {reducer_out,Out}->
		utils:log("~p reducers left",[N-1]),
		gather_reducers(N-1,[Out|L])
	end.
	
gather_mappers(0,L) -> L;
gather_mappers(N,_) -> 
    receive 
	{done}->
	    utils:log("~p maps left",[N-1]),
	    gather_mappers(N-1,[])
    end.

%% collect(0,L,_,_)->
%%     L;
%% collect(N,L,Message,Type) ->
%%  receive 
%% 	Message->
%% 	    utils:log("~p ~p left",[N,Type]),
%% 	    collect(N-1,[],Message,Type)
%%   end.

% TODO: implement a file record iterator that knows how to split. Maybe each record split reads different files.

mapreduce(Map,Reduce,RecordReaders)->
    T1 = erlang:now(),
    Context = self(), 
    Reducers = create_reducers(Context,5,Reduce),
    utils:log("Reducers Processes started ~p",[Reducers]),

    Mappers = create_mappers(Context, Reducers,RecordReaders,Map),
    utils:log("Mapper Processes started ~p",[Mappers]),
    
    io:format("TD1=~p~n",[timer:now_diff(erlang:now(),T1)]),

    lists:foreach(fun(Mapper)->spawn(fun()-> Mapper ! {map} end) end, Mappers),
    gather_mappers(length(Mappers),[]),
    
    io:format("TD2=~p~n",[timer:now_diff(erlang:now(),T1)]),
    io:format("Done Mapping ~n"),

    lists:foreach(fun(Reducer)-> spawn(fun()-> Reducer ! {reduce} end) end, Reducers),
    Ret = gather_reducers(length(Reducers),[]),

    io:format("TD3=~p~n",[timer:now_diff(erlang:now(),T1)]),

    Ret.

mapreduce1(Map,Reduce,RecordReaders)->
    T1 = erlang:now(),
    Context = self(), 
    Reducers = reducer_server:create_reducers(Context,5,Reduce),
    utils:log("Reducers Processes started ~p",[Reducers]),
    Mappers = mapper_server:create_mappers(Context, Reducers,RecordReaders,Map),

    io:format("TD1=~p~n",[timer:now_diff(erlang:now(),T1)]),
    
    utils:log("Mapper Processes started ~p",[Mappers]),

    lists:foreach(fun(Mapper)->spawn(fun()-> gen_server:call(Mapper,  {map}) end) end, Mappers),
    gather_mappers(length(Mappers),[]),
    
    io:format("TD2=~p~n",[timer:now_diff(erlang:now(),T1)]),

    io:format("Done Mapping ~n"),

    lists:foreach(fun(Reducer)-> spawn(fun()-> gen_server:call(Reducer,{reduce}) end) end, Reducers),
    Ret = gather_reducers(length(Reducers),[]),
    
    io:format("TD3=~p~n",[timer:now_diff(erlang:now(),T1)]),

    Ret.

main()->
    RecordReaders = lists:map(fun(L)-> record_reader:list_record_iterator(L) end,record_reader:list_splitter([1,1,2,2,3,3,3,3,4,4,5,5])),
    Result= mapreduce(fun(Value)->[{Value,Value}] end,fun(Key,Values)-> {Key,lists:sum(Values)} end,RecordReaders),

    SortedResult = lists:sort(lists:flatten(Result)),
    io:format("~p~n",[SortedResult]),
    SortedResult =:= [{1,2},{2,4},{3,12},{4,8},{5,10}].

wc()->
    Map = fun(Data)-> 
		  List = string:tokens(Data, " ();:.,->[]\n\t{}/\\ \"\'|_~=%!-*"), 
		  lists:map(fun(V)-> {V,1} end,List)
	  end,
    Reduce = fun(Key,Values)->
		     {Key,lists:sum(Values)}
	     end,
    Result = mapreduce(Map,Reduce,[record_reader:file_rr("mr.erl")]),
    SortedResult = lists:sort(lists:flatten(Result)),
    io:format("Words ~p ~n",[length(SortedResult)]).

dir_wc(Dir,Pattern)->
    Map = fun(Data)-> 
		  List = string:tokens(Data, " ();:.,->[]\n\t{}/\\ \"\'|_~=%!-*"), 
		  lists:map(fun(V)-> {V,1} end,List)
	  end,
    Reduce = fun(Key,Values)->
		     {Key,lists:sum(Values)}
	     end,
    RecordReaders = record_reader:create_record_readers(fun record_reader:file_rr/1,record_reader:directory_splitter(Dir,Pattern)),
    Result = mapreduce(Map,Reduce,RecordReaders),
    % Result = fprof:apply(mr,mapreduce,[Map,Reduce,RecordReaders]),
    SortedResult = lists:sort(lists:flatten(Result)),
    io:format("Words ~p ~n",[length(SortedResult)]).
    
single_wc(Dir,Pattern)->    
     Map = fun(Data)-> 
		  List = string:tokens(Data, " ();:.,->[]\n\t{}/\\ \"\'|_~=%!-*"), 
		  lists:map(fun(V)-> {V,1} end,List)
	  end,
    Reduce = fun(Key,Values)->
		     {Key,lists:sum(Values)}
	     end,
    RecordReaders = record_reader:create_record_readers(fun record_reader:file_rr/1,record_reader:directory_splitter(Dir,Pattern)),
    MapsOut = lists:map(fun(RR)-> RR(Map) end,RecordReaders),
    %io:format("~p~n",[MapsOut]),
    Flatten = lists:flatten(MapsOut),
    Groupped = utils:tree_group_by(fun({Key,Value})-> {Key,Value} end,Flatten),
    Counts = gb_trees:map(fun(K,V)-> Reduce(K,V) end,Groupped),
    io:format("Words ~p ~n",[gb_trees:size(Counts)]).
    




