-module (mr).
-compile(export_all).
-import(utils,[log/2,log/1, partitionn/2]).
-import(record_reader,[list_record_reader/1,list_splitter/1,file_rr/1]).
-import(mapper_server).
-import(reducer_server).
-import(task_tracker_server).


mapreduce(Map,Reduce,RecordReaders)->
    Context = self(), 
    Reducers = reducer_server:create_reducers(Context,5,Reduce),
    utils:log("Reducers Processes started ~p",[Reducers]),
    
    Mappers = mapper_server:create_mappers(Context, Reducers,RecordReaders,Map),
    utils:log("Mapper Processes started ~p",[Mappers]),

    mapper_server:start_mapping(Mappers),
    mapper_server:gather(length(Mappers),[]),

    io:format("Done Mapping ~n"),

    reducer_server:start_reducing(Reducers),
    Ret = reducer_server:gather(length(Reducers),[]),

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

    Ts = erlang:now(),
    MapsOut = lists:map(fun(RR)-> RR(Map) end,RecordReaders),
%    MapsOut = lists:map(fun(RR)-> RR(Map) end,RecordReaders),
    io:format("Map time: ~p~n",[timer:now_diff(erlang:now(),Ts)/1000]),
    
    Ts2 = erlang:now(),
    Flatten = lists:flatten(MapsOut),
    Groupped = utils:tree_group_by(fun({Key,Value})-> {Key,Value} end,Flatten),
    io:format("Prepare Time: ~p~n",[timer:now_diff(erlang:now(),Ts2)/1000]),

    Ts3 = erlang:now(),
    Counts = gb_trees:map(fun(K,V)-> Reduce(K,V) end,Groupped),
    io:format("Reduce Time: ~p~n",[timer:now_diff(erlang:now(),Ts3)/1000]),
    
    Te = erlang:now(),
    io:format("Words ~p in ~p Mu, ~p ms ~n",[gb_trees:size(Counts),timer:now_diff(Te,Ts),timer:now_diff(Te,Ts)/1000]).


bm()->
    Files = ["/Users/fddayan/projects/erlang/erlmr/etc/pg3160.txt","/Users/fddayan/projects/erlang/erlmr/etc/pg2610.txt","/Users/fddayan/projects/erlang/erlmr/etc/pg35934.txt","/Users/fddayan/projects/erlang/erlmr/etc/pg4300.txt"],
    Count = fun(File)-> 
%		    case file:open(File, read) of
		    case file:open(File, [raw,read_ahead]) of
			{ok, S} ->
			    Val = count_lines(S,0,[]),
			    file:close(S),
			    {File,Val};
			{error, Why} ->
			    io:format("Could not open file ~p ~n",[Why]),
			    {error, Why}
		    end

	    end,

    Ts = erlang:now(),
    LineCount = lists:map(Count,Files),
    Te = erlang:now(),

    io:format("~p lines in ~p Mu, ~p ms, ~p s ~n",[LineCount,timer:now_diff(Te,Ts),timer:now_diff(Te,Ts)/1000,(timer:now_diff(Te,Ts)/1000)/1000]).

count_lines(S,N,Acc) ->
%    case io:get_line(S, '') of
    case file:read_line(S) of
	eof -> N;
	{error,Why} -> Why;
	{ok,Data} -> 
	    List = string:tokens(Data, " ();:.,->[]\n\t{}/\\ \"\'|_~=%!-*"), 
	    Ret = lists:map(fun(V)-> {V,1} end,List),
	    count_lines(S,N+1,lists:append(Ret,Acc))
    end.

compile()->
    io:format("Compiling Home~n"),
    code:add_patha("."),
    compile:file(mr),
    compile:file(utils),
    compile:file(record_reader),
    compile:file(mapper_server),
    compile:file(reducer_server),
    compile:file(task_tracker_server),
    ok.

    




