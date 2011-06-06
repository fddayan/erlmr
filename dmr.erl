-module (dmr).
-compile(export_all).
-import(utils).
-import(record_reader,[list_record_reader/1,list_splitter/1,file_rr/1]).
-import(mapper_server).
-import(reducer_server).
-import(task_tracker).

start()->
    net_adm:world(),
    task_tracker:start().

main()->
    task_tracker:start(),

    Map = fun(Value)-> [{Value,Value}] end,
    Reduce = fun(Key,Values)-> {Key,lists:sum(Values)} end,
    RecordReaders = lists:map(fun(L)-> record_reader:list_record_iterator(L) end,record_reader:list_splitter([1,1,2,2,3,3,3,3,4,4,5,5])),

    ReducersResult = task_tracker:mapreduce(Map,Reduce,RecordReaders),

    SortedResult = lists:sort(lists:flatten(ReducersResult)),
    io:format("~p~n",[SortedResult]),
    SortedResult =:= [{1,2},{2,4},{3,12},{4,8},{5,10}].

wc()->
    task_tracker:start(),

    Map = fun(Data)-> 
		  List = string:tokens(Data, " ();:.,->[]\n\t{}/\\ \"\'|_~=%!-*"), 
		  lists:map(fun(V)-> {V,1} end,List)
	  end,
    Reduce = fun(Key,Values)->
		     {Key,lists:sum(Values)}
	     end,
    Result = task_tracker:mapreduce(Map,Reduce,[record_reader:file_rr("mr.erl")]),
    SortedResult = lists:sort(lists:flatten(Result)),
    io:format("Words ~p ~n",[length(SortedResult)]).

dir_wc(Dir,Pattern)->
    JobTracker = job_tracker:start_link(),

    io:format("JobTracker ~p~n",[JobTracker]),

    Map = fun(Data)-> 
		  List = string:tokens(Data, " ();:.,->[]\n\t{}/\\ \"\'|_~=%!-*"), 
		  lists:map(fun(V)-> {V,1} end,List)
	  end,
    Reduce = fun(Key,Values)->
		     {Key,lists:sum(Values)}
	     end,

    RecordReaders = record_reader:create_record_readers(fun record_reader:file_rr/1,record_reader:directory_splitter(Dir,Pattern)),
    Result = task_tracker:mapreduce(Map,Reduce,RecordReaders,JobTracker),

    SortedResult = lists:sort(lists:flatten(Result)),
    
    io:format("Words ~p ~n",[length(SortedResult)]).

    
