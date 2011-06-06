-module(all_tests).

-include_lib("eunit/include/eunit.hrl").

-import(record_reader).
-import(job_tracker).
-import(task_tracker).

task_tracker_mapreduce_with_directory_test()->
    task_tracker:start(),
    JobTracker = job_tracker:start_link(),
    Dir = "../resources/",
    Pattern = "(pg3160.txt|pg35934.txt)",    

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

    io:format("word:~p~n",[length(SortedResult)]),
    
    ?assert(19411 =:= length(SortedResult)).
    

task_tracker_mapreduce_with_lists_test()->
    task_tracker:start(),
    JobTracker = job_tracker:start_link(),

    Map = fun(Value)-> [{Value,Value}] end,
    Reduce = fun(Key,Values)-> {Key,lists:sum(Values)} end,
    RecordReaders = lists:map(fun(L)-> record_reader:list_record_iterator(L) end,record_reader:list_splitter([1,1,2,2,3,3,3,3,4,4,5,5])),

    ReducersResult = task_tracker:mapreduce(Map,Reduce,RecordReaders,JobTracker),

    SortedResult = lists:sort(lists:flatten(ReducersResult)),
    io:format("~p~n",[SortedResult]),
    ?assert(SortedResult =:= [{1,2},{2,4},{3,12},{4,8},{5,10}]).
    
