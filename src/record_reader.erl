-module(record_reader).
-compile(export_all).
%-export([list_record_iterator/1,list_splitter/1]).

create_record_readers(RecordReader,Splitter)->
    lists:map(RecordReader,Splitter).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Splitters
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

list_splitter(List)->
    utils:partitionn(List,3).

directory_splitter(Dir,RegexPattern)->
    {ok,Files} = file:list_dir(Dir),
    Filtered = lists:filter(fun(File)-> 
				    case re:run(File,RegexPattern,[{capture, none}]) of
					{match,_}->true;
					match->true;
					_ -> false
				    end   
			    end,Files),
    lists:map(fun(V)-> Dir ++ V end,Filtered).
    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Record Readers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

list_record_iterator(List)->
    fun(DoIt)->
	    lists:map(DoIt,List)
    end.

% File reader

file_rr(File)->
    fun(DoIt)->
	    foreach_line_map(DoIt,File)
    end.

foreach_line(Do,File) ->
    case file:open(File, [raw,read_ahead]) of
	{ok, S} ->
	    Val = for_line(Do,S),
	    file:close(S),
	    {ok, Val};
	{error, Why} ->
	    io:format("Could not open file ~p ~n",[Why]),
	    {error, Why}
    end.

for_line(F,S) ->
    case file:read_line(S) of
	eof -> [];
	{error,Why} -> Why;
	{ok,Line} -> 
	    F(Line),
	    for_line(F,S)
    end.

foreach_line_map(Do,File) ->
    case file:open(File, [raw,read_ahead]) of
	{ok, S} ->
	    Val = for_line_map(Do,S,[]),
	    file:close(S),
	    Val;
	{error, Why} ->
	    io:format("Could not open file ~p ~n",[Why]),
	    {error, Why}
    end.

for_line_map(F,S,Acc) ->
    case file:read_line(S) of
	eof -> Acc;
	{error,Why} -> Why;
	{ok,Line} -> 	  
	    R = F(Line),
	    for_line_map(F,S,lists:append(R,Acc))
    end.


for_line_maptree(F,S,Tree) ->
    case file:read_line(S) of
	eof -> Tree;
	{error,Why} -> Why;
	{ok,Line} -> 	  
	    {Key,Value} = F(Line),
	    for_line_map(F,S,case gb_trees:is_defined(Key, Tree) of
		 true ->
		     gb_trees:update(Key, lists:append([Value],gb_trees:get(Key,Tree)), Tree);
		 false ->
		     gb_trees:insert(Key,[Value],Tree)
	     end)
    end.

% Random file reader

random_foreach_line(Do,File,Location,Number)->
     case file:open(File, read) of
	{ok, S} ->
	    Val = random_for_line(Do,S,Location,Location+Number,Location,100),
	    file:close(S),
	    {ok, Val};
	{error, Why} ->
	    {error, Why}
    end.

random_for_line(F,S,From,To,Pos,Block)  when Pos+Block<To ->
    case file:pread(S,Pos,Block) of
	eof -> [];
	{ok,Data} -> 
	    F(Data),
	    random_for_line(F,S,From,To,Pos+Block,Block);
	Error -> Error
    end;    
random_for_line(F,S,_From,To,Pos,Block)  when Pos+Block>=To ->
   case file:pread(S,Pos,To-Pos) of
	eof -> [];
	{ok,Data} -> F(Data);
	Error -> Error
    end.

main()->    
    random_foreach_line(fun(V)->
				io:format("~p ~p~n",[V,length(V)])
			end
			,"record_reader.erl",0,1500).
    
