-module (utils).

-compile(export_all).

roundrobin(F,L1,L2) ->

	roundrobin(F,L1,L2,L1,L2).

roundrobin(F,L1,L2,[H1|T1],[H2|T2]) ->
	F(H1,H2),
	roundrobin(F,L1,L2,T1,T2);
roundrobin(F,L1,L2,[H1|T1],[]) -> 
	[H2|T2] = L2,
	F(H1,H2),
	roundrobin(F,L1,L2,T1,T2);
roundrobin(_,_,_,[],_) -> ok.


roundrobin_map(F,L1,L2) -> 
    roundrobin_map(F,L1,L2,L1,L2,[]).

roundrobin_map(F,L1,L2,[H1|T1],[H2|T2],Acc) ->
	R = F(H1,H2), 
	roundrobin_map(F,L1,L2,T1,T2,[R|Acc]);
roundrobin_map(F,L1,L2,[H1|T1],[],Acc) -> 
	[H2|T2] = L2,
	R = F(H1,H2),
	roundrobin_map(F,L1,L2,T1,T2,[R|Acc]);
roundrobin_map(_,_,_,[],_,Acc) -> Acc.


log(Value,Vars)->
    io:format(lists:append([Value,"~n"]),Vars).
log(Value)->
    io:format(lists:append([Value,"~n"])).

log_if(Verbose,Value,Vars)->
    case Verbose of 
	true->
	    io:format(lists:append([Value,"~n"]),Vars);
	  _ -> false
    end.
		    
log_if(Verbose,Value)->
    case Verbose of 
	true->
	    io:format(lists:append([Value,"~n"]));
	_ -> false
    end.


log_val(Var)->
    io:format("~p",[Var]).


partitionn(List,N) ->
    partitionn(List,N,[]).

partitionn([],_,Acc) ->
    Acc;
partitionn(List,N,Acc) when N =< length(List)->
    {H,T} = lists:split(N,List),
    partitionn(T,N,[H|Acc]);
partitionn(List,N,Acc) when N > length(List)-> [List|Acc].


tree_group_by(Fun,List)->
    tree_group_by(List,Fun,gb_trees:empty()).

dict_group_by(Fun,List)->
    dict_group_by(List,Fun,dict:new()).

dict_group_by([],_,Dict) -> Dict;
dict_group_by(List,Fun,Dict)->
   [H|T] = List,
   {Key,Value} = Fun(H),
   NewDict = case dict:is_key(Key, Dict) of
		 true ->
		     dict:append(Key, Value, Dict);
		 false ->
		     dict:store(Key,[Value], Dict)
	     end,
    dict_group_by(T,Fun,NewDict).

tree_group_by([],_,Tree) -> Tree;
tree_group_by(List,Fun,Tree)->
   [H|T] = List,
   {Key,Value} = Fun(H),
   NewTree = case gb_trees:is_defined(Key, Tree) of
		 true ->
		     gb_trees:update(Key, lists:append([Value],gb_trees:get(Key,Tree)), Tree);
		 false ->
		     gb_trees:insert(Key,[Value],Tree)
	     end,
    tree_group_by(T,Fun,NewTree).


tree_collect(Fun,Tree)->
    Iter = gb_trees:iterator(Tree),
     case gb_trees:next(Iter) of 
	{Key,Val,Iter2}->  tree_collect(Fun,Iter2,[Fun(Key,Val)]);
	none-> []
    end.
	
tree_collect(Fun,Iter,Acc)->
    case gb_trees:next(Iter) of 
	{Key,Val,Iter2}-> tree_collect(Fun,Iter2,[Fun(Key,Val)|Acc]);
	none-> Acc
    end.

wait_receives(0,_)-> ok;
wait_receives(N,Msg) -> 
    receive
	Msg->
%	    io:format("~p Wait ~p ~n",[self(),N]),
	    wait_receives(N-1,Msg)
    end.


print_label(Title)->
    io:format("------------------------------------ ~p ------------------------------------~n",[Title]).

print_label()->
    io:format("----------------------------------------------------------------------------------~n").

start(ServerName, {Module, Function, Args}) ->
  global:trans({ServerName, ServerName},
	       fun() ->
		   case global:whereis_name(ServerName) of
		     undefined -> 
		       Pid = spawn(Module, Function, Args),
		       global:register_name(ServerName, Pid);
		     _ ->
		       ok
		   end
	       end).

stop(ServerName) ->
    global:trans({ServerName, ServerName},
		 fun() ->
			 case global:whereis_name(ServerName) of
			     undefined ->
				 ok;
			     _ ->
				 global:send(ServerName, shutdown)
			 end
		 end).
