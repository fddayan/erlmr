-module(task_tracker_db).

%-compile([export_all]).
-export([start/0,stop/0,save_mapper_task/4,find_mapper_tasks/1,find_mapper_tasks/0,run/1]).

-include_lib("stdlib/include/qlc.hrl").

-define(SERVER, task_tracker_db).

-record(mapper_task,{id,time,duration,job_id}).

%% API
save_mapper_task(Id,Time,Duration,JobId) ->
  global:send(?SERVER, {save_map_task,Id,Time,Duration,JobId}).

find_mapper_tasks(JobId) ->
  global:send(?SERVER, {get_messages_by_job, self(),JobId}),
  receive
    {ok, MapperTasks} ->
      MapperTasks
  end.

find_mapper_tasks()->
    global:send(?SERVER, {get_all_mapper_tasks, self()}),
    receive
	{ok, MapperTasks} ->
	    MapperTasks
    end.

start() ->
  utils:start(?SERVER, {task_tracker_db, run, [true]}).

stop() ->
  utils:stop(?SERVER).


% SERVER
run(FirstTime) ->
  if
    FirstTime == true ->
      init_store(),
      run(false);
    true ->
      receive
	{save_map_task,Id,Time,Duration,JobId}->
	      save_mapper_task_impl(Id,Time,Duration,JobId),
	      run(FirstTime);
	{get_messages_by_job,Pid,JobId}->
	      Results = find_mapper_tasks_impl(JobId),
	      Pid ! {ok,Results},
	      run(FirstTime);
	{get_all_mapper_tasks,Pid}->
	      Results = find_all_mapper_tasks_impl(),
	      Pid ! {ok,Results},
	      run(FirstTime);
	shutdown ->
	  mnesia:stop(),
	  io:format("Shutting down...~n")
      end
  end. 

init_store() ->
  mnesia:create_schema([node()]),
  mnesia:start(),
  try
    mnesia:table_info(chat_message, type)
  catch
    exit: _ ->
      mnesia:create_table(mapper_task, [{attributes, record_info(fields, mapper_task)},
					 {type, bag},
					 {disc_copies, [node()]}])
  end.

% INTERNAL 
find_all_mapper_tasks_impl()->
     F = fun() ->
		Query = qlc:q([M || M <- mnesia:table(mapper_task)]),
		Results = qlc:e(Query),
		Results
	end,
    {atomic, MapperTasks} = mnesia:transaction(F),
    MapperTasks.

save_mapper_task_impl(Id, Time,Duration,JobId) ->
  F = fun() -> mnesia:write(#mapper_task{id=Id, time=Time, duration=Duration,job_id=JobId}) end,
  mnesia:transaction(F).

find_mapper_tasks_impl(JobId) ->
    F = fun() ->
		Query = qlc:q([M || M <- mnesia:table(mapper_task),M#mapper_task.job_id =:= JobId]),
		Results = qlc:e(Query),
		Results
	end,
    {atomic, MapperTasks} = mnesia:transaction(F),
    MapperTasks.
