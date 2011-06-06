-module(reporter).

-export([report_progress/2,report_error/2,report_progress/3,report_error/3]).



report_progress(Pid,Message)->
    Pid ! {progress,{Message,[]}}.

report_progress(Pid,Message,Data)->
    Pid ! {progress,{Message,Data}}.

report_error(Pid,Message)->
    Pid ! {error,{Message,[]}}.

report_error(Pid,Message,Data)->
    Pid ! {error,{Message,Data}}.

