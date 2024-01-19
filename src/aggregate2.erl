-module(aggregate2).

-export([ run/1
        , aggregate_measurements/2
        , chunk_processor/0
        ]).

-compile({inline,[{process_temp,1},{process_line,2}]}).

-include_lib("eunit/include/eunit.hrl").

run([Filename]) ->
%    logger:update_primary_config(#{ level => none }),
%    dbg:tracer(),dbg:p(all,c),dbg:tpl(?MODULE,x),
    aggregate_measurements(atom_to_list(Filename), [%{bufsize, 2 * 1024 * 1024}%, {parallel, 1}
                                                    {bufsize, 10 * 1024}%, {parallel, 1}
                                                   ]).

aggregate_measurements(Filename, Opts) ->
  process_flag(trap_exit, true),
  Start = erlang:monotonic_time(),
  BufSize = proplists:get_value(bufsize, Opts),
  logger:info(#{bufsize => BufSize}),
  NumProcessors =
    proplists:get_value(parallel, Opts, erlang:system_info(schedulers)),
  {ok, FD} = file:open(Filename, [raw, read, binary]),
  {ProcessorPids, AllPids} = start_processors(NumProcessors),
  {ok, Bin} = file:pread(FD, 0, BufSize),
  read_chunks(FD, 0, byte_size(Bin), Bin, BufSize, ProcessorPids, NumProcessors * 3),
  Now = erlang:monotonic_time(),
  logger:info(#{label => "All chunks read, waiting for processors to finish",
                elapsed_secs => (Now - Start) / 1000_000_000.0}),
  Map = wait_for_completion(AllPids, #{}),

  %% ?assertMatch({_, 180, _, _}, maps:get(<<"Abha">>, Map)),
  Fmt = format_final_map(Map),
  case proplists:get_value(no_output, Opts, false) of
    true -> ok;
    false ->
      io:format("~ts~n", [Fmt])
  end.

start_processors(NumProcs) ->
  logger:info(#{label => "Starting processing pipelines",
                num_pipelines => NumProcs}),
  lists:foldl(
    fun(_, {ProcessorPids, AllPids}) ->
        ProcessorPid = proc_lib:start_link(?MODULE, chunk_processor, []),
        ProcessorPid ! {result_pid, self()},
        {[ProcessorPid|ProcessorPids],
         [ProcessorPid|AllPids]}
    end, {[], []}, lists:seq(1, NumProcs)).

read_chunks(FD, N, Offset, PrevChunk, BufSize, TargetPids, 0) ->
    receive
        give_me_more ->
            read_chunks(FD, N, Offset, PrevChunk, BufSize, TargetPids, 1)
    end;
read_chunks(FD, N, Offset, PrevChunk, BufSize, TargetPids, Outstanding) ->
    TargetPid = lists:nth((N rem length(TargetPids)) + 1, TargetPids),
    case file:pread(FD, Offset, BufSize) of
        {ok, Bin} ->
            Size = byte_size(Bin),
            %% Read chunks pair-wise and split them so that each processed
            %% chunk is on an even newline boundary
            case binary:split(Bin, <<"\n">>) of
                [First, NextChunk] ->
                    send_chunk([PrevChunk, First], TargetPid),
                    %% sleep_if_target_pid_mql_too_long(TargetPid, 20),
                    read_chunks(FD, N + 1, Offset + Size, NextChunk, BufSize, TargetPids, Outstanding - 1);
                [Chunk] ->
                    send_chunk([Chunk], TargetPid),
                    read_chunks(FD, N + 1, Offset + Size, <<>>, BufSize, TargetPids, Outstanding - 1)
            end;
        eof ->
            %% Reached end of file, process the last chunk
            send_chunk([PrevChunk], TargetPid),
            lists:foreach(fun(Pid) -> Pid ! eof end, TargetPids),
            ok
    end.

send_chunk(Chunk, TargetPid) ->
    Buff = prim_buffer:new(),
    prim_buffer:write(Buff, Chunk),
    TargetPid ! {chunk, Buff}.

wait_for_completion([], Map) ->
  logger:info(#{label => "All subprocesses finished"}),
  Map;
wait_for_completion(Pids, Map) ->
  receive
    {'EXIT', Pid, normal} ->
      wait_for_completion(Pids -- [Pid], Map);
    {result, SenderPid, NewMap} ->
      logger:info(#{label => "Got result",
                    sender => SenderPid,
                    result_size => maps:size(NewMap)}),
      wait_for_completion(Pids, merge_location_data(Map, NewMap));
      give_me_more ->
          wait_for_completion(Pids, Map);
    M ->
      logger:error(#{label => "Unexpected message", msg => M})
  end.

merge_location_data(Map1, Map2) ->
  Stations = lists:usort(maps:keys(Map1) ++ maps:keys(Map2)),
  lists:foldl(
    fun(Station, Map) when Station =:= '$ancestors' orelse
                           Station =:= '$initial_call' ->
        Map;
       (Station, Map) ->
        case {maps:get(Station, Map1, undefined),
              maps:get(Station, Map2, undefined)} of
          {Data1, undefined} -> maps:put(Station, Data1, Map);
          {undefined, Data2} -> maps:put(Station, Data2, Map);
          {Data1, Data2} ->
            {Min1, Max1, Count1, Sum1} = Data1,
            {Min2, Max2, Count2, Sum2} = Data2,
            maps:put(
              Station,
              {min(Min1, Min2),
               max(Max1, Max2),
               Count1 + Count2,
               Sum1 + Sum2},
              Map)
        end
    end, #{}, Stations).

format_final_map(Map) ->
  "{" ++
    lists:join(
      ", ",
      lists:map(
        fun({Station, {Min, Max, Count, Sum}}) ->
            Mean = Sum / Count,
            io_lib:format("~ts=~.1f/~.1f/~.1f",
                          [Station, Min/10, Mean/10, Max/10])
        end, lists:sort(maps:to_list(Map))))
    ++ "}".

%%
%% Chunk processor: this step in the pipeline takes a binary
%% consisting of an even number of line, splits it at "\n" and ";" and
%% passes it on to the line processor.
%%
chunk_processor() ->
  proc_lib:init_ack(self()),
  logger:info(#{label => "Chunk processor running"}),
    try
        chunk_processor_loop(undefined)
    catch E:R:ST ->
            logger:error(#{ crashed => {E,R,ST} })
    end.

chunk_processor_loop(Pid) ->
  receive
    {result_pid, NewPid} ->
      chunk_processor_loop(NewPid);
    {chunk, Buff} ->
          process_line(Buff),
          Pid ! give_me_more,
          chunk_processor_loop(Pid);
    eof ->
      Map = maps:from_list(get()),
      Pid ! {result, self(), Map},
      {heap_size, HeapSize} = process_info(self(), heap_size),
      logger:info(#{label => "Chunk processor finished",
                    heap_size => HeapSize}),
          ok;
    M ->
      logger:error(#{label => "Unexpected message", msg => M})
  end.

%%
%% The line processor
%%

process_line(Buff) ->
    case prim_buffer:find_byte_index(Buff, $;) of
        {ok, StationIndex} ->
            Station = prim_buffer:read(Buff, StationIndex),
            prim_buffer:skip(Buff, 1),
            case prim_buffer:find_byte_index(Buff, $\n) of
                {ok, TempIndex} ->
                    Temp = prim_buffer:read(Buff, TempIndex),
                    prim_buffer:skip(Buff, 1),
                    process_line(Station, process_temp(Temp)),
                    process_line(Buff);
                not_found ->
                    Temp = prim_buffer:read(Buff, prim_buffer:size(Buff)),
                    process_line(Station, process_temp(Temp))
            end;
        not_found ->
            % <<>> = prim_buffer:read(Buff, prim_buffer:size(Buff)),
            ok
    end.

-define(TO_NUM(C), (C - $0)).

process_temp(<<$-, A, B, $., C>>) ->
    -1 * (?TO_NUM(A) * 100 + ?TO_NUM(B) * 10 + ?TO_NUM(C));
process_temp(<<$-, B, $., C>>) ->
    -1 * (?TO_NUM(B) * 10 + ?TO_NUM(C));
process_temp(<<A, B, $., C>>) ->
    ?TO_NUM(A) * 100 + ?TO_NUM(B) * 10 + ?TO_NUM(C);
process_temp(<<B, $., C>>) ->
    ?TO_NUM(B) * 10 + ?TO_NUM(C).

process_line(Station, Temp) ->
    case get(Station) of
        undefined ->
            put(Station, { Temp % min
                         , Temp % max
                         , 1    % count
                         , Temp % sum
                         });

        {OldMin, OldMax, OldCount, OldSum} ->
            put(Station, { min(OldMin, Temp)
                         , max(OldMax, Temp)
                         , OldCount + 1
                         , OldSum + Temp
                         })

    end.
