-module(aggregate2).

-export([ aggregate_measurements/2
        , chunk_processor/0
        , line_processor/0
        , parse_float/1
        ]).

-include_lib("eunit/include/eunit.hrl").

aggregate_measurements(Filename, Opts) ->
  process_flag(trap_exit, true),
  Start = erlang:monotonic_time(),
  BufSize = proplists:get_value(bufsize, Opts),
  logger:info(#{bufsize => BufSize}),
  {ok, FD} = prim_file:open(Filename, [read]),
  NumProcessors =
    proplists:get_value(parallel, Opts, erlang:system_info(schedulers) div 2),
  {ProcessorPids, AllPids} = start_processors(NumProcessors),
  read_chunks(FD, 0, 0, <<>>, BufSize, ProcessorPids),
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
        LineProcessorPid = proc_lib:start_link(?MODULE, line_processor, []),
        ProcessorPid = proc_lib:start_link(?MODULE, chunk_processor, []),
        ProcessorPid ! {line_processor, LineProcessorPid},
        LineProcessorPid ! {result_pid, self()},
        {[ProcessorPid|ProcessorPids],
         [ProcessorPid, LineProcessorPid|AllPids]}
    end, {[], []}, lists:seq(1, NumProcs)).

read_chunks(FD, N, Offset, PrevChunk, BufSize, TargetPids) ->
  TargetPid = lists:nth((N rem length(TargetPids)) + 1, TargetPids),
  case prim_file:pread(FD, Offset, BufSize) of
    {ok, Bin} ->
      Size = byte_size(Bin),
      %% Read chunks pair-wise and split them so that each processed
      %% chunk is on an even newline boundary
      case binary:split(Bin, <<"\n">>) of
        [First, NextChunk] ->
          send_chunk([PrevChunk, First], TargetPid),
          %% sleep_if_target_pid_mql_too_long(TargetPid, 20),
          read_chunks(FD, N + 1, Offset + Size, NextChunk, BufSize, TargetPids);
        [Chunk] ->
          send_chunk([Chunk], TargetPid),
          read_chunks(FD, N + 1, Offset + Size, <<>>, BufSize, TargetPids)
      end;
    eof ->
      %% Reached end of file, process the last chunk
      send_chunk([PrevChunk], TargetPid),
      lists:foreach(fun(Pid) -> Pid ! eof end, TargetPids),
      ok
  end.

send_chunk(Chunk, TargetPid) ->
  TargetPid ! {chunk, Chunk}.

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
    _ ->
      logger:error(#{label => "Unexpected message"})
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
  chunk_processor_loop(undefined).

chunk_processor_loop(Pid) ->
  receive
    {line_processor, LineProcessorPid} ->
      chunk_processor_loop(LineProcessorPid);
    {chunk, [Chunk]} ->
      Pid ! {lines, {binary:split(Chunk, [<<"\n">>, <<";">>], [global]), <<>>}},
      chunk_processor_loop(Pid);
    {chunk, [First, Second]} ->
          Pid ! {lines, {binary:split(First, [<<"\n">>, <<";">>], [global]), Second}},
      chunk_processor_loop(Pid);
    eof ->
      logger:info(#{label => "Chunk processor finished."}),
      Pid ! eof;
    M ->
      logger:error(#{label => "Unexpected message", msg => M})
  end.

%%
%% The line processor
%%

line_processor() ->
  proc_lib:init_ack(self()),
  line_processor_loop(undefined).

line_processor_loop(ResultPid) ->
  receive
    {result_pid, Pid} ->
      line_processor_loop(Pid);
    {lines, {Lines, Tail}} ->
      process_lines(Lines, Tail),
      line_processor_loop(ResultPid);
    eof ->
      Map = maps:from_list(get()),
      ResultPid ! {result, self(), Map},
      {heap_size, HeapSize} = process_info(self(), heap_size),
      logger:info(#{label => "Line processor finished",
                    heap_size => HeapSize}),
      ok;
    M ->
      logger:error(#{label => "Unexpected message", msg => M})
  end.

process_lines(Main, Tail) ->
    case process_lines(Main) of
        [] ->
            <<>> = Tail,
            ok;
        Rest when Tail =/= <<>> ->
            process_lines(Rest);
        [Station] ->
            process_lines(binary:split(<<Station/binary, Tail/binary>>, [<<";">>]));
        [Station, Temp] ->
            process_lines([Station, <<Temp/binary, Tail/binary>>])
    end.
process_lines([Station, TempBin|Rest]) when Rest =/= [] ->
  Temp = parse_float(TempBin),
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

  end,
    process_lines(Rest);
process_lines(Rest) ->
    Rest.


%% Very specialized float-parser for floats with a single fractional
%% digit, and returns the result as an integer * 10.
-define(TO_NUM(C), (C - $0)).

parse_float(<<$-, A, B, $., C>>) ->
  -1 * (?TO_NUM(A) * 100 + ?TO_NUM(B) * 10 + ?TO_NUM(C));
parse_float(<<$-, B, $., C>>) ->
  -1 * (?TO_NUM(B) * 10 + ?TO_NUM(C));
parse_float(<<A, B, $., C>>) ->
  ?TO_NUM(A) * 100 + ?TO_NUM(B) * 10 + ?TO_NUM(C);
parse_float(<<B, $., C>>) ->
  ?TO_NUM(B) * 10 + ?TO_NUM(C).

-ifdef(TEST).

parse_float_test() ->
  lists:foreach(fun({Bin, Exp}) ->
                    Float = parse_float(Bin),
                    ?debugVal({Bin, Float}),
                    ?assert(abs(Exp - Float) =< 0.0001)
                end,
                [ {<<"-0.5">>, -5}
                , {<<"-10.5">>, -105}
                , {<<"10.5">>, 105}
                ]).

-endif.
