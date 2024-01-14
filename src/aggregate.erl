-module(aggregate).

-export([ aggregate_measurements/2
        , chunk_processor/0
        , line_processor/0
        , parse_float/1
        ]).

-include_lib("eunit/include/eunit.hrl").

-record(location, { max   :: integer()
                  , min   :: integer()
                  , sum   :: integer()
                  , count :: integer()
                  }).


aggregate_measurements(Filename, Opts) ->
  process_flag(trap_exit, true),
  Start = erlang:monotonic_time(),
  BufSize = proplists:get_value(bufsize, Opts),
  logger:info(#{label => "Starting", bufsize => BufSize}),
  {ok, FD} = prim_file:open(Filename, [read]),
  NumProcessors = erlang:system_info(schedulers) div 2,
  {ProcessorPids, AllPids} = start_processors(NumProcessors),
  read_chunks(FD, 0, 0, <<>>, BufSize, ProcessorPids),
  Now = erlang:monotonic_time(),
  logger:info(#{label => "All chunks read",
                elapsed_secs => (Now - Start) / 1000_000_000.0}),
  Map = wait_for_completion(AllPids, #{}),
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

sleep_if_target_pid_mql_too_long(Pid, Threshold) ->
  {_, MQL} = process_info(Pid, message_queue_len),
  if MQL > Threshold ->
      %% logger:info(#{label => "MQL too long, sleeping",
      %%               target_pid => Pid}),
      timer:sleep(1),
      sleep_if_target_pid_mql_too_long(Pid, Threshold);
     true ->
      ok
  end.

read_chunks(FD, N, Offset, PrevChunk, BufSize, TargetPids) ->
  TargetPid = lists:nth((N rem length(TargetPids)) + 1, TargetPids),
  case prim_file:pread(FD, Offset, BufSize) of
    {ok, Bin} ->
      Size = byte_size(Bin),
      %% Read chunks pair-wise and split them so that each processed
      %% chunk is on an even newline boundary
      case binary:split(Bin, <<"\n">>) of
        [First, NextChunk] ->
          send_chunk(<<PrevChunk/binary, First/binary>>, TargetPid),
          %% sleep_if_target_pid_mql_too_long(TargetPid, 20),
          read_chunks(FD, N + 1, Offset + Size, NextChunk, BufSize, TargetPids);
        [Chunk] ->
          send_chunk(Chunk, TargetPid),
          read_chunks(FD, N + 1, Offset + Size, <<>>, BufSize, TargetPids)
      end;
    eof ->
      %% Reached end of file, process the last chunk
      send_chunk(PrevChunk, TargetPid),
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
                    result_size => maps:size(Map)}),
      wait_for_completion(Pids, merge_location_data(Map, NewMap));
    _ ->
      logger:error(#{label => "Unexpected message"})
  end.

merge_location_data(Map1, Map2) ->
  Stations = lists:usort(maps:keys(Map1) ++ maps:keys(Map2)),
  lists:foldl(
    fun(Station, Map) ->
        case {maps:get(Station, Map1, undefined),
              maps:get(Station, Map2, undefined)} of
          {Loc1, undefined} -> maps:put(Station, Loc1, Map);
          {undefined, Loc2} -> maps:put(Station, Loc2, Map);
          {Loc1, Loc2} ->
            {Min1, Max1, Count1, Sum1} = Loc1,
            {Min2, Max2, Count2, Sum2} = Loc2,
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
-record(chunk_state, { line_processor_pid
                     , count = 0
                     }).

chunk_processor() ->
  proc_lib:init_ack(self()),
  logger:info(#{label => "Chunk processor running"}),
  chunk_processor_loop(#chunk_state{}).

chunk_processor_loop(#chunk_state{count = _Count} = State) ->
  %% if Count rem 100 == 0 ->
  %%     logger:debug(#{chunks_processed => Count});
  %%    true -> ok
  %% end,
  %%{message_queue_len, MQL} = process_info(self(), message_queue_len),
  %%logger:info(#{mql => MQL}),

  receive
    {line_processor, Pid} ->
      chunk_processor_loop(State#chunk_state{line_processor_pid = Pid});
    {chunk, Chunk} ->
      State#chunk_state.line_processor_pid !
        {lines, binary:split(Chunk, [<<"\n">>, <<";">>], [global])},
      chunk_processor_loop(State#chunk_state{count = State#chunk_state.count + 1});
    eof ->
      logger:info(#{label => "Chunk processor finished."}),
      State#chunk_state.line_processor_pid ! eof,
      ok;
    _ ->
      logger:error(#{label => "Unexpected message"})
  end.

%%
%% The line processor
%%

-record(line_state, { map = #{}, result_pid = undefined }).

line_processor() ->
  proc_lib:init_ack(self()),
  line_processor_loop(#line_state{}).

line_processor_loop(State) ->
  receive
    {result_pid, Pid} ->
      line_processor_loop(State#line_state{result_pid = Pid});
    {lines, Lines} ->
      State0 = process_lines(Lines, State),
      line_processor_loop(State0);
    eof ->
      Map = maps:from_list(
              lists:filtermap(
                fun({{station, Station}, Data}) ->
                    {true, {Station, Data}};
                   (_) -> false
                end, get())),
      State#line_state.result_pid ! {result, self(), Map},
      {heap_size, HeapSize} = process_info(self(), heap_size),
      logger:info(#{label => "Line processor finished",
                    heap_size => HeapSize}),
      ok;
    _ ->
      logger:error(#{label => "Unexpected message"})
  end.

process_lines([], State) ->
  State;
process_lines([<<>>], State) ->
  State;
process_lines([Station, TempBin|Rest], State) ->
  Temp = parse_float(TempBin),
  Key = {station, Station},
  case get(Key) of
    undefined ->
      put(Key, { Temp % min
               , Temp % max
               , 1    % count
               , Temp % sum
               });

    {OldMin, OldMax, OldCount, OldSum} ->
      put(Key, { min(OldMin, Temp)
               , max(OldMax, Temp)
               , OldCount + 1
               , OldSum + Temp
               })

  end,
  State0 = State,
  process_lines(Rest, State0).

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
