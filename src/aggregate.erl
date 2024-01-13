-module(aggregate).

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
  {ok, FD} = prim_file:open(Filename, [read]),
  NumProcessors = erlang:system_info(schedulers) div 2,
  {ProcessorPids, AllPids} = start_processors(NumProcessors),
  read_chunks(FD, 0, 0, <<>>, BufSize, ProcessorPids),
  Now = erlang:monotonic_time(),
  logger:info(#{label => "All chunks read",
                elapsed_secs => (Now - Start) / 1000_000_000.0}),
  wait_for_completion(AllPids).

start_processors(NumProcs) ->
  logger:info(#{label => "Starting processing pipelines",
                num_pipelines => NumProcs}),
  lists:foldl(
    fun(_, {ProcessorPids, AllPids}) ->
        LineProcessorPid = proc_lib:start_link(?MODULE, line_processor, []),
        ProcessorPid = proc_lib:start_link(?MODULE, chunk_processor, []),
        ProcessorPid ! {line_processor, LineProcessorPid},
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
          send_chunk(<<PrevChunk/binary, First/binary>>, TargetPid),
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

wait_for_completion([]) ->
  logger:info(#{label => "All subprocesses finished"}),
  ok;
wait_for_completion(Pids) ->
  receive
    {'EXIT', Pid, normal} ->
      wait_for_completion(Pids -- [Pid]),
      ok;
    _ ->
      logger:error(#{label => "Unexpected message"})
  end.

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
line_processor() ->
  proc_lib:init_ack(self()),
  line_processor_loop().

line_processor_loop() ->
  receive
    {lines, Lines} ->
      process_lines(Lines),
      line_processor_loop();
    eof ->
      logger:info(#{label => "Line processor finished"}),
      ok;
    _ ->
      logger:error(#{label => "Unexpected message"})
  end.

process_lines([]) ->
  ok;
process_lines([<<>>]) ->
  ok;
process_lines([_Station, Temp|Rest]) ->
  _ = parse_float(Temp),
  process_lines(Rest).

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
