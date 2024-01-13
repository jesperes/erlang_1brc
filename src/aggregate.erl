-module(aggregate).

-export([ aggregate_measurements/2
        , chunk_processor/0
        , line_processor/0
        ]).

-include_lib("eunit/include/eunit.hrl").

aggregate_measurements(Filename, Opts) ->
  process_flag(trap_exit, true),
  Start = erlang:monotonic_time(),
  BufSize = proplists:get_value(bufsize, Opts),
  {ok, FD} = prim_file:open(Filename, [read]),
  LineProcessorPid1 = proc_lib:start_link(?MODULE, line_processor, []),
  LineProcessorPid2 = proc_lib:start_link(?MODULE, line_processor, []),
  ProcessorPid1 = proc_lib:start_link(?MODULE, chunk_processor, []),
  ProcessorPid2 = proc_lib:start_link(?MODULE, chunk_processor, []),
  ProcessorPid1 ! {line_processor, LineProcessorPid1},
  ProcessorPid2 ! {line_processor, LineProcessorPid2},
  read_chunks(FD, 0, 0, <<>>, BufSize, [ProcessorPid1, ProcessorPid2]),
  Now = erlang:monotonic_time(),
  logger:info(#{label => "All chunks read",
                elapsed_secs => (Now - Start) / 1000_000_000.0}),
  wait_for_completion(
   [ ProcessorPid1
   , ProcessorPid2
   , LineProcessorPid1
   , LineProcessorPid2
   ]).

read_chunks(FD, N, Offset, PrevChunk, BufSize, TargetPids) ->

  TargetPid = lists:nth((N rem length(TargetPids)) + 1, TargetPids),
  %% logger:info(#{label => "Reading chunk",
  %%               target_pid => TargetPid}),
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
  logger:info(#{label => "Waiting for subprocesses to finish",
                pids => Pids}),
  receive
    {'EXIT', Pid, normal} ->
      logger:info(#{label => "Pid finished", pid => Pid}),
      wait_for_completion(Pids -- [Pid]),
      ok;
    _ ->
      logger:error(#{label => "Unexpected message"})
  end.

-record(chunk_state, { line_processor_pid
                     }).

chunk_processor() ->
  proc_lib:init_ack(self()),
  logger:info(#{label => "Chunk processor running"}),
  chunk_processor_loop(#chunk_state{}).

chunk_processor_loop(State) ->
  receive
    {line_processor, Pid} ->
      chunk_processor_loop(State#chunk_state{line_processor_pid = Pid});
    {chunk, Chunk} ->
      process_chunk(Chunk, State),
      chunk_processor_loop(State);
    eof ->
      logger:info(#{label => "Chunk processor finished."}),
      State#chunk_state.line_processor_pid ! eof,
      ok;
    _ ->
      logger:error(#{label => "Unexpected message"})
  end.

process_chunk(Chunk, State) ->
  Lines = binary:split(Chunk, [<<"\n">>, <<";">>], [global]),
  State#chunk_state.line_processor_pid ! {lines, Lines}.

line_processor() ->
  proc_lib:init_ack(self()),
  logger:info(#{label => "Line processor running"}),
  line_processor_loop().

line_processor_loop() ->
  receive
    {lines, _Lines} ->
      %% process_lines(Lines),
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
process_lines([_Station, _Temp|Rest]) ->
  %% logger:info(#{label => "Processing line",
  %%               station => Station,
  %%               temp => Temp}),
  process_lines(Rest).
