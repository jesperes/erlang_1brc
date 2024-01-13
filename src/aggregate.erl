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
  LineProcessorPid = proc_lib:start_link(?MODULE, line_processor, []),
  ProcessorPid = proc_lib:start_link(?MODULE, chunk_processor, []),
  ProcessorPid ! {line_processor, LineProcessorPid},
  read_chunks(FD, 0, <<>>, BufSize, ProcessorPid),
  Now = erlang:monotonic_time(),
  logger:info(#{label => "All chunks read",
                elapsed_secs => (Now - Start) / 1000_000_000.0}),
  wait_for_completion(LineProcessorPid).

read_chunks(FD, Offset, PrevChunk, BufSize, TargetPid) ->
  case prim_file:pread(FD, Offset, BufSize) of
    {ok, Bin} ->
      Size = byte_size(Bin),
      %% Read chunks pair-wise and split them so that each processed
      %% chunk is on an even newline boundary
      case binary:split(Bin, <<"\n">>) of
        [First, NextChunk] ->
          send_chunk(<<PrevChunk/binary, First/binary>>, TargetPid),
          read_chunks(FD, Offset + Size, NextChunk, BufSize, TargetPid);
        [Chunk] ->
          send_chunk(Chunk, TargetPid),
          read_chunks(FD, Offset + Size, <<>>, BufSize, TargetPid)
      end;
    eof ->
      %% Reached end of file, process the last chunk
      send_chunk(PrevChunk, TargetPid),
      TargetPid ! eof,
      ok
  end.

send_chunk(Chunk, TargetPid) ->
  TargetPid ! {chunk, Chunk}.

chunk_processor() ->
  proc_lib:init_ack(self()),
  logger:info(#{label => "Chunk processor running"}),
  chunk_processor_loop(undefined).

chunk_processor_loop(State) ->
  receive
    {line_processor, LineProcessorPid} ->
      chunk_processor_loop(LineProcessorPid);
    {chunk, Chunk} ->
      process_chunk(Chunk, State),
      chunk_processor_loop(State);
    eof ->
      State ! eof,
      ok;
    _ ->
      logger:error(#{label => "Unexpected message"})
  end.

wait_for_completion(WaitForPid) ->
  receive
    {'EXIT', Pid, normal} when Pid =:= WaitForPid ->
      logger:info(#{label => "Pid finished", pid => Pid}),
      ok;
    {'EXIT', Pid, normal} ->
      logger:info(#{label => "Pid finished", pid => Pid}),
      wait_for_completion(WaitForPid);
    _ ->
      logger:error(#{label => "Unexpected message"})
  end.

process_chunk(Chunk, LineProcessorPid) ->
  Lines = binary:split(Chunk, <<"\n">>, [global]),
  LineProcessorPid ! {lines, Lines}.

line_processor() ->
  proc_lib:init_ack(self()),
  logger:info(#{label => "Line processor running"}),
  line_processor_loop().

line_processor_loop() ->
  receive
    {lines, Lines} ->
      lists:foreach(fun process_line/1, Lines),
      line_processor_loop();
    eof ->
      ok;
    _ ->
      logger:error(#{label => "Unexpected message"})
  end.

process_line(_Line) ->
  ok.
