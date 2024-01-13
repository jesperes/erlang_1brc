-module(aggregate).

-export([ aggregate_measurements/2
        , chunk_processor/0
        ]).

aggregate_measurements(Filename, Opts) ->
  process_flag(trap_exit, true),
  Start = erlang:monotonic_time(),
  BufSize = proplists:get_value(bufsize, Opts),
  {ok, FD} = prim_file:open(Filename, [read]),
  ProcessorPid = proc_lib:start_link(?MODULE, chunk_processor, []),
  read_chunks(FD, BufSize, ProcessorPid),
  Now = erlang:monotonic_time(),
  io:format("All chunks read after ~.2f seconds, waiting for chunk processor~n",
            [(Now - Start) / 1000_000_000.0]),
  wait_for_completion(ProcessorPid).

read_chunks(FD, BufSize, TargetPid) ->
  case prim_file:read(FD, BufSize) of
    {ok, Bin} ->
      process_chunk(Bin, TargetPid),
      read_chunks(FD, BufSize, TargetPid);
    eof ->
      TargetPid ! eof,
      ok
  end.

process_chunk(Chunk, TargetPid) ->
  TargetPid ! {chunk, Chunk}.

chunk_processor() ->
  proc_lib:init_ack(self()),
  chunk_processor_loop().

chunk_processor_loop() ->
  receive
    {chunk, Chunk} ->
      process_chunk(Chunk),
      chunk_processor_loop();
    eof -> ok;
    Other ->
      throw({unexpected, Other})
  end.

wait_for_completion(ProcessorPid) ->
  receive
    {'EXIT', Pid, normal} when Pid =:= ProcessorPid ->
      io:format("Processor pid finished.~n", []);
    {'EXIT', Pid, Reason} ->
      io:format("*** Unexpected crash in ~p: ~p~n", [Pid, Reason]),
      erlang:halt(1);
    Other ->
      io:format("*** Unexpected return >>> ~p~n", [Other]),
      wait_for_completion(ProcessorPid),
      erlang:halt(1)
  end.

process_chunk(Chunk) ->
  io:format("Chunk processor received ~p bytes of data~n", [byte_size(Chunk)]),
  timer:sleep(100).
