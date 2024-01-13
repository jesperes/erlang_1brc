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
  io:format("All chunks read after ~p secs, waiting for chunk processor~n",
            [erlang:convert_time_unit(Now - Start, native, second)]),
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
  io:format("Processing chunk of size ~p, sending to ~p~n", [byte_size(Chunk), TargetPid]),
  TargetPid ! {chunk, Chunk}.

chunk_processor() ->
  proc_lib:init_ack(self()),
  io:format("Started chunk processor: ~p~n", [self()]),
  chunk_processor_loop().

chunk_processor_loop() ->
  receive
    {chunk, Chunk} ->
      io:format("Chunk processor received ~p~n", [byte_size(Chunk)]),
      chunk_processor_loop();
    eof ->
      %% No more chunks to process
      ok;
    Other ->1
      io:format("Other: ~p~n", [Other]),
      chunk_processor_loop()
  end.

wait_for_completion(ProcessorPid) ->
  receive
    {'EXIT', Pid, normal} when Pid =:= ProcessorPid ->
      io:format("Processor pid finished.~n", []);
    X ->
      io:format(">>> ~p~n", [X]),
      wait_for_completion(ProcessorPid)
  end.
