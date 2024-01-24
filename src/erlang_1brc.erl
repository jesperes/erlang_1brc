-module(erlang_1brc).

-export([ main/1
        , run/1
        , chunk_processor/0
        ]).

-compile({inline,[{process_temp,2},{process_line,3}]}).

-define(BUFSIZE, 2 * 1024 * 1024).

options() ->
  [ {file,      $f, "file",      {string, "measurements.txt"}, "The input file."}
  , {eprof,     $e, "eprof",     undefined,                    "Run code under eprof."}
  , {bufsize,   $c, "bufsize",   {integer, 2 * 1024 * 1024},   "Buffer size."}
  , {parallel,  $p, "parallel",  integer,                      "Number of parallel processing pipelines."}
  ].

main(Args) ->
  {ok, {Opts, []}} = getopt:parse(options(), Args),

  Time =
    case proplists:get_value(eprof, Opts) of
      true ->
        logger:info(#{label => "Enabling eprof"}),
        eprof:start(),
        eprof:start_profiling(erlang:processes()),
        T = do_main(Opts),
        eprof:stop_profiling(),
        eprof:analyze(),
        eprof:stop(),
        T;
      _ ->
        do_main(Opts)
    end,

  io:format("Finished, time = ~w seconds~n",
            [erlang:convert_time_unit(Time, microsecond, second)]).

do_main(Opts) ->
  run(proplists:get_value(file, Opts)).

run(Filename) ->
  process_flag(trap_exit, true),
  {ok, FD} = file:open(Filename, [raw, read, binary]),
  NumProcessors = erlang:system_info(logical_processors),
  {ProcessorPids, AllPids} = start_processors(NumProcessors),
  {ok, Bin} = file:pread(FD, 0, ?BUFSIZE),
  read_chunks(FD, 0, byte_size(Bin), Bin, ?BUFSIZE, ProcessorPids, NumProcessors * 3),
  Map = wait_for_completion(AllPids, #{}),
  Fmt = format_final_map(Map),
  io:format("~ts~n", [Fmt]).

start_processors(NumProcs) ->
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
  TargetPid ! {chunk, Chunk}.

wait_for_completion([], Map) ->
  Map;
wait_for_completion(Pids, Map) ->
  receive
    {'EXIT', Pid, normal} ->
      wait_for_completion(Pids -- [Pid], Map);
    {result, _SenderPid, NewMap} ->
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
    try
        chunk_processor_loop(undefined)
    catch E:R:ST ->
            logger:error(#{ crashed => {E,R,ST} })
    end.

chunk_processor_loop(Pid) ->
  receive
    {result_pid, NewPid} ->
      chunk_processor_loop(NewPid);
    {chunk, [Chunk]} ->
      process_station(Chunk),
          Pid ! give_me_more,
      chunk_processor_loop(Pid);
    {chunk, [First, Second]} ->
          case process_station(First) of
              <<>> ->
                  ok;
              {Rest, Station} ->
                  process_temp(<<Rest/binary, Second/binary>>, Station);
              Rest ->
                  process_station(<<Rest/binary, Second/binary>>)
          end,
          Pid ! give_me_more,
      chunk_processor_loop(Pid);
    eof ->
      Map = maps:from_list(get()),
      Pid ! {result, self(), Map},
      ok;
    M ->
      io:format("Unexpected message: ~w~n", [M])
  end.

%%
%% The line processor
%%

process_station(Station) ->
    process_station(Station, Station, 0).
process_station(Bin, <<";", Rest/bitstring>>, Cnt) ->
    <<Station:Cnt/binary, _/bitstring>> = Bin,
    process_temp(Rest, Station);
process_station(Bin, <<_:8, Rest/bitstring>>, Cnt) ->
    process_station(Bin, Rest, Cnt + 1);
process_station(Bin, _, _Cnt) ->
    Bin.

-define(TO_NUM(C), (C - $0)).

process_temp(<<$-, A, B, $., C, Rest/binary>>, Station) ->
    process_line(Rest, Station, -1 * (?TO_NUM(A) * 100 + ?TO_NUM(B) * 10 + ?TO_NUM(C)));
process_temp(<<$-, B, $., C, Rest/binary>>, Station) ->
    process_line(Rest, Station, -1 * (?TO_NUM(B) * 10 + ?TO_NUM(C)));
process_temp(<<A, B, $., C, Rest/binary>>, Station) ->
    process_line(Rest, Station, ?TO_NUM(A) * 100 + ?TO_NUM(B) * 10 + ?TO_NUM(C));
process_temp(<<B, $., C, Rest/binary>>, Station) ->
    process_line(Rest, Station, ?TO_NUM(B) * 10 + ?TO_NUM(C));
process_temp(Rest, Station) ->
    {Rest, Station}.

process_line(Rest, Station, Temp) ->
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
    case Rest of
        <<>> -> <<>>;
        <<"\n",NextStation/binary>> ->
            process_station(NextStation)
    end.
