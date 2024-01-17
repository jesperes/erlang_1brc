-module(aggregate2).

-export([ aggregate_measurements/2
        , chunk_processor/0
        ]).

-compile({inline,[{process_temp,2},{process_line,3}]}).

-include_lib("eunit/include/eunit.hrl").

aggregate_measurements(Filename, Opts) ->
  process_flag(trap_exit, true),
  Start = erlang:monotonic_time(),
  BufSize = proplists:get_value(bufsize, Opts),
  logger:info(#{bufsize => BufSize}),
  {ok, FD} = prim_file:open(Filename, [read]),
  NumProcessors =
    proplists:get_value(parallel, Opts, erlang:system_info(schedulers)),
  {ProcessorPids, AllPids} = start_processors(NumProcessors),
  {ok, Bin} = prim_file:pread(FD, 0, BufSize),
  read_chunks(FD, 0, byte_size(Bin), Bin, BufSize, ProcessorPids),
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
    {chunk, [Chunk]} ->
      process_station(Chunk),
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

process_station(<<Station:1/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:2/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:3/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:4/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:5/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:6/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:7/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:8/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:9/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:10/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:11/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:12/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:13/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:14/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:15/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:16/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:17/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:18/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:19/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:20/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:21/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:22/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:23/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:24/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:25/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:26/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:27/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:28/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:29/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(<<Station:30/binary,";",Rest/binary>>) ->
    process_temp(Rest, Station);
process_station(Rest) ->
    %% %% Debug
    %% case binary:split(Rest, [<<";">>]) of
    %%     [Station, Temp] ->
    %%         throw(Station);
    %%     _ -> ok
    %% end,
    Rest.

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
