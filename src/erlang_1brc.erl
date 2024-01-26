-module(erlang_1brc).

-feature(maybe_expr, enable).

-export([ main/1 %% Entrypoint for escript
        , run/1  %% Entrypoint for run.sh
        ]).

-compile({inline, [ {process_temp,2}
                  , {process_line,3}
                  ]}).

-define(BUFSIZE, 2 * 1024 * 1024).

options() ->
  [ {file,      $f, "file",      {string, "measurements.txt"}, "The input file."}
  , {eprof,     $e, "eprof",     undefined,                    "Run code under eprof."}
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

%% Allow any logger events to be printed to console before exiting.
flush() ->
  logger_std_h:filesync(default).

do_main(Opts) ->
  {Time, _} = timer:tc(fun() -> run(proplists:get_value(file, Opts)) end),
  Time.

run([Filename]) ->
  run(Filename);
run(Filename) when is_atom(Filename) ->
  run(atom_to_list(Filename));
run(Filename) ->
  {Time, _} = timer:tc(fun() -> map_cities(Filename) end),
  io:format("Mapped ~p citites in ~w ms~n",
            [length(get()),
             erlang:convert_time_unit(Time, microsecond, millisecond)]),
  try
    process_flag(trap_exit, true),
    case file:open(Filename, [raw, read, binary]) of
      {ok, FD} ->
        ProcessorPids = start_processors(),
        {ok, Bin} = file:pread(FD, 0, ?BUFSIZE),
        read_chunks(FD, 0, byte_size(Bin), Bin, ?BUFSIZE, ProcessorPids, length(ProcessorPids) * 3),
        Map = wait_for_completion(ProcessorPids, #{}),
        Fmt = format_final_map(Map),
        io:format("~ts~n", [Fmt]);
      {error, Reason} ->
        io:format("*** Failed to open ~ts: ~p~n", [Filename, Reason]),
        flush(),
        erlang:halt(1)
    end
  catch Class:Error:Stacktrace ->
      io:format("*** Caught exception: ~p~n", [{Class, Error, Stacktrace}]),
      flush(),
      erlang:halt(1)
  end.

map_cities(Filename) ->
  case file:open(Filename, [raw, read, binary]) of
    {ok, FD} ->
      {ok, Bin} = file:pread(FD, 0, ?BUFSIZE),
      map_cities0(Bin, 1);
    {error, Reason} ->
      io:format("*** Failed to open ~ts: ~p~n", [Filename, Reason]),
      flush(),
      erlang:halt(1)
  end.

-define(KEY(C, Acc), ((C * 17) bxor Acc) bsl 1).

station_key(Station) ->
  lists:foldl(fun(C, Acc) -> ?KEY(C, Acc) end,
              0, binary_to_list(Station)).

map_cities0(<<>>, _) ->
  ok;
map_cities0(Bin, N) ->
  maybe
    [First, Rest] ?= binary:split(Bin, <<"\n">>),
    [Station, _] ?= binary:split(First, <<";">>),
    Key = station_key(Station),
    case get({key, Key}) of
      undefined ->
        put({key, Key}, {station, Station}),
        map_cities0(Rest, N + 1);
      {station, Clash} when Clash =/= Station ->
        throw({name_clash, Key, Station, Clash});
      _ ->
        map_cities0(Rest, N + 1)
    end
  end.

%% Wait for processors to finish
wait_for_completion([], Map) ->
  Map;
wait_for_completion(Pids, Map) ->
  receive
    {'EXIT', Pid, normal} ->
      wait_for_completion(Pids -- [Pid], Map);
    {'EXIT', Pid, Other} ->
      %% Rethrow non-normal exits, they will be caught in run/1
      throw({unexpected_exit, Pid, Other});
    {result, Data} ->
      wait_for_completion(Pids, merge_location_data(Map, Data));
    give_me_more ->
      %% These are received when the chunk processors wants more data,
      %% but we have already consumed the entire file.
      wait_for_completion(Pids, Map)
  end.

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
      lists:sort(lists:map(
        fun({Station, {Min, Max, Count, Sum}}) ->
            Mean = Sum / Count,
            case get({key, Station}) of
              {station, StationBin} ->
                io_lib:format("~ts=~.1f/~.1f/~.1f",
                              [StationBin, Min/10, Mean/10, Max/10]);
              Other ->
                io:format("~p~n", [get()]),
                throw({failed_to_lookup_station, Other, Station})
            end
        end, maps:to_list(Map))))
    ++ "}".


%% Chunk processor. Receives chunks from the main process, and parses
%% them into temperatures.
start_processors() ->
  start_processors(erlang:system_info(logical_processors)).

start_processors(NumProcs) ->
  Self = self(),
  lists:foldl(
    fun(_, Pids) ->
        [spawn_link(fun() -> chunk_processor(Self) end)|Pids]
    end, [], lists:seq(1, NumProcs)).

chunk_processor(Pid) ->
  receive
    %% This only happens on the very last line.
    {chunk, [Chunk]} ->
      process_station(Chunk),
      Pid ! give_me_more,
      chunk_processor(Pid);
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
      chunk_processor(Pid);
    eof ->
      Map = maps:from_list(get()),
      Pid ! {result, Map},
      ok;
    Other ->
      throw({unexpected_message, self(), Other})
  end.

process_station(Station) ->
  process_station(Station, 0).

process_station(<<";", Rest/bitstring>>, Station) ->
  process_temp(Rest, Station);
process_station(<<C:8, Rest/bitstring>>, StationKey) ->
  process_station(Rest, ?KEY(C, StationKey));
process_station(Bin, _) ->
  Bin.

%% Specialized float parser for 2-digit floats with one fractional
%% digit.
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

process_line(Rest, Key, Temp) ->
  case get(Key) of
    undefined ->
      put(Key, {Temp, Temp, 1, Temp});
    {OldMin, OldMax, OldCount, OldSum} ->
      put(Key, {min(OldMin, Temp),
                max(OldMax, Temp),
                OldCount + 1,
                OldSum + Temp})
  end,
  case Rest of
    <<>> -> <<>>;
    <<"\n",NextStation/binary>> ->
      process_station(NextStation)
  end.
