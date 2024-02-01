-module(erlang_1brc).

-feature(maybe_expr, enable).

-export([main/1]).

%% These inlinings are actually necessary. On my machine, it yields a
%% 15-20% performance improvement.
-compile({inline, [ {process_temp, 2}
                  , {process_line, 3}
                  , {process_station, 2}
                  ]}).

%% This is the size of the chunks we read from the input file at a
%% time.  There is some overhead "stitching" buffers together, so this
%% should be large enough to keep the worker threads busy in the
%% process_* functions.
-define(BUFSIZE, 2 * 1024 * 1024).

%% Fnv32 hash function constants
-define(FNV32_PRIME, 16777619).
-define(FNV32_OFFSET, 2166136261).
-define(FNV32_MASK, 16#ffffffff).

-define(FNV32_HASH(C, Hash), (((Hash * ?FNV32_PRIME) bxor C) band ?FNV32_MASK)).

input_filename([Filename]) ->
  Filename;
input_filename([]) ->
  "measurements.txt".

main(Args) ->
  {Time, _} = timer:tc(fun() -> run(input_filename(Args)) end),
  io:format("Elapsed: ~f seconds~n", [Time / 1000000.0]),
  Time.

run(Filename) ->
  process_flag(trap_exit, true),
  process_flag(message_queue_data, off_heap),
  process_flag(priority, high),

  Workers = start_workers(),
  read_chunks(Filename, Workers),
  Map = wait_for_completion(Workers, #{}),
  StationNames = map_station_names(Map, Filename),
  print_results(Map, StationNames).

read_station_name(Bin) ->
  maybe
    [First, Rest] ?= binary:split(Bin, <<"\n">>),
    [Name, _] ?= binary:split(First, <<";">>),
    {Name, Rest}
  else
    _ -> false
  end.

%% The data received from the workers uses FNV32 hashes as keys,
%% so to be able to print the actual station names, we need to
%% scan the file from the beginning to find each station name.
%%
%% Note that this is vulnerable to badly formed input files. If we
%% have a input file where there is a station name with only one
%% reading as the very last line, this will cause this step to have to
%% scan the *entire* file to find all the station names.
map_station_names(Map, Filename) ->
  {ok, FD} = file:open(Filename, [raw, read, binary]),
  {ok, Bin} = file:pread(FD, 0, ?BUFSIZE),
  map_station_names(Map, FD, Bin, #{}).

map_station_names(Map, FD, Bin, StationNames) ->
  case maps:size(Map) of
    0 -> StationNames;
    _ ->
      case read_station_name(Bin) of
        {StationName, Rest} ->
          Key = fnv32_hash(StationName),
          case maps:is_key(Key, StationNames) of
            true ->
              map_station_names(Map, FD, Rest, StationNames);
            false ->
              map_station_names(
                maps:remove(Key, Map), FD, Rest,
                maps:put(Key, StationName, StationNames))
          end;
        false ->
          {ok, More} = file:pread(FD, 0, ?BUFSIZE),
          map_station_names(Map, FD, <<Bin/binary, More/binary>>, StationNames)
      end
  end.

print_results(Map, StationNames) ->
  Str = "{" ++
    lists:join(
      ", ",
      lists:sort(lists:map(
        fun({Key, {Min, Max, Count, Sum}}) ->
            Mean = Sum / Count,
            StationName = maps:get(Key, StationNames),
            io_lib:format("~ts=~.1f/~.1f/~.1f",
                          [StationName, Min/10, Mean/10, Max/10])
        end, maps:to_list(Map))))
    ++ "}",
  io:format("~ts~n", [Str]).

fnv32_hash(Binary) ->
  fnv32_hash(Binary, ?FNV32_OFFSET).
fnv32_hash(<<>>, Hash) ->
  Hash;
fnv32_hash(<<C, Rest/binary>>, Hash) ->
  fnv32_hash(Rest, ?FNV32_HASH(C, Hash)).

%% Wait for workers to finish
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
      %% These are received when the workers wants more data, but we
      %% have already consumed the entire file.
      wait_for_completion(Pids, Map)
  end.

read_chunks(Filename, Workers) ->
  {ok, FD} = file:open(Filename, [raw, read, binary]),
  {ok, Bin} = file:pread(FD, 0, ?BUFSIZE),
  read_chunks(FD, 0, byte_size(Bin), Bin, ?BUFSIZE, Workers, length(Workers) * 3).

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


start_workers() ->
  start_workers(erlang:system_info(logical_processors)).

start_workers(NumProcs) ->
  process_flag(message_queue_data, off_heap),
  process_flag(priority, high),
  Options = [link,
             {min_heap_size, 1024*1024},
             {min_bin_vheap_size, 1024*1024},
             {max_heap_size, (1 bsl 59) -1}
            ],
  Self = self(),
  io:format("Starting ~p parallel workers~n", [NumProcs]),
  lists:foldl(
    fun(_, Pids) ->
        [spawn_opt(fun() -> worker_loop(Self) end, Options)|Pids]
    end, [], lists:seq(1, NumProcs)).

worker_loop(Pid) ->
  receive
    %% This only happens on the very last line.
    {chunk, [Chunk]} ->
      process_station(Chunk),
      Pid ! give_me_more,
      worker_loop(Pid);
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
      worker_loop(Pid);
    eof ->
      Map = maps:from_list(get()),
      Pid ! {result, Map},
      ok;
    Other ->
      throw({unexpected_message, self(), Other})
  end.

process_station(Station) ->
  process_station(Station, ?FNV32_OFFSET).

process_station(<<";", Rest/bitstring>>, Hash) ->
  process_temp(Rest, Hash);
process_station(<<C:8, Rest/bitstring>>, Hash) ->
  process_station(Rest, ?FNV32_HASH(C, Hash));
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
  %% This return breaks the match context reuse optimization, but it
  %% is only executed at the end of each chunk, so it doesn't really
  %% matter much. The first four clauses here are the hot ones, together
  %% with the two first ones in process_station/2.
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
