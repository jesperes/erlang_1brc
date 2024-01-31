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
  Workers = start_workers(),
  read_chunks(Filename, Workers),
  io:format("Finished reading input file, waiting for workers to finish.~n", []),
  Map = wait_for_completion(Workers, #{}),
  print_results(Map).

print_results(Map) ->
  Str = "{" ++
    lists:join(
      ", ",
      lists:sort(lists:map(
        fun({Station, {Min, Max, Count, Sum}}) ->
            Mean = Sum / Count,
            Station0 = list_to_binary(lists:reverse(binary_to_list(Station))),
            io_lib:format("~ts=~.1f/~.1f/~.1f",
                          [Station0, Min/10, Mean/10, Max/10])
        end, maps:to_list(Map))))
    ++ "}",
  io:format("~ts~n", [Str]).

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
  Self = self(),
  io:format("Starting ~p parallel workers~n", [NumProcs]),
  lists:foldl(
    fun(_, Pids) ->
        [spawn_link(fun() -> worker_loop(Self) end)|Pids]
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
  process_station(Station, <<>>).

process_station(<<";", Rest/bitstring>>, Station) ->
  process_temp(Rest, Station);
process_station(<<C:8, Rest/bitstring>>, Station) ->
  process_station(Rest, <<C, Station/binary>>);
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
