-module(erlang_1brc).

-export([main/1]).

-include_lib("eunit/include/eunit.hrl").

options() ->
  [ {file,      $f, "file",     {string, "measurements.txt"}, "The input file."}
  , {io_bench,  $i, "io-bench", string,                       "Perform I/O benchmarking"}
  , {repeat,    $n, "repeat",   {integer, 1},                 "Number of iterations."}
  , {eprof,     $e, "eprof",    undefined,                    "Run code under eprof."}
  , {bufsize,   $c, "bufsize",  {integer, 2 * 1024 * 1024},   "Buffer size."}
  , {log_level, $l, "log_level", {atom, info},                "Log level."}
  , {no_output, undefined, "no_output", undefined,            "Do not print output to stdout."}
  ].

main(Args) ->
  logger:update_formatter_config(
    default,
    #{ legacy_header => false
     , single_line => true
     , template => [time, " ", level, ": ", pid, ": ", msg, "\n"]
     }),

  case getopt:parse(options(), Args) of
    {ok, {Opts, []}} ->
      LogLevel = proplists:get_value(log_level, Opts),
      logger:update_primary_config(#{level => LogLevel}),

      Iters = proplists:get_value(repeat, Opts),
      Time =
        case proplists:get_value(eprof, Opts) of
          true ->
            logger:info(#{label => "Enabling eprof"}),
            eprof:start(),
            eprof:start_profiling(erlang:processes()),
            T = bench(fun() -> do_main(Opts) end, Iters),
            eprof:stop_profiling(),
            eprof:analyze(),
            eprof:stop(),
            T;
          _ ->
            bench(fun() -> do_main(Opts) end, Iters)
        end,

      logger:info(#{label => "Finished",
                    elapsed_secs => Time / 1_000_000.0,
                    iterations => Iters});

    {error, Reason} ->
      io:format("Failed to parse options: ~p~n", [Reason]),
      io:format("~p~n", [getopt:usage(options(), escript:script_name())])
  end,
  logger_std_h:filesync(default).


do_main(Opts) ->
  Filename = proplists:get_value(file, Opts),
  case proplists:get_value(io_bench, Opts, false) of
    false ->
      aggregate:aggregate_measurements(Filename, Opts);
    Type ->
      ?debugVal(Type),
      do_io_bench(list_to_atom(Type), Opts, Filename)
  end.

do_io_bench(readfile, _Opts, Filename) ->
  {ok, _Bin} = file:read_file(Filename);
do_io_bench(primfileread, _Opts, Filename) ->
  Size = filelib:file_size(Filename),
  {ok, FD} = prim_file:open(Filename, [read]),
  {ok, _Bin} = prim_file:read(FD, Size),
  prim_file:close(FD);
do_io_bench(primfile_chunks, Opts, Filename) ->
  read_chunks(Filename, default, Opts);
do_io_bench(primfile_chunks_iterate, Opts, Filename) ->
  read_chunks(Filename, iterate, Opts);
do_io_bench(iterate, _Opts, Filename) ->
  {ok, Bin} = file:read_file(Filename),
  iterate(Bin).

%% Run Fun() `Iters` times and return the average execution time in
%% microseconds.
bench(Fun, Iters) ->
  L = lists:seq(1, Iters),
  {Time, _} = timer:tc(fun() -> [Fun() || _ <- L] end),
  trunc(Time / Iters).

iterate(<<>>) ->
  ok;
iterate(<<_, Rest/binary>>) ->
  iterate(Rest).

read_chunks(Filename, Type, Opts) ->
  {ok, FD} = prim_file:open(Filename, [read]),
  do_read_chunks(FD, Type, Opts),
  prim_file:close(FD).

do_read_chunks(FD, Type, Opts) ->
  BufSize = proplists:get_value(bufsize, Opts),
  case prim_file:read(FD, BufSize) of
    eof -> ok;
    {ok, _Bin} when Type =:= default -> do_read_chunks(FD, Type, Opts);
    {ok, Bin} when Type =:= iterate ->
      do_read_chunks(FD, Type, Opts),
      iterate(Bin)
  end.
