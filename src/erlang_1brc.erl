-module(erlang_1brc).

-export([main/1]).

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
  Filename = proplists:get_value(file, Opts),
    Backend = proplists:get_value(backend, Opts),
  {Time, _} = timer:tc(fun() -> Backend:aggregate_measurements(Filename, Opts) end),
  Time.
