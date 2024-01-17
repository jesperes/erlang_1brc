-module(erlang_1brc).

-export([main/1]).

options() ->
  [ {file,      $f, "file",     {string, "measurements.txt"}, "The input file."}
  , {eprof,     $e, "eprof",    undefined,                    "Run code under eprof."}
  , {bufsize,   $c, "bufsize",  {integer, 2 * 1024 * 1024},   "Buffer size."}
  , {log_level, $l, "log_level", {atom, info},                "Log level."}
  , {no_output, undefined, "no_output", undefined,            "Do not print output to stdout."}
  , {parallel,  $p, "parallel", integer,                      "Number of parallel processing pipelines."}
  , {backend, $b, "backend", {atom, aggregate}, "The backend to use"}
  ].

main(Args) ->
  logger:update_formatter_config(
    default,
    #{ legacy_header => true
     , single_line => false
     , template => ["== ", time, " ", level, " ", pid, " ==\n", msg, "\n\n"]
     }),

  {ok, {Opts, []}} = getopt:parse(options(), Args),
  LogLevel = proplists:get_value(log_level, Opts),
  logger:update_primary_config(#{level => LogLevel}),

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

  logger:info(#{label => "Finished",
                elapsed_secs => Time / 1_000_000.0}),
  logger_std_h:filesync(default).

do_main(Opts) ->
  Filename = proplists:get_value(file, Opts),
    Backend = proplists:get_value(backend, Opts),
  {Time, _} = timer:tc(fun() -> Backend:aggregate_measurements(Filename, Opts) end),
  Time.
