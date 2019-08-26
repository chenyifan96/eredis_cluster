-module(eredis_cluster_sup).
-behaviour(supervisor).

%% Supervisor.
-export([start_link/0,
         start_link/1]).
-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_link(Name) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Name]).

-spec init([])
    -> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([Name]) ->
    MonitorStringName = atom_to_list(Name)++ "_" ++ "eredis_cluster_monitor",
    MonitorName = list_to_atom(MonitorStringName),
    Procs = [ {MonitorName,
                {eredis_cluster_monitor, start_link, [Name]},
                permanent, 5000, worker, [dynamic]}
            ],
    {ok, {{one_for_one, 1, 5}, Procs}}.
