-module(eredis_cluster_pool).
-behaviour(supervisor).

%% API.
-export([create/3]).
-export([stop/1]).
-export([transaction/2]).

%% Supervisor
-export([start_link/0]).
-export([init/1]).

-include("eredis_cluster.hrl").

-spec create(Host::string(), Port::integer(),atom()) ->
    {ok, PoolName::atom()} | {error, PoolName::atom()}.
create(Host, Port, Name) ->
    PoolName = get_name(Host, Port, Name),

    case whereis(PoolName) of
        undefined ->
            NodeInfo = application:get_env(message_store,Name,[]),
            DataBase = proplists:get_value(db, NodeInfo, 0),
            Password = proplists:get_value(password, NodeInfo, ""),
            WorkerArgs = [{host, Host},
                          {port, Port},
                          {database, DataBase},
                          {password, Password}
                         ],

            Size = proplists:get_value(pool_size, NodeInfo, 0),
            MaxOverflow = proplists:get_value(max_overflow, NodeInfo, 0),

            PoolArgs = [{name, {local, PoolName}},
                        {worker_module, eredis_cluster_pool_worker},
                        {size, Size},
                        {max_overflow, MaxOverflow}],

            ChildSpec = poolboy:child_spec(PoolName, PoolArgs, WorkerArgs),

            {Result, _} = supervisor:start_child(?MODULE,ChildSpec),
            {Result, PoolName};
        _ ->
            {ok, PoolName}
    end.

-spec transaction(PoolName::atom(), fun((Worker::pid()) -> redis_result())) ->
    redis_result().
transaction(PoolName, Transaction) ->
    try
        poolboy:transaction(PoolName, Transaction)
    catch
        exit:_ ->
            {error, no_connection}
    end.

-spec stop(PoolName::atom()) -> ok.
stop(PoolName) ->
    supervisor:terminate_child(?MODULE,PoolName),
    supervisor:delete_child(?MODULE,PoolName),
    ok.

-spec get_name(Host::string(), Port::integer(),atom()) -> PoolName::atom().
get_name(Host, Port, Name) ->
    list_to_atom(atom_to_list(Name) ++ "_" ++ Host ++ "#" ++ integer_to_list(Port)).

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([])
    -> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
    {ok, {{one_for_one, 1, 5}, []}}.
