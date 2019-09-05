-module(eredis_cluster_monitor).
-behaviour(gen_server).

%% API.
-export([start_link/0]).
-export([start_link/1]).
-export([connect/2]).
-export([refresh_mapping/2]).
-export([get_state/1, get_state_version/1]).
-export([get_pool_by_slot/2, get_pool_by_slot/3]).
-export([get_all_pools/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% Type definition.
-include("eredis_cluster.hrl").
-record(state, {
    init_nodes :: [#node{}],
    slots :: tuple(), %% whose elements are integer indexes into slots_maps
    slots_maps :: tuple(), %% whose elements are #slots_map{}
    version :: integer()
}).

%% API.
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

start_link(Name) ->
    ServerName = get_server_name(Name),
    gen_server:start_link({local,ServerName}, ?MODULE, [Name], []).

connect(InitServers, Name) ->
    ServerName = get_server_name(Name),
    gen_server:call(ServerName,{connect,InitServers, Name}).

refresh_mapping(Version, Name) ->
    ServerName = get_server_name(Name),
    gen_server:call(ServerName,{reload_slots_map,Version,Name}).

%% =============================================================================
%% @doc Given a slot return the link (Redis instance) to the mapped
%% node.
%% @end
%% =============================================================================

-spec get_state(atom()) -> #state{}.
get_state(Name) ->
    EtsName = get_server_name(Name),
    [{cluster_state, State}] = ets:lookup(EtsName, cluster_state),
    State.

get_state_version(State) ->
    State#state.version.

-spec get_all_pools(atom()) -> [pid()].
get_all_pools(Name) ->
    State = get_state(Name),
    SlotsMapList = tuple_to_list(State#state.slots_maps),
    [SlotsMap#slots_map.node#node.pool || SlotsMap <- SlotsMapList,
        SlotsMap#slots_map.node =/= undefined].

%% =============================================================================
%% @doc Get cluster pool by slot. Optionally, a memoized State can be provided
%% to prevent from querying ets inside loops.
%% @end
%% =============================================================================
-spec get_pool_by_slot(Slot::integer(), State::#state{},atom())->
    {PoolName::atom() | undefined, Version::integer()}.
get_pool_by_slot(Slot, State, _Name) -> 
    Index = element(Slot+1,State#state.slots),
    Cluster = element(Index,State#state.slots_maps),
    if
        Cluster#slots_map.node =/= undefined ->
            {Cluster#slots_map.node#node.pool, State#state.version};
        true ->
            {undefined, State#state.version}
    end.

-spec get_pool_by_slot(Slot::integer(),atom()) ->
    {PoolName::atom() | undefined, Version::integer()}.
get_pool_by_slot(Slot, Name) ->
    State = get_state(Name),
    get_pool_by_slot(Slot, State, Name).

maybe_reload_slots_map(State, Name) ->
    try
        NewState =  
            reload_slots_map(State, Name),
        {ok,NewState}
    catch
        {error,cannot_connect_to_cluster} ->
            {error,State};
        _Other ->
            {error,State}
    end.

-spec reload_slots_map(State::#state{},atom()) -> NewState::#state{}.
reload_slots_map(State, Name) ->
    [close_connection(SlotsMap)
        || SlotsMap <- tuple_to_list(State#state.slots_maps)],

    ClusterSlots = get_cluster_slots(State#state.init_nodes,Name),

    SlotsMaps = parse_cluster_slots(ClusterSlots),
    ConnectedSlotsMaps = connect_all_slots(SlotsMaps,Name),
    Slots = create_slots_cache(ConnectedSlotsMaps),

    NewState = State#state{
        slots = list_to_tuple(Slots),
        slots_maps = list_to_tuple(ConnectedSlotsMaps),
        version = State#state.version + 1
    },
    EtsName = get_server_name(Name),
    true = ets:insert(EtsName, [{cluster_state, NewState}]),

    NewState.

-spec get_cluster_slots([#node{}],atom()) -> [[bitstring() | [bitstring()]]].
get_cluster_slots([], _Name) ->
    throw({error,cannot_connect_to_cluster});
get_cluster_slots([Node|T], Name) ->
    case safe_eredis_start_link(Node#node.address, Node#node.port, Name) of
        {ok,Connection} ->
          case eredis:q(Connection, ["CLUSTER", "SLOTS"]) of
            {error,<<"ERR unknown command 'CLUSTER'">>} ->
                get_cluster_slots_from_single_node(Node);
            {error,<<"ERR This instance has cluster support disabled">>} ->
                get_cluster_slots_from_single_node(Node);
            {ok, ClusterInfo} ->
                eredis:stop(Connection),
                ClusterInfo;
            _ ->
                eredis:stop(Connection),
                get_cluster_slots(T,Name)
        end;
        _ ->
            get_cluster_slots(T,Name)
  end.

-spec get_cluster_slots_from_single_node(#node{}) ->
    [[bitstring() | [bitstring()]]].
get_cluster_slots_from_single_node(Node) ->
    [[<<"0">>, integer_to_binary(?REDIS_CLUSTER_HASH_SLOTS-1),
    [list_to_binary(Node#node.address), integer_to_binary(Node#node.port)]]].

-spec parse_cluster_slots([[bitstring() | [bitstring()]]]) -> [#slots_map{}].
parse_cluster_slots(ClusterInfo) ->
    parse_cluster_slots(ClusterInfo, 1, []).

parse_cluster_slots([[StartSlot, EndSlot | [[Address, Port | _] | _]] | T], Index, Acc) ->
    SlotsMap =
        #slots_map{
            index = Index,
            start_slot = binary_to_integer(StartSlot),
            end_slot = binary_to_integer(EndSlot),
            node = #node{
                address = binary_to_list(Address),
                port = binary_to_integer(Port)
            }
        },
    parse_cluster_slots(T, Index+1, [SlotsMap | Acc]);
parse_cluster_slots([], _Index, Acc) ->
    lists:reverse(Acc).



-spec close_connection(#slots_map{}) -> ok.
close_connection(SlotsMap) ->
    Node = SlotsMap#slots_map.node,
    if
        Node =/= undefined ->
            try eredis_cluster_pool:stop(Node#node.pool) of
                _ ->
                    ok
            catch
                _ ->
                    ok
            end;
        true ->
            ok
    end.

-spec connect_node(#node{},atom()) -> #node{} | undefined.
connect_node(Node, Name) ->
    case eredis_cluster_pool:create(Node#node.address, Node#node.port,Name) of
        {ok, Pool} ->
            Node#node{pool=Pool};
        _ ->
            undefined
    end.

safe_eredis_start_link(Address,Port, Name) ->
    process_flag(trap_exit, true),
    NodeInfo = application:get_env(message_store,Name,[]),
    DataBase = proplists:get_value(db, NodeInfo, 0),
    Password = proplists:get_value(password, NodeInfo, ""),
    Payload = eredis:start_link(Address, Port, DataBase, Password),
    process_flag(trap_exit, false),
    Payload.

-spec create_slots_cache([#slots_map{}]) -> [integer()].
create_slots_cache(SlotsMaps) ->
  SlotsCache = [[{Index,SlotsMap#slots_map.index}
        || Index <- lists:seq(SlotsMap#slots_map.start_slot,
            SlotsMap#slots_map.end_slot)]
        || SlotsMap <- SlotsMaps],
  SlotsCacheF = lists:flatten(SlotsCache),
  SortedSlotsCache = lists:sort(SlotsCacheF),
  [ Index || {_,Index} <- SortedSlotsCache].

-spec connect_all_slots([#slots_map{}],atom()) -> [integer()].
connect_all_slots(SlotsMapList,Name) ->
    [SlotsMap#slots_map{node=connect_node(SlotsMap#slots_map.node,Name)}
        || SlotsMap <- SlotsMapList].

-spec connect_([{Address::string(), Port::integer()}],atom()) -> #state{}.
connect_([],_Name) ->
    #state{};
connect_(InitNodes,Name) ->
    State = #state{
        slots = undefined,
        slots_maps = {},
        init_nodes = [#node{address = A, port = P} || {A,P} <- InitNodes],
        version = 0
    },

    reload_slots_map(State, Name).

%% gen_server.

init([Name]) ->
    EtsName = get_server_name(Name),
    ets:new(EtsName, [protected, set, named_table]),
    NodeInfo = application:get_env(message_store, Name, []),
    InitNodes = get_host_port(NodeInfo),
    {ok, connect_(InitNodes, Name)}.

handle_call({reload_slots_map,Version, Name}, _From, #state{version=Version} = State) ->
    case maybe_reload_slots_map(State, Name) of
        {ok,NewState} ->
            {reply, ok, NewState};
        {error, NewState} ->
            {reply, error, NewState}
    end;
handle_call({reload_slots_map,_,_Name}, _From, State) ->
    {reply, ok, State};
handle_call({connect, InitServers,Name}, _From, _State) ->
    {reply, ok, connect_(InitServers, Name)};
handle_call({get_pool_by_slot, Slot, Name}, _From, State) ->
    Result = get_pool_by_slot(Slot,Name),
    {reply,Result,State};
handle_call({get_state,Name}, _From, State) ->
    {reply,get_state(Name),State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%
%%%internal
%%%%%%%%%%%%%%%%%%%%%%%
get_host_port(Args) ->
    Host = proplists:get_value(host, Args, undefined),
    Port = proplists:get_value(port, Args, undefined),
    case (Host /= undefined) and (Port =/= undefined) of
        true ->
            [{Host, Port}];
        false ->
            HostPortList = proplists:get_value(host_port_list, Args, []),
            HostPortList
    end.

get_server_name(Name) ->
    StringName = atom_to_list(Name)++ "_" ++ "eredis_cluster_monitor",
    ServerName = list_to_atom(StringName),
    ServerName.



