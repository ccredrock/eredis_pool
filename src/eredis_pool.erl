%%%-------------------------------------------------------------------
%%% @author Hiroe Shin <shin@mac-hiroe-orz-17.local>
%%% @copyright (C) 2011, Hiroe Shin
%%% @doc
%%%
%%% @end
%%% Created :  9 Oct 2011 by Hiroe Shin <shin@mac-hiroe-orz-17.local>
%%%-------------------------------------------------------------------
-module(eredis_pool).

%% Include
-include_lib("eunit/include/eunit.hrl").

%% Default timeout for calls to the client gen_server
%% Specified in http://www.erlang.org/doc/man/gen_server.html#call-3
-define(TIMEOUT, 5000).

-define(ETS, '$eredis_pool_ets').

%% API
-export([start_ets/0]).
-export([start/0, stop/0]).
-export([q/1, q/2, q/3, qp/1, qp/2, qp/3, transaction/1, transaction/2,
         create_pool/2, create_pool/3, create_pool/4, create_pool/5,
         create_pool/6, create_pool/7, 
         delete_pool/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

start_ets() ->
    ets:new(?ETS, [named_table, public, {read_concurrency, true}]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

%% ===================================================================
%% @doc create new pool.
%% @end
%% ===================================================================
-spec(create_pool(PoolName::atom(), Size::integer()) -> 
             {ok, pid()} | {error,{already_started, pid()}}).

create_pool(PoolName, Size) ->
    eredis_pool_sup:create_pool(PoolName, Size, []).

-spec(create_pool(PoolName::atom(), Size::integer(), Host::string()) -> 
             {ok, pid()} | {error,{already_started, pid()}}).

create_pool(PoolName, Size, Host) ->
    eredis_pool_sup:create_pool(PoolName, Size, [{host, Host}]).

-spec(create_pool(PoolName::atom(), Size::integer(), 
                  Host::string(), Port::integer()) -> 
             {ok, pid()} | {error,{already_started, pid()}}).

create_pool(PoolName, Size, Host, Port) ->
    eredis_pool_sup:create_pool(PoolName, Size, [{host, Host}, {port, Port}]).

-spec(create_pool(PoolName::atom(), Size::integer(), 
                  Host::string(), Port::integer(), Database::string()) -> 
             {ok, pid()} | {error,{already_started, pid()}}).

create_pool(PoolName, Size, Host, Port, Database) ->
    eredis_pool_sup:create_pool(PoolName, Size, [{host, Host}, {port, Port},
                                                 {database, Database}]).

-spec(create_pool(PoolName::atom(), Size::integer(), 
                  Host::string(), Port::integer(), 
                  Database::string(), Password::string()) -> 
             {ok, pid()} | {error,{already_started, pid()}}).

create_pool(PoolName, Size, Host, Port, Database, Password) ->
    eredis_pool_sup:create_pool(PoolName, Size, [{host, Host}, {port, Port},
                                                 {database, Database},
                                                 {password, Password}]).

-spec(create_pool(PoolName::atom(), Size::integer(), 
                  Host::string(), Port::integer(), 
                  Database::string(), Password::string(),
                  ReconnectSleep::integer()) -> 
             {ok, pid()} | {error,{already_started, pid()}}).

create_pool(PoolName, Size, Host, Port, Database, Password, ReconnectSleep) ->
    eredis_pool_sup:create_pool(PoolName, Size, [{host, Host}, {port, Port},
                                                 {database, Database},
                                                 {password, Password},
                                                 {reconnect_sleep, ReconnectSleep}]).


%% ===================================================================
%% @doc delet pool and disconnected to Redis.
%% @end
%% ===================================================================
-spec(delete_pool(PoolName::atom()) -> ok | {error,not_found}).

delete_pool(PoolName) ->
    eredis_pool_sup:delete_pool(PoolName).

%%--------------------------------------------------------------------
%% @doc
%% Executes the given command in the specified connection. The
%% command must be a valid Redis command and may contain arbitrary
%% data which will be converted to binaries. The returned values will
%% always be binaries.
%% @end
%%--------------------------------------------------------------------
-spec q(Command::iolist()) -> {ok, binary() | [binary()]} | {error, Reason::binary()}.

q(Command) ->
    q(Command, ?TIMEOUT).

-spec q(Command::iolist(), Timeout::integer()) -> {ok, binary() | [binary()]} | {error, Reason::binary()}.

q(Command, Timeout) ->
    q(do_get_pool(), Command, Timeout).

q(PoolName, Command, Timeout) ->
    pb_transaction(PoolName, fun(Worker) -> eredis:q(Worker, Command, Timeout) end).

pb_transaction(PoolName, F) ->
    try
        case poolboy:transaction(PoolName, F) of
            {error, Reason} -> do_check_switch(), {error, Reason};
            {ok, Result} -> {ok, Result}
        end
    catch
        C:R -> do_check_switch(), {error, {C, R}}
    end.

-spec qp(Command::iolist(), Timeout::integer()) ->
               {ok, binary() | [binary()]} | {error, Reason::binary()}.

qp(Pipeline) ->
    qp(Pipeline, ?TIMEOUT).

qp(Pipeline, Timeout) ->
    qp(do_get_pool(), Pipeline, Timeout).

qp(PoolName, Pipeline, Timeout) ->
    pb_transaction(PoolName, fun(Worker) -> eredis:qp(Worker, Pipeline, Timeout) end).

transaction(List) when is_list(List) ->
    qp([["MULTI"] | List] ++ [["EXEC"]]);
transaction(Fun) when is_function(Fun) ->
    transaction(do_get_pool(), Fun).

transaction(PoolName, Fun) when is_function(Fun) ->
    F = fun(C) ->
                try
                    {ok, <<"OK">>} = eredis:q(C, ["MULTI"]),
                    Fun(C),
                    eredis:q(C, ["EXEC"])
                catch Klass:Reason ->
                        {ok, <<"OK">>} = eredis:q(C, ["DISCARD"]),
                        io:format("Error in redis transaction. ~p:~p", 
                                  [Klass, Reason]),
                        {Klass, Reason}
                end
        end,
    pb_transaction(PoolName, F).

do_get_pool() ->
    case ets:lookup(?ETS, redis_pool) of
        [{_, Pool}] -> Pool;
        [] -> %% after start server first get pool
            {ok, List} = application:get_env(eredis_pool, pools),
            do_select_master(List)
    end.

do_select_master([Pool | T]) ->
    case do_active_pool(Pool) of
        skip -> do_select_master(T);
        PoolName -> PoolName
    end;
do_select_master([]) -> null.

do_active_pool({PoolName, _, _}) ->
    case do_master_pool(PoolName) of
        false -> skip;
        true -> ets:insert(?ETS, {redis_pool, PoolName}), PoolName
    end.

do_master_pool(PoolName) ->
    case eredis_pool:q(PoolName, ["INFO"], ?TIMEOUT) of
        {ok, Bin} -> length(re:split(Bin, "role:master")) > 1;
        _F -> false
    end.

do_check_switch() ->
    Now = rtcdr_util:tsms(),
    case ets:lookup(?ETS, redis_check) of
        [{_, Next}] when Now < Next -> skip;
        _ ->
            ets:insert(?ETS, {redis_check, Now + ?TIMEOUT}),
            do_active_pool(do_push_switch())
    end.

do_push_switch() ->
    case ets:lookup(?ETS, redis_switch) of
        [{_, [H | T]}] ->
            ets:insert(?ETS, {redis_switch, T}), H;
        _ ->
            {ok, [H | T]} = application:get_env(eredis_pool, pools),
            ets:insert(?ETS, {redis_switch, T}), H
    end.

