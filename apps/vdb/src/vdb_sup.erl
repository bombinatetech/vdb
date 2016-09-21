%%%-------------------------------------------------------------------
%% @doc vdb top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-define(CHILD(I, Type, Args), {I, {I, start_link, Args},
                               permanent, 5000, Type, [I]}).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    {ok, { {one_for_one, 5, 10},
            [
               ?CHILD(vdb_user_sup, supervisor, []),
               ?CHILD(vdb_sub_sup, supervisor, []),
               ?CHILD(vdb_pub_sup, supervisor, [])
              ]} }.

%%====================================================================
%% Internal functions
%%====================================================================
