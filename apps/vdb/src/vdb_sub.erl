-module(vdb_sub).
-behaviour(gen_server).

-include("../include/vdb.hrl").

%% API
-export([start_link/1,
	install_subs_table/2,
	add_sub/2,
	del_sub/2]).


%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(TABLE,vdb_sub_pool).

-record(state, {future_purpose
               }).

%%%===================================================================
%%% API
%%%===================================================================
start_link(I) ->
	gen_server:start_link(?MODULE, [I], []).



install_subs_table(Nodes,Frag)->
%       mnesia:stop(),
%       mnesia:create_schema(Nodes),
%       mnesia:start(),
        mnesia:create_table(vdb_topics,[
                    {frag_properties,[
                        {node_pool,Nodes},{hash_module,mnesia_frag_hash},
                        {n_fragments,Frag},
                        {n_disc_copies,length(Nodes)}]
                    },
                    {index,[]},{type,bag},
                    {attributes,record_info(fields,vdb_topics)}]).

add_sub(SubscriberId,Topics)->
	Val = call(SubscriberId,{add_sub,SubscriberId,Topics}),
	Val.

del_sub(SubscriberId,Topics)->
        call(SubscriberId,{del_sub,SubscriberId,Topics}).


call(Key,Req) ->
        case vdb_sub_sup:get_server_pid(Key) of
                {ok,Pid} ->
                        gen_server:call(Pid, Req, infinity);
                Res ->
                        {no_process,Res}
        end.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([I]) ->
	ets:insert(?TABLE, {self()}),
	{ok,#state{future_purpose = I}}.
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State) ->
    {reply, handle_req(Request, State), State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
     ets:delete(?TABLE,self()),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_req({add_sub,SubscriberId,[{RoutingKey,1}] = Topics},_State) ->
   Rec = #vdb_topics{subscriberId = SubscriberId,topic = Topics},
   vdb_table_if:write(vdb_topics,Rec),
   case vdb_table_if:read(vdb_retain,RoutingKey) of
	[] ->
		[];
	Rec1 ->
		Rec1#vdb_retain.vmq_msg
   end;
   
handle_req({del_sub,SubscriberId,Topics},_State) ->
   vdb_table_if:delete(vdb_topics,{Topics,SubscriberId});

handle_req({read_sub,Key},_State) ->
   vdb_table_if:read(vdb_topics,Key);


handle_req(_Othre,_)->
	ok.


