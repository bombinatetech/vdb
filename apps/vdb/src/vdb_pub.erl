-module(vdb_pub).
-behaviour(gen_server).

-include("../include/vdb.hrl").

%% API
-export([start_link/1,
	install_store_table/2,
	handle_publish_msgs/4,
	show_table/1,
	waiting_for_acks/2,
	write_store/3,
   	puback/2,
	publish/2]).


%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(TABLE,vdb_pub_pool).

-record(state, {future_purpose
               }).

%%%===================================================================
%%% API
%%%===================================================================
start_link(I) ->
	gen_server:start_link(?MODULE, [I], []).



install_store_table(Nodes,Frag)->
%       mnesia:stop(),
%       mnesia:create_schema(Nodes),
%       mnesia:start(),
        mnesia:create_table(vdb_store,[
                    {frag_properties,[
                        {node_pool,Nodes},{hash_module,mnesia_frag_hash},
                        {n_fragments,Frag},
                        {n_disc_copies,length(Nodes)}]
                    },
                    {index,[]},{type,bag},
                    {attributes,record_info(fields,vdb_store)}]).

publish(SubscriberId,Msg)->
	call(SubscriberId,{publish,SubscriberId,Msg}).


puback(SubscriberId,MsgRef) ->
	call(SubscriberId,{puback,SubscriberId,MsgRef}).

waiting_for_acks(SubscriberId,Msgs)->
	call(SubscriberId,{waiting_for_acks,SubscriberId,Msgs}).



call(Key,Req) ->
        %case vdb_user_sup:get_rr_pid() of
        case vdb_pub_sup:get_server_pid(Key) of
                {ok,Pid} ->
                        gen_server:call(Pid, Req, infinity);
                Res ->
                        {no_process,Res}
        end.

show_table(Table_name)->
   Iterator =  fun(Rec,_)->
                    io:format("~p~n",[Rec]),
                     []
                 end,
     case mnesia:is_transaction() of
         true -> mnesia:foldl(Iterator,[],Table_name);
         false ->
             Exec = fun({Fun,Tab}) -> mnesia:foldl(Fun, [],Tab) end,
             mnesia:activity(transaction,Exec,[{Iterator,Table_name}],mnesia_frag)
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

handle_req({publish,SubscriberId,#vmq_msg{msg_ref=MsgRef,retain=Retain,
			routing_key=RoutingKey } = Msg},_State) ->
	Msg1 = Msg#vmq_msg{retain=false},
	case Retain of
	   true->
		store_retain(RoutingKey,Msg1);
	   _ ->
		ok
	end,
	route_publish(RoutingKey,MsgRef,Msg1);

handle_req({puback,SubscriberId,MsgRef},_State) ->
	%Before = vdb_table_if:read(vdb_store,{SubscriberId,MsgRef}),
	%MatchSpec = [{{vdb_store,{SubscriberId,MsgRef},SubscriberId,'$1'},[],['$1']}],
	%Val = vdb_table_if:select(vdb_store,MatchSpec),
	vdb_table_if:delete(vdb_store,{SubscriberId,MsgRef}),
	%After = vdb_table_if:read(vdb_store,{SubscriberId,MsgRef}),
        %io:format("puback after:~p~n",[After]),
	ok;

handle_req({waiting_for_acks,SubscriberId,Msg},_State) when is_list(Msg)->
	store_in_offline(SubscriberId,Msg);

handle_req({waiting_for_acks,SubscriberId,Msg},_State) ->
	store_in_offline(SubscriberId,[Msg]);

handle_req({no_session,SessionId,Msg},_State) ->
	Val = #vdb_users{subscriberId = '$1',status = '_',on_node='_',sessionId=SessionId},
 	MatchSpec = [{Val,[],['$1']}],
	SubscriberId = vdb_table_if:select(vdb_store,MatchSpec),
        store_in_offline(SubscriberId,[Msg]);

handle_req(_,_)->
	ok.

store_in_offline(SubscriberId,Msgs) ->
	[ vdb_table_if:write( vdb_store,#vdb_store{subscriberId = SubscriberId,vmq_msg = X} ) || 
		{_,_,X} <- Msgs ] .

store_retain(RoutingKey,Msg) ->
	Rec = #vdb_retain{topic = RoutingKey,vmq_msg = Msg},
	vdb_table_if:write(vdb_retain,Rec).

route_publish([Key1,Key2],MsgRef,Msg) ->
	route_publish1([Key1,<<"#">>],MsgRef,Msg);

route_publish(RoutingKey,MsgRef,Msg)->
	route_publish1(RoutingKey,MsgRef,Msg).

route_publish1(RoutingKey,MsgRef,Msg) ->
	case vdb_table_if:read(vdb_topics,[{RoutingKey,1}]) of
		[] ->
			{[],[]};
		Recs when is_list(Recs) ->
			session(Recs,MsgRef,Msg);
		Rec ->
		   case vdb_table_if:read(vdb_users,Rec#vdb_topics.subscriberId) of
			[] ->
				[];
			#vdb_users{status = online} = Usr  ->
				write_store(Rec#vdb_topics.subscriberId,MsgRef,Msg),
				ActiveUsers = [{Usr#vdb_users.on_node,[{Usr#vdb_users.subscriberId,
								Usr#vdb_users.sessionId}]}],
				InactiveUsers = [],
				{ActiveUsers,[]};
			#vdb_users{status = offline} = Usr  ->
				spawn(?MODULE,write_store,[Rec#vdb_topics.subscriberId,MsgRef,Msg]),
				{[],[Usr#vdb_users.subscriberId]}
		  end
	end.

session(Recs,MsgRef,Msg) ->
	UserTab = [vdb_table_if:read(vdb_users,X#vdb_topics.subscriberId) ||X <- Recs],
	InactiveUsers = [X#vdb_users.subscriberId || X <- UserTab,X#vdb_users.status == offline],
	%spawn(?MODULE,handle_offline_msgs,[InactiveUsers,MsgRef,Msg]),
	ActiveUsers = [{X#vdb_users.on_node,{X#vdb_users.subscriberId, X#vdb_users.sessionId}} || X <- UserTab, X#vdb_users.status == online ], 
 	handle_publish_msgs(ActiveUsers,InactiveUsers,MsgRef,Msg),
	Nodes = lists:usort([X#vdb_users.on_node || X <- UserTab, X#vdb_users.status == online]),
	ActiveSessions = [session_info(X,ActiveUsers) || X <- Nodes,Nodes =/= []],
	{ActiveSessions,InactiveUsers}.

session_info(_Node,[])->
	{};

session_info([],_)->
	{};

session_info(Node,ActiveUsers) ->
	SessionInfo = [Y || {X,Y} <- ActiveUsers,X=:= Node],
	{Node,SessionInfo}.

handle_publish_msgs([],[],_,_) ->
	ok;
handle_publish_msgs(ActiveUsers,InactiveUsers,MsgRef,Msg) ->
	[write_store(Y,MsgRef,Msg) || {_X,{Y,_Z}} <- ActiveUsers],
	[write_store(X,MsgRef,Msg) || X <- InactiveUsers],
	ok.

write_store(SubId,MsgRef,Msg)->
	Rec = #vdb_store{key = {SubId,MsgRef},subscriberId = SubId,vmq_msg = Msg},
	Val = vdb_table_if:write(vdb_store,Rec),
	io:format("write_store:~p~n",[Val]).
