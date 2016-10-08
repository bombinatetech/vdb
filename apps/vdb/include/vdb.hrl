%%MQTT database

%%Subscription data

-record(vdb_topics,{topic,
		   subscriberId }).

-record(vdb_store,{key,subscriberId,
                        vmq_msg }).


-record(vdb_users,{subscriberId,
			 status,
                         on_node,
			sessionId,
			ts }).

-record(vdb_retain,{topic,
                   vmq_msg }).

-record(vmq_msg, {
          msg_ref,               
          routing_key,           
          payload,               
          retain=false,          
          dup=false,             
          qos,                   
          trade_consistency=false, 
          reg_view=vmq_reg_trie,   
          mountpoint,            
          persisted=false       
         }).
