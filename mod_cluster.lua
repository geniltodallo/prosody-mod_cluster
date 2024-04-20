-- Mod Cluster for Prosody
-- Copyright (C) 2023-2024 Genilto Dallo 
-- GENILTO DALLO - 05/10/2023
--
-- This project is MIT/X11 licensed. Please see the
--

local jid_split = require "util.jid".split;
local jid_bare = require "util.jid".bare;
local set = require "util.set";
local st = require "util.stanza";
local fire_event = prosody.events.fire_event;
local hosts = prosody.hosts;

local remote_servers = module:get_option("cluster_servers", {});
local node_name = module:get_option("cluster_node_name", nil);

if not node_name then
    error("cluster_node_name not configured.", 0);
end

module:log("info", "%s added to cluster %s", module.host, node_name);


if not prosody.cluster_users then
    prosody.cluster_users = {};
  end

local cluster_users = prosody.cluster_users;


local bare_sessions = prosody.bare_sessions;

function splitHostAndPort(inputstr)
    local host, port = inputstr:match("([^:]+):([^:]+)")
    return host, port
end

function get_room_from_jid(jid)
    local node, host = jid_split(jid);
    local component = hosts[host];
    if component then
            local muc = component.modules.muc
            if muc and rawget(muc,"rooms") then
                    -- We're running 0.9.x or 0.10 (old MUC API)
                    return muc.rooms[jid];
            elseif muc and rawget(muc,"get_room_from_jid") then
                    -- We're running >0.10 (new MUC API)
                    return muc.get_room_from_jid(jid);
            else
                    return
            end
    end
end

local function get_local_rooms(host)
    local rooms = set.new();
    for room in hosts[host].modules.muc.live_rooms() do
        rooms:add(room.jid);
    end
    return rooms;
end

local function is_local_room(room_jid)
    local node, host, _ = jid_split(room_jid);
    if not node then return false; end

    if not hosts[host] or not hosts[host].modules.muc then
        return false;
    end
    for room in hosts[host].modules.muc.live_rooms() do
        if room.jid == room_jid then return true; end
    end
    return false;
end

local function get_members(muc_room)
    module:log("debug", "Funcao get_members room.affiliations %s", muc_room);
    local res = set.new();
    if muc_room and muc_room._affiliations then
            for o_jid, _ in pairs(muc_room._affiliations) do
                    module:log("debug", "Funcao get_members AFFILIATION JID: %s", o_jid);
                    res:add(o_jid);
            end
    end
    return res;
end

local function handle_room_event(event)
    --EVENTO GRUPOS
    --Verificar lista de usuarios, se tem algum usuario da lista desse grupo que esta online em outro cluster.
    --enviar a mensagem para o node.

    local to = event.stanza.attr.to;
    local node, host, _ = jid_split(to);
    local rhost, room_jid;

    module:log("debug", "looking up target room node for %s", to);

    if node then
        room_jid = jid_bare(to);
        if is_local_room(room_jid) then
            module:log("debug", "room[%s] is hosted here. Nothing to do", room_jid);
            
            local muc_room = get_room_from_jid(room_jid);

            local muc_members = get_members(muc_room);

            local clusters_enviar = {};
            
            local jid_from = jid_bare(event.stanza.attr.from);
            local stanza_c = st.clone(event.stanza);


            --TODO FAZER A BUSCA EM UMA THREAD..
            for user_jid in muc_members do
                module:log("debug", "VERIFICANDO node MEMBER[%s]", user_jid);
                --lista de membros, ver quem é de outro node e esta online (se esta na lista do node esta online).
                local enviar = true;

                if prosody.bare_sessions[user_jid] then
                    module:log("debug", user_jid.." has a session here, nothing to do");
                    enviar = false;
                end
            
                --deliver_message(event);
                module:log("debug", "looking up target node for "..user_jid);
            
                -- node pode ter mais de um node/servidor....
                -- fazer um array de node/servidores que precisa enviar
                -- TODO: definir estrutura array separar index por host ou index por server e host?
                -- Tem ideia de concatenar o index [@host1.net-@cluster1.host1.net]
                -- [@host1.net-@cluster1.host1.net] [@host1.net-@cluster2.host1.net]
                --


                local user_nodes = cluster_users[user_jid];
                if not user_nodes then
                    module:log("debug","node lookup for "..user_jid.." failed!");
                    return nil;
                end
            
                --For every node
                for _, node in pairs(user_nodes) do
                    if node == node_name then
                        module:log("debug", "we are node "..node..", nothing to do");
                    else
                        module:log("debug", "target node for "..user_jid.." is "..node);
                        fire_event("cluster/send", { node = node, host = host, stanza = event.stanza });
                        clusters_enviar[node] = node;
                        enviar = true;
                    end
                end


                -- local node = cluster_users[user_jid];

                -- if not node then
                --     module:log("debug","node lookup for "..user_jid.." failed!");
                --     enviar = false;
                -- else 
                --     module:log("debug", "target node for "..user_jid.." is "..node);
                --     clusters_enviar[node] = node;
                --     enviar = true;

                -- end
            
           
                -- if node == node_name then
                --     module:log("debug", "we are cluster "..node..", nothing to do");
                --     enviar = false;
                -- end

            end

            if clusters_enviar then
                for clusterz, _ in pairs(clusters_enviar) do
                    module:log("debug", "ENVIANDO STANZA:", tostring(stanza_c));
                    fire_event("cluster/send", { node = clusterz, host = host, stanza = stanza_c });
                end
                

            end

            return false;
        end
    end

    --fire_event("cluster/send", { node = rhost, stanza = event.stanza });
    return false;
end

-- Send carbon copy to local session if has jid from
local function send_carbon_copy(stanza, node)
	local orig_type = stanza.attr.type;
    local jid_from = stanza.attr.from;
    local bare_jid_from = jid_bare(jid_from);
	local user_sessions = bare_sessions[bare_jid_from];
    
    local xmlns_carbons = "urn:xmpp:carbons:2";
    local xmlns_forward = "urn:xmpp:forward:0";

    local copy = st.clone(stanza);
    copy.attr.xmlns = "jabber:client";
    local carbon = st.message{ from = jid_from, type = orig_type, }
        :tag("sent", { xmlns = xmlns_carbons })
            :tag("forwarded", { xmlns = xmlns_forward })
                :add_child(copy):reset();
    carbon.attr.to = bare_jid_from;
    
    module:log("debug", "Sending carbon to remote user %s", bare_jid_from);
    fire_event("cluster/send", { node = node, host = node, stanza = carbon });


    --TODO
    -- local user_sessions = bare_sessions[bare_jid_from];
    -- for _, session in pairs(user_sessions.sessions) do   
    --     -- Carbons are sent to resources that have enabled it
    --     if session.want_carbons then
end


local function handle_msg(event)
    local to = event.stanza.attr.to;
    local from = event.stanza.attr.from;
    local username, host = jid_split(to);
    local stanza = event.stanza;
    

    -- <presence xml:lang='en' from='dev2@hostx.net/xxx.2.3_e7cifh'><show>away</show><status>away</status><x xmlns='vcard-temp:x:update'><photo/></x></presence>
    module:log("debug", "mod_cluster: Processing stanza:", tostring(stanza));


    if not host then
        return nil
    end

    module:log("debug", host.." host do user, host do modulo:" ..module.host);


    if hosts[host].modules.muc then
        module:log("debug", "to MUC %s detected", host);
        return handle_room_event(event);
    end


    if host ~= module.host then
        module:log("debug", host.." host do user diferente do modulo:" ..module.host);
        return nil
    end

    local jid = jid_bare(to);

    local jid_from = jid_bare(from);

    if stanza.name == "presence" then
        module:log("debug", "STANZA.PRESENCE ---------------");

        if stanza.attr.type == "probe" then
                -- module:log("debug", "Nao encaminhar presence probe");
                -- return nil
        end

    end

    --deliver_message(event);
    module:log("debug", "looking up target node for "..jid);

    -- TODO FUTURE IDEA:
    -- user_sessions = { resource, node };
    -- [{ app, cluster1.host1.com}, { desk, cluster2.host1.com}, { desk2, cluster3.host1.com }]

    -- Carbon:
    --TODO Verificar se sessao do user tem carbon
    --Gerar carbon aqui
    local user_nodes_carbon = cluster_users[jid_from];
    if user_nodes_carbon then
        --For every node
        for _, node in pairs(user_nodes_carbon) do
            if node == node_name then
                module:log("debug", "we are node carbon? "..node..", nothing to do");
            else
                send_carbon_copy(stanza, node);            
            end
        end
    end

    
    local user_nodes = cluster_users[jid];
    if not user_nodes then
        module:log("debug","node lookup for "..jid.." failed!");
        return nil;
    end
    --For every node
    for _, node in pairs(user_nodes) do
        if node == node_name then
            module:log("debug", "we are node "..node..", nothing to do");
        else
            module:log("debug", "target node for "..jid.." is "..node);
            fire_event("cluster/send", { node = node, host = host, stanza = event.stanza });
        end
    end

    if prosody.bare_sessions[jid] then
        module:log("debug", jid.." has a session here, nothing more to do");
        --Usuario pode ter sessao remota e mais de um nó remoto!
        --TODO ver como salva isso
        return nil
    end
       
    --presences
    --<presence type='probe' from='dev2@hostx.net/hostx.2.3_34i44h' to='gd3@hostx.net'/> -- desabilitar probe
    --<presence from='dev2@hostx.net/hostx.2.3_34i44h' xml:lang='en'><show>online</show><status>online</status><x xmlns='vcard-temp:x:update'><photo/></x><delay from='hostx.net' stamp='2023-10-12T14:02:58Z' xmlns='urn:xmpp:delay'/></presence>
    --<presence from='dev2@hostx.net/hostx.2.3_34i44h' to='dev@hostx.net' xml:lang='en'><show>away</show><status>away</status><x xmlns='vcard-temp:x:update'><photo/></x></presence>

    -- local stanza_c = st.clone(event.stanza);
    --             stanza_c.attr.to = node..'@'..host..'/'..r;
    --             module:log("debug", "target node for %s is %s",
    --                 stanza_c.attr.to ,node);
    --             fire_event("cluster/send", { node = node, stanza = stanza_c });
                

    return true; --nao processar mais a mensagem, esta salvando como offline pois user nao tem session here
    -- precisa processar ACK! ack com prioridade 1001 processou antes do mod_cluster

end

local function handleUserConnected (event) 
    local session = event.session;
    local username = session.username;
    --	local from_jid = session.full_jid or (username .. "@" .. module.host);
    --local jid = jid_bare(session.full_jid);

    local jid = session.full_jid;




    for key, srv in pairs(remote_servers) do
		
        module:log("debug", "mod_cluster: Sending user session available to remote server:" .. srv);
        --criar um pacote xmpp de sessao do user
        --<cluster type='available' node_from='cluster1.hostx.net' from='gd@hostx.net' xmlns='urn:xmpp:cluster'/>
        userSessionStanza = st.stanza("cluster", { xmlns = 'urn:xmpp:cluster'});
        userSessionStanza.attr.type = "available";
        userSessionStanza.attr.from = jid;

        local host, port = splitHostAndPort(srv)
        fire_event("cluster/send", { node = host, host = host, stanza = userSessionStanza });

    end
    
end

local function handleUserDisconnected (event) 
    local session = event.session;
    local username = session.username;
    local jid = jid_bare(session.full_jid);

    for key, srv in pairs(remote_servers) do
		
        module:log("debug", "mod_cluster: Sending user session unavailable to remote server:" .. srv);
        --criar um pacote xmpp de sessao do user
        --<cluster type='unavailable' node_from='cluster1.hostx.net' from='gd@hostx.net' xmlns='urn:xmpp:cluster'/>
        userSessionStanza = st.stanza("cluster", { xmlns = 'urn:xmpp:cluster'});
        userSessionStanza.attr.type = "unavailable";
        userSessionStanza.attr.from = jid;

        local host, port = splitHostAndPort(srv)
        fire_event("cluster/send", { node = host, host = host, stanza = userSessionStanza });

    end

end

local function handleConnectedToCluster (event) 
    local remoteHost = event.node;
    module:log("debug", "mod_cluster: handleConnectedToCluster: " ..remoteHost);
end

-- se conectou ao cluster remoto, requisitar usuarios, e enviar usuarios locais
local function handleConnectedToCluster_old (event) 

    local remoteHost = event.cluster;
    module:log("debug", "mod_cluster: handleConnectedToCluster: " ..remoteHost);

    --probe for remote users
	userSessionStanza = st.stanza("cluster", { xmlns = 'urn:xmpp:cluster'});
	userSessionStanza.attr.type = "probe";
	userSessionStanza.attr.node_from = node_name;
	userSessionStanza.attr.node_to = remoteHost;

	fire_event("cluster/send", { node = remoteHost, host = remoteHost, stanza = userSessionStanza });
	--conn:write(tostring(userSessionStanza));

	--send local users
	for jid, session in pairs(bare_sessions) do
		
        module:log("debug", "mod_cluster: Sending user session available to remote server:" .. remoteHost);
        --criar um pacote xmpp de sessao do user
        --<cluster type='available' node_from='cluster1.host.net' from='gd@host.net' xmlns='urn:xmpp:cluster'/>
        userSessionStanza = st.stanza("cluster", { xmlns = 'urn:xmpp:cluster'});
        userSessionStanza.attr.type = "available";
        userSessionStanza.attr.from = jid;

        fire_event("cluster/send", { node = remoteHost, host = remoteHost, stanza = userSessionStanza });

    end

end

local function handleDisconnectedToCluster (event) 

    local remoteHost = event.node;
    module:log("debug", "mod_cluster: handleDisconnectedToCluster: " ..remoteHost);

end

local function handle_msg_thread(event) 

    --handle_msg(event);
    local thread1 = coroutine.create(handle_msg);
    return coroutine.resume(thread1, event);

end

-- IMPORTANTE: FOI TENTANDO HANDLE_MSG_THREAD COM COROUTINE, PRECISA RETORNAR O VALOR DA OUTRA FUNCAO, CONTINUOU PROCESSANDO STANZAS MESMO COM RETURN TRUE
-- module:hook("pre-message/full", handle_msg_thread, 1001); -- precisa ser prioridade 1 aqui no cluster e 2 no mod_log_messages sei la nao funciona nada
-- module:hook("pre-message/bare", handle_msg_thread, 1001); -- precisa ser prioridade 1 aqui no cluster e 2 no mod_log_messages nao funcoina nada 1000 1 coco xixi
-- module:hook("pre-presence/bare", handle_msg_thread, 1001);
-- module:hook("pre-presence/full", handle_msg_thread, 1001);

module:hook("pre-message/full", handle_msg, 1); 
module:hook("pre-message/bare", handle_msg, 1); 
module:hook("pre-presence/bare", handle_msg, 1000);
module:hook("pre-presence/full", handle_msg, 1000);
module:hook("resource-bind", handleUserConnected);
module:hook("resource-unbind", handleUserDisconnected);

-- 09/04/2024
-- Com 1000 cluster e 2 no log_messages, chamou duas vezes - full e bare? - 1001 ficou por primeiro, ack segundo, log_messages terceiro offline storage quarto.
-- Com 1 cluster e 2 no log_messages, chamou uma vez só. - ack em primeiro, log messages em segundo, cluster em terceiro, offline em quarto
-- Com prioridade 1000 acima de log_messages, nao gravou pra quem enviou. so pra quem recebeu. Certo é 1 aqui e 2 no log_messages.

--module:hook_global("cluster/connectedToCluster", handleConnectedToCluster, 1000); --dispara pra cada dominio
--module:hook_global("cluster/disconnectedToCluster", handleDisconnectedToCluster, 1000); -- dispara pra cada dominio

module:log("debug", "hooked at %s", module:get_host());


-- PROSODY EVENT PRIORITIES:
-- -1 Delivery to the session only
-- 0 "Read-only" stanza interception, for stanzas that are going to be delivered, e.g. for archiving. Do not modify or drop the stanza in priority 0 handlers.
-- 1 - 999 For general processing modules that may modify or drop stanzas
-- 1000+ Pre-processing filters, typically used to drop stanzas before they are seen by most modules.
