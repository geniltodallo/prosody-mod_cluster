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
local cluster_name = module:get_option("cluster_name", nil);

if not cluster_name then
    error("cluster_name not configured.", 0);
end

module:log("info", "%s added to cluster %s", module.host, cluster_name);


if not prosody.cluster_users then
    prosody.cluster_users = {};
  end

local cluster_users = prosody.cluster_users;


local bare_sessions = prosody.bare_sessions;

function splitHostAndPort(inputstr)
    local host, port = inputstr:match("([^:]+):([^:]+)")
    return host, port
end


local function deliver_message(event)
    local to = event.stanza.attr.to;
    local from = event.stanza.attr.from;
    local username, host = jid_split(to);
    local jid = username.."@"..host;

    module:log("debug", "looking up target cluster for "..jid);

    local cluster = cluster_users[jid]
    if not cluster then
        module:log("debug","cluster lookup for "..jid.." failed!");
        return nil;
    end

    module:log("debug", "target cluster for "..jid.." is "..cluster);

    if cluster == cluster_name then
        module:log("debug", "we are cluster "..cluster..", nothing to do");
        return nil;
    end

    -- local stanza_c = st.clone(event.stanza);
    --             stanza_c.attr.to = node..'@'..host..'/'..r;
    --             module:log("debug", "target cluster for %s is %s",
    --             stanza_c.attr.to ,cluster);
    --             fire_event("cluster/send", { cluster = cluster, stanza = stanza_c });
                

    fire_event("cluster/send", { cluster = cluster, host = host, stanza = event.stanza });

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
    --enviar a mensagem para o cluster.

    local to = event.stanza.attr.to;
    local node, host, _ = jid_split(to);
    local rhost, room_jid;

    module:log("debug", "looking up target room cluster for %s", to);

    if node then
        room_jid = jid_bare(to);
        if is_local_room(room_jid) then
            module:log("debug", "room[%s] is hosted here. Nothing to do", room_jid);
            --check_redis_info(room_jid);
            local muc_room = get_room_from_jid(room_jid);

            local muc_members = get_members(muc_room);

            local clusters_enviar = {};
            
            local jid_from = jid_bare(event.stanza.attr.from);
            local stanza_c = st.clone(event.stanza);


            --TODO FAZER A BUSCA EM UMA THREAD..
            for user_jid in muc_members do
                module:log("debug", "VERIFICANDO CLUSTER MEMBER[%s]", user_jid);
                --lista de membros, ver quem é de outro cluster e esta online (se esta na lista do cluster esta online).
                local enviar = true;

                if prosody.bare_sessions[user_jid] then
                    module:log("debug", user_jid.." has a session here, nothing to do");
                    enviar = false;
                end
            
                --deliver_message(event);
                module:log("debug", "looking up target cluster for "..user_jid);
            
                -- cluster pode ter mais de um cluster/servidor....
                -- fazer um array de clusters/servidores que precisa enviar

                local cluster = cluster_users[user_jid];

                if not cluster then
                    module:log("debug","cluster lookup for "..user_jid.." failed!");
                    enviar = false;
                else 
                    module:log("debug", "target cluster for "..user_jid.." is "..cluster);
                    clusters_enviar[cluster] = cluster;
                    enviar = true;

                end
            
           
                if cluster == cluster_name then
                    module:log("debug", "we are cluster "..cluster..", nothing to do");
                    enviar = false;
                end

            end

            if clusters_enviar then
                for clusterz, _ in pairs(clusters_enviar) do
                    module:log("debug", "ENVIANDO STANZA:", tostring(stanza_c));
                    fire_event("cluster/send", { cluster = clusterz, host = host, stanza = stanza_c });
                end
                

            end

            return false;
        end
        --rhost = redis_mucs.get_room_host(to);
    end

    --fire_event("cluster/send", { cluster = rhost, stanza = event.stanza });
    return false;
end

local function handle_msg(event)
    local to = event.stanza.attr.to;
    local from = event.stanza.attr.from;
    local username, host = jid_split(to);
    local stanza = event.stanza;
    

    -- <presence xml:lang='en' from='dev2@hostx.net/xxx.2.3_e7cifh'><show>away</show><status>away</status><x xmlns='vcard-temp:x:update'><photo/></x></presence>
    module:log("debug", "mod_cluster": Recebido stanza:", tostring(stanza));


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

    local jid = username.."@"..host;

    if prosody.bare_sessions[jid] then
        module:log("debug", jid.." has a session here, nothing to do");
        return nil
    end

    if stanza.name == "presence" then
        module:log("debug", "STANZA.PRESENCE ---------------");

        if stanza.attr.type == "probe" then
                -- module:log("debug", "Nao encaminhar presence probe");
                -- return nil
        end

    end

    --deliver_message(event);
    module:log("debug", "looking up target cluster for "..jid);


    local cluster = cluster_users[jid]
    if not cluster then
        module:log("debug","cluster lookup for "..jid.." failed!");
        return nil;
    end

    module:log("debug", "target cluster for "..jid.." is "..cluster);

    if cluster == cluster_name then
        module:log("debug", "we are cluster "..cluster..", nothing to do");
        return nil;
    end
        

    fire_event("cluster/send", { cluster = cluster, host = host, stanza = event.stanza });

    --presences
    --<presence type='probe' from='dev2@hostx.net/hostx.2.3_34i44h' to='gd3@hostx.net'/> -- desabilitar probe
    --<presence from='dev2@hostx.net/hostx.2.3_34i44h' xml:lang='en'><show>online</show><status>online</status><x xmlns='vcard-temp:x:update'><photo/></x><delay from='hostx.net' stamp='2023-10-12T14:02:58Z' xmlns='urn:xmpp:delay'/></presence>
    --<presence from='dev2@hostx.net/hostx.2.3_34i44h' to='dev@hostx.net' xml:lang='en'><show>away</show><status>away</status><x xmlns='vcard-temp:x:update'><photo/></x></presence>

    -- local stanza_c = st.clone(event.stanza);
    --             stanza_c.attr.to = node..'@'..host..'/'..r;
    --             module:log("debug", "target cluster for %s is %s",
    --                 stanza_c.attr.to ,cluster);
    --             fire_event("cluster/send", { cluster = cluster, stanza = stanza_c });
                

 

    return true; --nao processar mais a mensagem, esta salvando como offline pois user nao tem session here
    -- precisa processar ACK! ack com prioridade 1000 processou antes do mod_cluster

end

local function handleUserConnected (event) 
    local session = event.session;
    local username = session.username;
    --	local from_jid = session.full_jid or (username .. "@" .. module.host);
    local jid = jid_bare(session.full_jid);


    for key, srv in pairs(remote_servers) do
		
        module:log("debug", "mod_cluster: Sending user session available to remote server:" .. srv);
        --criar um pacote xmpp de sessao do user
        --<cluster type='available' cluster_from='cluster1.hostx.net' from='gd@hostx.net' xmlns='urn:xmpp:cluster'/>
        userSessionStanza = st.stanza("cluster", { xmlns = 'urn:xmpp:cluster'});
        userSessionStanza.attr.type = "available";
        userSessionStanza.attr.from = jid;

        local host, port = splitHostAndPort(srv)
        fire_event("cluster/send", { cluster = host, host = host, stanza = userSessionStanza });

    end
    
end

local function handleUserDisconnected (event) 
    local session = event.session;
    local username = session.username;
    local jid = jid_bare(session.full_jid);

    for key, srv in pairs(remote_servers) do
		
        module:log("debug", "mod_cluster: Sending user session unavailable to remote server:" .. srv);
        --criar um pacote xmpp de sessao do user
        --<cluster type='unavailable' cluster_from='cluster1.hostx.net' from='gd@hostx.net' xmlns='urn:xmpp:cluster'/>
        userSessionStanza = st.stanza("cluster", { xmlns = 'urn:xmpp:cluster'});
        userSessionStanza.attr.type = "unavailable";
        userSessionStanza.attr.from = jid;

        local host, port = splitHostAndPort(srv)
        fire_event("cluster/send", { cluster = host, host = host, stanza = userSessionStanza });

    end

end

local function handleConnectedToCluster (event) 
    local remoteHost = event.cluster;
    module:log("debug", "mod_cluster: handleConnectedToCluster: " ..remoteHost);
end

-- se conectou ao cluster remoto, requisitar usuarios, e enviar usuarios locais
local function handleConnectedToCluster_old (event) 

    local remoteHost = event.cluster;
    module:log("debug", "mod_cluster: handleConnectedToCluster: " ..remoteHost);

    --probe for remote users
	userSessionStanza = st.stanza("cluster", { xmlns = 'urn:xmpp:cluster'});
	userSessionStanza.attr.type = "probe";
	userSessionStanza.attr.cluster_from = cluster_name;
	userSessionStanza.attr.cluster_to = remoteHost;

	fire_event("cluster/send", { cluster = remoteHost, host = remoteHost, stanza = userSessionStanza });
	--conn:write(tostring(userSessionStanza));

	--send local users
	for jid, session in pairs(bare_sessions) do
		
        module:log("debug", "mod_cluster: Sending user session available to remote server:" .. remoteHost);
        --criar um pacote xmpp de sessao do user
        --<cluster type='available' cluster_from='cluster1.host.net' from='gd@host.net' xmlns='urn:xmpp:cluster'/>
        userSessionStanza = st.stanza("cluster", { xmlns = 'urn:xmpp:cluster'});
        userSessionStanza.attr.type = "available";
        userSessionStanza.attr.from = jid;

        fire_event("cluster/send", { cluster = remoteHost, host = remoteHost, stanza = userSessionStanza });

    end

end

local function handleDisconnectedToCluster (event) 

    local remoteHost = event.cluster;
    module:log("debug", "mod_cluster: handleDisconnectedToCluster: " ..remoteHost);

end


--TODO: Quando cair conexao do host, excluir sessoes dos usuarios remotos

-- pre-message/bare ta caindo aqui -- com return true na funcao handle ele para de rotear a mensagem(nao salva sql local nem offline message etc, deixar pra salvar no server remoto,
-- pois offline tava salvando aqui, pois a sessao do user ta em outro server...
--message/full ta caindo, mas ta dizendo que tem sessao aqui mesmo nao tendo?!? e ta salvando offline
module:hook("pre-message/full", handle_msg, 1);
module:hook("pre-message/bare", handle_msg, 1);
module:hook("pre-presence/bare", handle_msg, 1000);
module:hook("pre-presence/full", handle_msg, 1000);
module:hook("resource-bind", handleUserConnected);
module:hook("resource-unbind", handleUserDisconnected);
module:hook_global("cluster/connectedToCluster", handleConnectedToCluster, 1000);
module:hook_global("cluster/disconnectedToCluster", handleDisconnectedToCluster, 1000);

module:log("debug", "hooked at %s", module:get_host());
