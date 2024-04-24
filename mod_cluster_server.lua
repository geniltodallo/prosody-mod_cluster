-- Mod Cluster for Prosody
-- Copyright (C) 2023-2024 Genilto Dallo 
-- GENILTO DALLO - 05/10/2023
--
-- This project is MIT/X11 licensed. Please see the
--
module:set_global();

local st = require "util.stanza";
local jid_split = require"util.jid".split;
local jid_bare = require"util.jid".bare;

local new_xmpp_stream = require"util.xmppstream".new;
local uuid_gen = require"util.uuid".generate;

local core_process_stanza = prosody.core_process_stanza;
local hosts = prosody.hosts;

local traceback = debug.traceback;

local cluster_server_port = module:get_option("cluster_server_port", 7473);

local node_name = module:get_option("cluster_node_name", nil);
if not node_name then
    error("cluster_node_name not configured", 0);
end

local sessions = module:shared("sessions");
local bare_sessions = prosody.bare_sessions;
--- Network and stream part ---

local xmlns_cluster = 'prosody:cluster';

local listener = {};

--- Callbacks/data for xmppstream to handle streams for us ---

local stream_callbacks = {
    default_ns = xmlns_cluster
};

local xmlns_xmpp_streams = "urn:ietf:params:xml:ns:xmpp-streams";

if not prosody.cluster_users then
    prosody.cluster_users = {};
end

-- TODO: sessions cluster_users_sessions? ou cluster_users.sessions?
-- Salvar sessoes de usuario remoto, pelo menos uma sessao. Ai sabe que ele ta remoto. E se ta local ver mesmo assim. Pois pode ta remoto tambem.
-- Talvez só precise verificar se ta remoto, e depois se ta local tambem. talvez nao precise saber do resource da sessao? O prosody remoto se encarrega da sessao.



local cluster_users = prosody.cluster_users;

-- local mod_muc = module:depends"muc"; -- nao deu, diz que precisa ser componente
-- local rooms = rawget(mod_muc, "rooms");
-- local get_room_from_jid = rawget(mod_muc, "get_room_from_jid") or
--	function (room_jid)
--			return rooms[room_jid];
-- end

function stream_callbacks.error(session, error, data, data2)
    if session.destroyed then
        return;
    end
    module:log("warn", "Error processing component stream: %s", tostring(error));
    if error == "no-stream" then
        session:close("invalid-namespace");
    elseif error == "parse-error" then
        module:log("warn", "External component %s XML parse error: %s", tostring(session.host), tostring(data));
        session:close("not-well-formed");
    elseif error == "stream-error" then
        local condition, text = "undefined-condition";
        for child in data:children() do
            if child.attr.xmlns == xmlns_xmpp_streams then
                if child.name ~= "text" then
                    condition = child.name;
                else
                    text = child:get_text();
                end
                if condition ~= "undefined-condition" and text then
                    break
                end
            end
        end
        text = condition .. (text and (" (" .. text .. ")") or "");
        module:log("info", "Session closed by remote with error: %s", text);
        session:close(nil, text);
    end 
end

function stream_callbacks.streamopened(session, attr)
    if attr.to ~= node_name then
        session:close{
            condition = "host-unknown",
            text = "unknown node name " .. tostring(attr.to)
        };
        return;
    end
    --module:log("debug", "Received remote stream_callbacks: %s", attr);

    session.host = attr.to;
    session.streamid = uuid_gen();
    session.notopen = nil;
    -- Return stream header
    session:open_stream();
end

function stream_callbacks.streamclosed(session)
    module:log("debug", "Received </stream:stream>");
    session:close();
end

--- Finds and returns room by its jid
-- @param room_jid the room jid to search in the muc component
-- @return returns room if found or nil
function get_room_from_jid(room_jid)
    local _, host = jid_split(room_jid);
    local component = hosts[host];
    if component then
        local muc = component.modules.muc
        if muc and rawget(muc, "rooms") then
            -- We're running 0.9.x or 0.10 (old MUC API)
            return muc.rooms[room_jid];
        elseif muc and rawget(muc, "get_room_from_jid") then
            -- We're running >0.10 (new MUC API)
            return muc.get_room_from_jid(room_jid);
        else
            return
        end
    end
end

local function send_muc_message(stanza)

    local dest_room = get_room_from_jid(stanza.attr.to);
    if not dest_room then
        return;
    end

    -- local from_room, from_host, from_nick = jid_split(from_room_jid);
    -- chega do servidor from dev2@hostx to grupo@conference
    -- chega no client: from dev2 to dev@hostx type=groupchat

    local stanza_c = st.clone(stanza);
    stanza_c.attr.from = stanza.attr.to .. '/' .. stanza.attr.from;

    module:log("debug", 'Broadcast message to muc:', stanza.attr.to);
    module:log("debug", 'Briadcast stanza:', stanza);
    dest_room:broadcast_message(stanza_c);

end

-- nao gerar carbona qui
-- Send carbon copy to local session if has jid from
local function send_carbon_copy(stanza)
	local orig_type = stanza.attr.type;
    local jid_from = stanza.attr.from;
    local bare_jid_from = jid_bare(jid_from);
	local user_sessions = bare_sessions[bare_jid_from];
    
    if not user_sessions then
        module:log("debug", "CLUSTER CARBON - User not has session carbon here");
        return
    end
    if not user_sessions.sessions then
        module:log("debug", "CLUSTER CARBON - User not has session carbon here");
        return
    end

	--for _, session in pairs(user_sessions) do
    for _, session in pairs(user_sessions.sessions) do

		module:log("debug", "CARBON ENTROU LOOP USER_SESSIONS DE:" .._);
 
		-- Carbons are sent to resources that have enabled it
		if session.want_carbons then
            module:log("debug", "CARBON SESSION.WANT_CARBONS::" ..session.want_carbons);
            
            local xmlns_carbons = "urn:xmpp:carbons:2";
            local xmlns_forward = "urn:xmpp:forward:0";
        
		    local copy = st.clone(stanza);
            copy.attr.xmlns = "jabber:client";
            local carbon = st.message{ from = jid_from, type = orig_type, }
                :tag("sent", { xmlns = xmlns_carbons })
                    :tag("forwarded", { xmlns = xmlns_forward })
                        :add_child(copy):reset();
			carbon.attr.to = session.full_jid;
			
            module:log("debug", "Sending carbon to %s", session.full_jid);
			session.send(carbon);
            
		end
	end
end

local function clearRemoteSessions(remoteServer)

    module:log("info", "Cluster clear remote sessions from " ..remoteServer);

    for jid, user_nodes in pairs(cluster_users) do
        
        for fulljid, host in pairs(user_nodes) do

            if host == remoteServer then
                module:log("debug", "Clear remote session jid:" .. fulljid .. " from " ..remoteServer);
                user_nodes[fulljid] = nil;
            end

        end

    end

end

function sendLocalSessions(remoteServer)


    --para cada usuario
    for jid, user_sessions in pairs(bare_sessions) do

        --para cada sessao do usuario
        for key, session in pairs(user_sessions.sessions) do

            module:log("debug", "mod_cluster PROBE: Sending local user ".. jid .. " to remote server:" .. remoteServer);
            module:log("debug", "mod_cluster PROBE: Sending local user key:".. key .. " session: " .. session.full_jid .. " to remote server:" .. remoteServer);
            -- criar um pacote xmpp de sessao do user
            -- <cluster type='available' node_from='cluster1.hostx.net' from='gd@hostx.net' xmlns='urn:xmpp:cluster'/>
            userSessionStanza = st.stanza("cluster", {
                xmlns = 'urn:xmpp:cluster'
            });
    
            userSessionStanza.attr.type = "available";
            userSessionStanza.attr.from = session.full_jid;
    
            module:fire_event("cluster/send", {
                node = remoteServer,
                host = remoteServer,
                stanza = userSessionStanza
            });

        end

    end

end

-- Atualizar lista de usuarios remotos ou envia lista de usuarios locais
local function handleClusterStanza(stanza)

    module:log("debug", "cluster_SERVER handleClusterStanza: %s", tostring(stanza));
    -- <cluster type='available' node_from='cluster1.hostx.net' from='gd@hostx.net' xmlns='urn:xmpp:cluster'/>
    local jid = jid_bare(stanza.attr.from);
    local jidfull = stanza.attr.from;
        
    if stanza.name == "cluster" then

        if stanza.attr.type == "available" then

            
            --cluster_users[jid] = stanza.attr.node_from;
            
            --cluster_users_nodes[gd@domain.net/sessionzyx] = sv-xyz.domain.net
            --gd@domain.net/-hsrWe-9 = sv03-3.domain.net
            --gd@domain.net/resource2_2.2.40_bgi33a = sv03-3.domain.net
            --gd@domain.net/resource1_1.6.0_abc123 = sv03-3.domain.net

            local user_nodes = cluster_users[jid];
            if not user_nodes then
                user_nodes = {};
            end

            user_nodes[jidfull] = stanza.attr.node_from;

            cluster_users[jid] = user_nodes;

            module:log("info", "Cluster add remote session: ".. jidfull .. " from "..stanza.attr.node_from);

        elseif stanza.attr.type == "unavailable" then

            local user_nodes = cluster_users[jid];
            if not user_nodes then
                user_nodes = {};
            end

            module:log("info", "Cluster removing remote session: ".. jidfull .. " from "..stanza.attr.node_from);

            user_nodes[jidfull] = nil;
            --TODO pode ter mais de um user no mesmo nó remoto

            local user_nodes_size = 0;
            for key, node in pairs(user_nodes) do
                user_nodes_size = user_nodes_size + 1;
            end

            if user_nodes_size > 0 then                
                cluster_users[jid] = user_nodes;
            else
                cluster_users[jid] = nil; 
            end

        elseif stanza.attr.type == "probe" then
            if stanza.attr.node_from then
                sendLocalSessions(stanza.attr.node_from);
            end

        end

        if stanza.attr.type == "get" and stanza:get_child("ping", "urn:xmpp:ping") then

            -- reply pong
            -- <iq from='montague.lit' to='capulet.lit' id='s2s1' type='result'/>
            local pongStanza = st.stanza("cluster", {
                xmlns = 'urn:xmpp:cluster'
            });
            pongStanza.attr.type = "result";
            pongStanza.attr.node_to = stanza.attr.node_from;
			pongStanza.attr.id = stanza.attr.id;

            module:fire_event("cluster/send", {
                node = pongStanza.attr.node_to,
                host = pongStanza.attr.node_to,
                stanza = pongStanza
            });

        end
    end
end




local function handleerr(err)
    log("error", "Traceback[component]: %s", traceback(tostring(err), 2));
end
-- Trata pacote/stanza recebido
function stream_callbacks.handlestanza(session, stanza)
    -- module:log("debug", "cluster_SERVER stream_callbacks.handlestanza: %s", stanza);

    if stanza.name == "cluster" then
        handleClusterStanza(stanza);
    end

    local to = stanza.attr.to;
    local node, host = jid_split(to);

    if not host then
        return
    end

    -- modificado grupos
    if stanza.attr.type == "groupchat" then
        return send_muc_message(stanza);
    end

    local h = hosts[host];
    local nh = {};
    for k, v in pairs(h) do
        nh[k] = v;
    end
    nh.type = "component";
    --module:log("debug", "cluster_SERVER stream_callbacks.handlestanza NH::", tostring(nh));

    return xpcall(function()
        return core_process_stanza(nh, stanza)
    end, handleerr);

end

--- Closing a component connection
local stream_xmlns_attr = {
    xmlns = 'urn:ietf:params:xml:ns:xmpp-streams'
};
local default_stream_attr = {
    ["xmlns:stream"] = "http://etherx.jabber.org/streams",
    xmlns = stream_callbacks.default_ns,
    version = "1.0",
    id = ""
};
local function session_close(session, reason)
    if session.destroyed then
        return;
    end
    if session.conn then
        if session.notopen then
            session.send("<?xml version='1.0'?>");
            session.send(st.stanza("stream:stream", default_stream_attr):top_tag());
        end
        if reason then
            if type(reason) == "string" then -- assume stream error
                module:log("info", "Disconnecting component, <stream:error> is: %s", reason);
                session.send(st.stanza("stream:error"):tag(reason, {
                    xmlns = 'urn:ietf:params:xml:ns:xmpp-streams'
                }));
            elseif type(reason) == "table" then
                if reason.condition then
                    local stanza = st.stanza("stream:error"):tag(reason.condition, stream_xmlns_attr):up();
                    if reason.text then
                        stanza:tag("text", stream_xmlns_attr):text(reason.text):up();
                    end
                    if reason.extra then
                        stanza:add_child(reason.extra);
                    end
                    module:log("info", "Disconnecting component, <stream:error> is: %s", tostring(stanza));
                    session.send(stanza);
                elseif reason.name then -- a stanza
                    module:log("info", "Disconnecting component, <stream:error> is: %s", tostring(reason));
                    session.send(reason);
                end
            end
        end
        session.send("</stream:stream>");
        session.conn:close();
        listener.ondisconnect(session.conn, "stream error");

    end
end

--Verify if node is authorized
local function is_trusted_node(conn)

    -- While this may seem insecure, the module defaults to only trusting 127.0.0.1 and ::1
	if trusted_networks:empty() then
        module:log("info", "No trusted nodes configured! Please add cluster_nodes_trusted={x.x.x.x} to conf.");
		return false;
	end

	-- Iterate through all trusted proxies and check for match against connected IP address
	local conn_ip = ip.new_ip(conn:ip());
	for trusted_network in trusted_networks:items() do
		if ip.match(trusted_network.ip, conn_ip, trusted_network.cidr) then
			return true;
		end
	end

	-- Connection does not match any trusted
	return false;
end

--- Component connlistener
function listener.onconnect(conn)

    conn:setoption("keepalive", true);

    local _send = conn.write;
    local session = {
        type = "cluster",
        conn = conn,
        send = function(data)
            return _send(conn, tostring(data));
        end
    };

    local node = conn:ip();

    	-- Client is using legacy SSL (otherwise mod_tls sets this flag)
	if conn:ssl() then

		session.secure = true;
		session.encrypted = true;
        module:log("info", "Cluster SECURE SSL incoming node connection from" ..node);

		-- Check if TLS compression is used
		local sock = conn:socket();
		if sock.info then
			session.compressed = sock:info"compression";
		elseif sock.compression then
			session.compressed = sock:compression(); --COMPAT mw/luasec-hg
		end
	else
    
        module:log("info", "Cluster UNSECURE incoming node connection from " ..node);

    end

	-- if conn:ip() == nil then
	-- 	conn:close();
	-- 	return;
	-- end

    -- TODO: is_trusted_node()
	-- Check if connection is coming from a trusted proxy
	-- if not is_trusted_proxy(conn) then
	-- 	conn:close();
	-- 	module:log("warn", "Dropped connection from untrusted proxy: %s", conn:ip());
	-- 	return;
	-- end

    -- Logging functions --
    local conn_name = "ss" .. tostring(session):match("[a-f0-9]+$");
    -- session.log = logger.init(conn_name);
    session.close = session_close;

    local stream = new_xmpp_stream(session, stream_callbacks);
    session.stream = stream;

    session.notopen = true;

    function session.reset_stream()
        session.notopen = true;
        session.stream:reset();
    end

    function session.data(conn, data)
        local ok, err = stream:feed(data);
        if ok then
            module:log("debug", "Received remote data: %s", data);
            return;
        end
        module:log("debug", "Received invalid XML (%s) %d bytes: %s", tostring(err), #data,
            data:sub(1, 300):gsub("[\r\n]+", " "):gsub("[%z\1-\31]", "_"));
        session:close("not-well-formed");
    end

    session.dispatch_stanza = stream_callbacks.handlestanza;

    sessions[conn] = session;

end

function listener.onreadtimeout(conn)

    local session = sessions[conn];
	if session then
		session.send(" ");
		return true;
	end

end

-- Recebe pacotes do outro server
function listener.onincoming(conn, data)
    local session = sessions[conn];
    local remoteHost = session.host;
    module:log("debug", "Received incoming remote data: %s", data);

    session.data(conn, data);
end
function listener.ondisconnect(conn, err)
    local session = sessions[conn];
    if session then
        local node = conn:ip()
        module:log("info", "Cluster client node disconnected: %s (%s)", tostring(node), tostring(err));

        if session.host then
           clearRemoteSessions(session.host);
        end
        if session.on_destroy then
            session:on_destroy(err);
        end
        sessions[conn] = nil;
        for k in pairs(session) do
            if k ~= "log" and k ~= "close" then
                session[k] = nil;
            end
        end
        session.destroyed = true;
        session = nil;
    end
end

function listener.ondetach(conn)
    sessions[conn] = nil;
end

module:provides("net", {
    name = "cluster",
    private = true,
    listener = listener,
	-- encryption = "ssl";
	-- ssl_config = {
	-- 	verify = "none";
    --     -- lua_ssl_verify_depth = 0;
	-- };
    -- default_port = 7003,
    default_port = cluster_server_port,
    multiplex = {
        pattern = "^<.*:stream.*%sxmlns%s*=%s*(['\"])" .. xmlns_cluster .. "%1.*>"
    }
});
