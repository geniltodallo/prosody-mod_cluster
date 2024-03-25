-- Mod Cluster for Prosody
-- Copyright (C) 2023-2024 Genilto Dallo 
-- GENILTO DALLO - 05/10/2023
--
-- This project is MIT/X11 licensed. Please see the
--
module:set_global();

local st = require "util.stanza";
local jid_split = require "util.jid".split;
local jid_bare = require "util.jid".bare;

local new_xmpp_stream = require "util.xmppstream".new;
local uuid_gen = require "util.uuid".generate;

local core_process_stanza = prosody.core_process_stanza;
local hosts = prosody.hosts;

local traceback = debug.traceback;

local cluster_name = module:get_option("cluster_name", nil);
if not cluster_name then
    error("cluster_name not configured", 0);
end

local opt_keepalives = module:get_option_boolean("cluster_tcp_keepalives", module:get_option_boolean("tcp_keepalives", true));

local sessions = module:shared("sessions");

--- Network and stream part ---

local xmlns_cluster = 'prosody:cluster';

local listener = {};

--- Callbacks/data for xmppstream to handle streams for us ---

local stream_callbacks = { default_ns = xmlns_cluster };

local xmlns_xmpp_streams = "urn:ietf:params:xml:ns:xmpp-streams";


if not prosody.cluster_users then
    prosody.cluster_users = {};
  end

local cluster_users = prosody.cluster_users;

--local mod_muc = module:depends"muc"; -- nao deu, diz que precisa ser componente
--local rooms = rawget(mod_muc, "rooms");
--local get_room_from_jid = rawget(mod_muc, "get_room_from_jid") or
--	function (room_jid)
--			return rooms[room_jid];
--end

function stream_callbacks.error(session, error, data, data2)
	if session.destroyed then return; end
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
					break;
				end
			end
		end
		text = condition .. (text and (" ("..text..")") or "");
		module:log("info", "Session closed by remote with error: %s", text);
		session:close(nil, text);
	end
end

function stream_callbacks.streamopened(session, attr)
	if attr.to ~= cluster_name then
		session:close{ condition = "host-unknown", text = "unknown cluster name "..tostring(attr.to) };
		return;
	end
	module:log("debug", "Received remote stream_callbacks: %s", attr);

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
        if muc and rawget(muc,"rooms") then
            -- We're running 0.9.x or 0.10 (old MUC API)
            return muc.rooms[room_jid];
        elseif muc and rawget(muc,"get_room_from_jid") then
            -- We're running >0.10 (new MUC API)
            return muc.get_room_from_jid(room_jid);
        else
            return
        end
    end
end	


-- aqui funciona, mas nao precisou
local function send_muc_message(stanza) 

	local dest_room = get_room_from_jid(stanza.attr.to);
	if not dest_room then return; end

	--local from_room, from_host, from_nick = jid_split(from_room_jid);
	-- chega do servidor from dev2@hostx to grupo@conference
	-- chega no client: from dev2 to dev@hostx type=groupchat

	local stanza_c = st.clone(stanza);
    stanza_c.attr.from = stanza.attr.to..'/'..stanza.attr.from;
                
	module:log("debug", 'Broadcast message to muc:', stanza.attr.to);
	module:log("debug", 'Briadcast stanza:', stanza);
	dest_room:broadcast_message(stanza_c);

end

function sendLocalSessions(remoteServer) 

	for jid, session in pairs(bare_sessions) do
		
        module:log("debug", "mod_cluster PROBE: Sending local users to remote server:" .. remoteServer);
        --criar um pacote xmpp de sessao do user
        --<cluster type='available' cluster_from='cluster1.hostx.net' from='gd@hostx.net' xmlns='urn:xmpp:cluster'/>
        userSessionStanza = st.stanza("cluster", { xmlns = 'urn:xmpp:cluster'});
        userSessionStanza.attr.type = "available";
        userSessionStanza.attr.from = jid;

        module:fire_event("cluster/send", { cluster = remoteServer, host = remoteServer, stanza = userSessionStanza });

    end

end

-- Atualizar lista de usuarios remotos ou envia lista de usuarios locais
local function handleClusterStanza(stanza) 

	module:log("debug", "cluster_SERVER handleClusterStanza: %s", stanza.attr.cluster_from);
	--<cluster type='available' cluster_from='cluster1.hostx.net' from='gd@hostx.net' xmlns='urn:xmpp:cluster'/>
	jid = jid_bare(stanza.attr.from);

	if stanza.name == "cluster" then

		if stanza.attr.type == "available" then

			cluster_users[jid] = stanza.attr.cluster_from;
		
		elseif stanza.attr.type == "unavailable" then
			cluster_users[jid] = nil;
		
		elseif stanza.attr.type == "probe" then
			if stanza.attr.cluster_from then
				sendLocalSessions(stanza.attr.cluster_from);
			end
			
		end

	end

end

local function handleerr(err) log("error", "Traceback[component]: %s", traceback(tostring(err), 2)); end
--Trata pacote/stanza recebido
function stream_callbacks.handlestanza(session, stanza)
	module:log("debug", "cluster_SERVER stream_callbacks.handlestanza: %s", stanza);

	--TODO: verificar se temos esse host
	--Se for cluster, precisa atualizar a lista de sessoes remotas
	if stanza.name == "cluster" then
		handleClusterStanza(stanza);
	end

	
	local to = stanza.attr.to;
	local node, host = jid_split(to);

	if not host then return end
	module:log("debug", "cluster_SERVER stream_callbacks.handlestanza HOST:: %s", host);


	--modificado grupos
	if stanza.attr.type == "groupchat" then
		return send_muc_message(stanza);
	end

	local h = hosts[host];
	local nh = {};
	for k,v in pairs(h) do 
		nh[k] = v; 
	end
	nh.type = "component";
	module:log("debug", "cluster_SERVER stream_callbacks.handlestanza NH::", tostring(nh));

	return xpcall(function () return core_process_stanza(nh, stanza) end, handleerr);

end


--- Closing a component connection
local stream_xmlns_attr = {xmlns='urn:ietf:params:xml:ns:xmpp-streams'};
local default_stream_attr = { ["xmlns:stream"] = "http://etherx.jabber.org/streams", xmlns = stream_callbacks.default_ns, version = "1.0", id = "" };
local function session_close(session, reason)
	if session.destroyed then return; end
	if session.conn then
		if session.notopen then
			session.send("<?xml version='1.0'?>");
			session.send(st.stanza("stream:stream", default_stream_attr):top_tag());
		end
		if reason then
			if type(reason) == "string" then -- assume stream error
				module:log("info", "Disconnecting component, <stream:error> is: %s", reason);
				session.send(st.stanza("stream:error"):tag(reason, {xmlns = 'urn:ietf:params:xml:ns:xmpp-streams' }));
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

		--TODO: Quando cair conexao do host, excluir sessoes dos usuarios remotos
		
	end
end

--- Component connlistener
function listener.onconnect(conn)
	local _send = conn.write;
	local session = { type = "cluster", conn = conn, send = function (data) return _send(conn, tostring(data)); end };

	-- Logging functions --
	local conn_name = "ss"..tostring(session):match("[a-f0-9]+$");
	-- session.log = logger.init(conn_name);
	session.close = session_close;

	if opt_keepalives then
		conn:setoption("keepalive", opt_keepalives);
	end

	module:log("info", "incoming cluster connection");

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
		module:log("debug", "Received invalid XML (%s) %d bytes: %s", tostring(err), #data, data:sub(1, 300):gsub("[\r\n]+", " "):gsub("[%z\1-\31]", "_"));
		session:close("not-well-formed");
	end

	session.dispatch_stanza = stream_callbacks.handlestanza;

	sessions[conn] = session;

end

-- Recebe pacotes do outro server
function listener.onincoming(conn, data)
	local session = sessions[conn];
	module:log("debug", "Received incoming remote data: %s", data);

	session.data(conn, data);
end
function listener.ondisconnect(conn, err)
	local session = sessions[conn];
	if session then
		module:log("info", "component disconnected: %s (%s)", tostring(session.host), tostring(err));
		if session.on_destroy then session:on_destroy(err); end
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
	name = "cluster";
	private = true;
	listener = listener;
	default_port = 7473;
	multiplex = {
		pattern = "^<.*:stream.*%sxmlns%s*=%s*(['\"])"..xmlns_cluster.."%1.*>";
	};
});
