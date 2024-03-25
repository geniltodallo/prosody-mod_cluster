-- Mod Cluster for Prosody
-- Copyright (C) 2023-2024 Genilto Dallo 
-- GENILTO DALLO - 05/10/2023
--
-- This project is MIT/X11 licensed. Please see the
--
local socket = require "socket"
local logger = require "util.logger";
local sha1 = require "util.hashes".sha1;
local st = require "util.stanza";
local timer = require "util.timer";

local jid_split = require "util.jid".split;
local new_xmpp_stream = require "util.xmppstream".new;
local uuid_gen = require "util.uuid".generate;

local core_process_stanza = prosody.core_process_stanza;
local hosts = prosody.hosts;

local log = module._log;

local opt_keepalives = module:get_option_boolean("cluster_tcp_keepalives", module:get_option_boolean("tcp_keepalives", true));

local conns = {};
local queue = {};

local listener = {};

local sessions = module:shared("sessions");

local xmlns_cluster = 'prosody:cluster';
local stream_callbacks = { default_ns = xmlns_cluster };

local xmlns_xmpp_streams = "urn:ietf:params:xml:ns:xmpp-streams";

local remote_servers = module:get_option("cluster_servers", {});
local cluster_name = module:get_option("cluster_name", nil);

module:set_global();

function splitHostAndPort(inputstr)
    local host, port = inputstr:match("([^:]+):([^:]+)")
    return host, port
end

function stream_callbacks.error(session, error, data, data2)
	if session.destroyed then return; end
	module:log("warn", "Error processing cluster stream: %s", tostring(error));
	if error == "no-stream" then
		session:close("invalid-namespace");
	elseif error == "parse-error" then
		module:log("warn", "External cluster %s XML parse error: %s", tostring(session.host), tostring(data));
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

function stream_callbacks.streamclosed(session)
	module:log("debug", "Received </stream:stream>");
	session:close();
end

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
	end
end

-- se conectou ao cluster remoto, requisitar usuarios, e enviar usuarios locais
local function requisitarUsuariosRemotos(remoteHost) 

    --probe for remote users
	userSessionStanza = st.stanza("cluster", { xmlns = 'urn:xmpp:cluster'});
	userSessionStanza.attr.type = "probe";
	userSessionStanza.attr.cluster_from = cluster_name;
	userSessionStanza.attr.cluster_to = remoteHost;

	module:fire_event("cluster/send", { cluster = remoteHost, host = remoteHost, stanza = userSessionStanza });

end


function listener.onconnect(conn)
    local cluster = conn:ip();

	local session = { type = "cluster", conn = conn, send = function (data) return conn:write(tostring(data)); end, cluster = cluster };

	-- Logging functions --
	local conn_name = "sc"..tostring(session):match("[a-f0-9]+$");
	-- session.log = logger.init(conn_name);
	session.close = session_close;

	if opt_keepalives then
		conn:setoption("keepalive", opt_keepalives);
	end

	module:log("info", "outgoing cluster connection");

	local stream = new_xmpp_stream(session, stream_callbacks);
	session.stream = stream;

	function session.data(conn, data)
		local ok, err = stream:feed(data);
		if ok then return; end
		module:log("debug", "Received invalid XML (%s) %d bytes: %s", tostring(err), #data, data:sub(1, 300):gsub("[\r\n]+", " "):gsub("[%z\1-\31]", "_"));
		session:close("not-well-formed");
	end

	session.dispatch_stanza = stream_callbacks.handlestanza;

	session.notopen = true;
	session.send(st.stanza("stream:stream", {
		to = conn:ip(),
		["xmlns:stream"] = 'http://etherx.jabber.org/streams';
		xmlns = xmlns_cluster;
	}):top_tag());

    local queue = queue[cluster];
    for i,s in pairs(queue) do
        conn:write(tostring(s));
    end
    queue[cluster] = nil;

	sessions[conn] = session;

	requisitarUsuariosRemotos(cluster);

	module:fire_event("cluster/connectedToCluster", { cluster = cluster });

end

function listener.onincoming(conn, data)
	local session = sessions[conn];
	session.data(conn, data);
end

function listener.ondisconnect(conn, err)
	local session = sessions[conn];

	if (session) then
		module:log("info", "cluster disconnected: %s (%s)", tostring(session.cluster), tostring(err));
		if session.on_destroy then session:on_destroy(err); end
		sessions[conn] = nil;
		conns[session.cluster] = nil;
		for k in pairs(session) do
			if k ~= "log" and k ~= "close" then
				session[k] = nil;
			end
		end
		session.destroyed = true;
	end

	module:log("error", "connection lost");
	module:fire_event("cluster/disconnected", { reason = err, cluster = conn:ip() });
	--module:fire_event("cluster/disconnectedToCluster", { cluster = conn:ip() });

end

function connect (cluster)

	module:log("info", "mod_cluster_client: Connecting to cluster server:"..cluster);

	local conn = socket.tcp ( )
	conn:settimeout ( 10 )
	local ok, err = conn:connect (cluster, 7473)
	if not ok and err ~= "timeout" then
		return nil, err;
	end

	local handler, conn = server.wrapclient ( conn , cluster , 7473 , listener , "*a")
	return handler;
end

module:hook_global("server-stopping", function(event)
	local reason = event.reason;
	if session then
		session:close{ condition = "system-shutdown", text = reason };
	end
end, 1000);

function handle_send (event)
    local cluster = event.cluster;
    local stanza = event.stanza;
	local to = event.stanza.attr.to;
    local from = event.stanza.attr.from;
    local username, host = jid_split(to);

	--Adicionar qual cluster ta enviando o stanza né
	stanza.attr.cluster_from = cluster_name;
	
    module:log("debug", "got stanza for cluster "..cluster);

	--if event.host ~= module.host then
    --    module:log("debug", event.host.." host do user diferente do modulo:" ..module.host);
    --    return nil
    --end
	
    local conn = conns[cluster];
    if conn == nil then
        module:log("debug", "connecting to "..cluster.." for delivery");
        local err;
        conn, err = connect(cluster);
        if not conn then
            module:log("error", "couldn't connect to "..cluster..": "..err);
            return;
        end
        conns[cluster] = conn;
        queue[cluster] = {};
    end

    local session = sessions[conn]
    if session == nil then
        table.insert(queue[cluster], stanza)
    else
        conn:write(tostring(stanza));
    end
end


-- Verify connection timer
function timerConnectRemote() 

	module:log("debug", "mod_cluster_client timerConnectRemote");
	
		for key, srv in pairs(remote_servers) do
		
		local host, port = splitHostAndPort(srv)

		local conn = conns[host];
		if conn == nil then
			--module:log("debug", "connecting to cluster "..host);
			local err;
			conn, err = connect(host);
			if not conn then
				module:log("error", "couldn't connect to cluster "..host..": "..err);
				--return;
			else
				conns[host] = conn;
				queue[host] = {};

			end
		end

	end

	return 60; -- 60 seconds

end

function handle_start (event)

	module:log("debug", "Module cluster_client server-started");
	--Timer to verify remote connection -- 60 seconds
	timer.add_task(60, timerConnectRemote);

end

module:hook_global("cluster/send", handle_send, 1000);
--module:hook("cluster/send", handle_send, 1000);
module:hook_global("server-started", handle_start);
