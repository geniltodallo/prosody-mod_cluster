-- Mod Cluster for Prosody
-- Opensource 2023-2024 Genilto Dallo 
-- GENILTO DALLO - 05/10/2023
--
local socket = require "socket"
local logger = require "util.logger";
local sha1 = require"util.hashes".sha1;
local st = require "util.stanza";
local timer = require "util.timer";
local bit = require "bit";
local jid_split = require"util.jid".split;
local new_xmpp_stream = require"util.xmppstream".new;
local uuid_gen = require"util.uuid".generate;

local core_process_stanza = prosody.core_process_stanza;
local hosts = prosody.hosts;

local log = module._log;

local conns = {};
local queue = {};

local listener = {};

local sessions = module:shared("sessions");

local xmlns_cluster = 'prosody:cluster';

local stream_callbacks = {
    default_ns = xmlns_cluster
};

local xmlns_xmpp_streams = "urn:ietf:params:xml:ns:xmpp-streams";

local remote_servers = module:get_option("cluster_servers", {});
local remote_port = module:get_option("cluster_port", nil);
local node_name = module:get_option("cluster_node_name", nil);

if not prosody.cluster_users then
    prosody.cluster_users = {};
end
local cluster_users = prosody.cluster_users;

module:set_global();

function splitHostAndPort(inputstr)
    local host, port = inputstr:match("([^:]+):([^:]+)")
    return host, port
end

local function clearRemoteSessions(remoteServer)

    module:log("info", "Cluster clear remote sessions from " .. remoteServer);

    -- for jid, host in pairs(cluster_users) do

    --     if host == remoteServer then
    --         module:log("debug", "Clear remote session jid:" .. jid .. " from " ..remoteServer);
    --         cluster_users[jid] = nil;
    --     end

    -- end

    for jid, user_nodes in pairs(cluster_users) do

        for fulljid, host in pairs(user_nodes) do

            if host == remoteServer then
                module:log("debug", "Clear remote session jid:" .. fulljid .. " from " .. remoteServer);
                user_nodes[fulljid] = nil;
            end

        end

    end

end

function stream_callbacks.error(session, error, data, data2)
    if session.destroyed then
        return;
    end
    module:log("warn", "Error processing node stream: %s", tostring(error));
    if error == "no-stream" then
        session:close("invalid-namespace");
    elseif error == "parse-error" then
        module:log("warn", "External node %s XML parse error: %s", tostring(session.host), tostring(data));
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

function stream_callbacks.streamclosed(session)
    module:log("debug", "Received </stream:stream>");
    session:close();
end

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

-- se conectou ao cluster remoto, requisitar usuarios, e enviar usuarios locais
local function requisitarUsuariosRemotos(remoteHost)

    -- limpa sessões remotas
    clearRemoteSessions(remoteHost);

    -- probe for remote users
    userSessionStanza = st.stanza("cluster", {
        xmlns = 'urn:xmpp:cluster'
    });
    userSessionStanza.attr.type = "probe";
    userSessionStanza.attr.node_from = node_name;
    userSessionStanza.attr.node_to = remoteHost;

    module:fire_event("cluster/send", {
        node = remoteHost,
        host = remoteHost,
        stanza = userSessionStanza
    });

end

function listener.onconnect(conn)
    local node = conn:ip();

    local session = {
        type = "cluster",
        conn = conn,
        send = function(data)
            return conn:write(tostring(data));
        end,
        node = node
    };

    -- Logging functions --
    local conn_name = "sc" .. tostring(session):match("[a-f0-9]+$");
    -- session.log = logger.init(conn_name);
    session.close = session_close;

    conn:setoption("keepalive", true);

    module:log("info", "Cluster connected to " .. node);

    -- VERIFY CERT
    local sock = conn:socket();
    if sock.info then
        local info = sock:info();
        (session.log or log)("info", "Stream encrypted (%s with %s)", info.protocol, info.cipher);
    end
    local cert
    if conn.getpeercertificate then
        cert = conn:getpeercertificate()
    end

    local stream = new_xmpp_stream(session, stream_callbacks);
    session.stream = stream;

    function session.data(conn, data)
        local ok, err = stream:feed(data);
        if ok then
            return;
        end
        module:log("debug", "Received invalid XML (%s) %d bytes: %s", tostring(err), #data,
            data:sub(1, 300):gsub("[\r\n]+", " "):gsub("[%z\1-\31]", "_"));
        session:close("not-well-formed");
    end

    session.dispatch_stanza = stream_callbacks.handlestanza;

    session.notopen = true;
    session.send(st.stanza("stream:stream", {
        to = conn:ip(),
        ["xmlns:stream"] = 'http://etherx.jabber.org/streams',
        xmlns = xmlns_cluster
    }):top_tag());

    local queue = queue[node];
    for i, s in pairs(queue) do
        conn:write(tostring(s));
    end
    queue[node] = nil;

    sessions[conn] = session;

    requisitarUsuariosRemotos(node);

    module:fire_event("cluster/connectedToCluster", {
        node = node
    });

end

function listener.onincoming(conn, data)
    local session = sessions[conn];
    session.data(conn, data);
end

function listener.ondisconnect(conn, err)
    local session = sessions[conn];
    if (session) then
        local node_ = session.node;
        module:log("info", "Cluster server node disconnected: %s (%s)", tostring(node_), tostring(err));
        clearRemoteSessions(node_);
        if session.on_destroy then
            session:on_destroy(err);
        end
        sessions[conn] = nil;
        conns[node_] = nil;
        for k in pairs(session) do
            if k ~= "log" and k ~= "close" then
                session[k] = nil;
            end
        end
        session.destroyed = true;
    else
        local node_ = conn:ip();
        module:log("debug", "On disconnect session null: %s", node_);
        conns[node_] = nil;
    end

    module:log("error", "connection lost");
    module:fire_event("cluster/disconnected", {
        reason = err,
        node = conn:ip()
    });
    -- module:fire_event("cluster/disconnectedToCluster", { node = conn:ip() });

end

function listener.onreadtimeout(conn)

    module:log("debug", "Listener onreadtimeout");
    local session = sessions[conn];
    if session then
        session.send(" ");
        return true;
    end

end

function connect(node_, port_)

    module:log("info", "Cluster connecting to node server: " .. node_);

    if not port_ then
        port_ = remote_port;
    end

    module:log("info", "Cluster connecting to node server: " .. node_ .. " Port:" .. port_);

    local conn = socket.tcp()
    conn:settimeout(10)
    local ok, err = conn:connect(node_, port_)
    if not ok and err ~= "timeout" then
        return nil, err;
    end

    local handler, conn = server.wrapclient(conn, node_, port_, listener, "*a")
    return handler;
end

module:hook_global("server-stopping", function(event)
    local reason = event.reason;
    if session then
        session:close{
            condition = "system-shutdown",
            text = reason
        };
    end
end, 1000);

function handle_send(event)
    local node = event.node;
    local stanza = event.stanza;
    local to = event.stanza.attr.to;
    local from = event.stanza.attr.from;
    local username, host = jid_split(to);

    -- Adicionar qual node ta enviando o stanza
    stanza.attr.node_from = node_name;

    module:log("debug", "got stanza for node " .. node);

    -- if event.host ~= module.host then
    --    module:log("debug", event.host.." host do user diferente do modulo:" ..module.host);
    --    return nil
    -- end

    local conn = conns[node];
    if conn == nil then
        module:log("debug", "connecting to " .. node .. " for delivery");
        local err;
        conn, err = connect(node);
        if not conn then
            module:log("error", "couldn't connect to " .. node .. ": " .. err);
            return;
        end
        conns[node] = conn;
        queue[node] = {};
    end

    module:log("debug", "Tem conexao ativa:" .. node);

    local session = sessions[conn]
    if session == nil then
        module:log("debug", "Handle send, session null");
        table.insert(queue[node], stanza)
    else
        conn:write(tostring(stanza));
    end
end

local function handleClusterStanza(stanza)
    module:log("debug", "cluster_CLIENT handleClusterStanza: %s", tostring(stanza));

    if stanza.name == "cluster" then
        if stanza.attr.type == "hash_response" then
            -- Processar resposta de hash
            local remoteNode = stanza.attr.node_from;
            local remoteHash = stanza.attr.remote_hash;
            local remoteCount = tonumber(stanza.attr.remote_count) or 0;
            local ourHash = stanza.attr.our_hash;
            local ourCount = tonumber(stanza.attr.our_count) or 0;

            module:log("debug", "Recebida resposta de hash do nó %s", remoteNode);

            -- Disparar evento global para que o mod_cluster.lua processe
            module:fire_event("cluster/hash_response", {
                node = remoteNode,
                remote_hash = remoteHash,
                remote_count = remoteCount,
                our_hash = ourHash,
                our_count = ourCount
            });
        end
    end
end

function sendPing(node)

    -- Ping
    -- <iq from='capulet.lit' to='montague.lit' id='s2s1' type='get'>
    --	<ping xmlns='urn:xmpp:ping'/>
    -- </iq>

    -- Pong
    -- <iq from='montague.lit' to='capulet.lit' id='s2s1' type='result'/>
    local pingStanza = st.stanza("cluster", {
        xmlns = 'urn:xmpp:cluster'
    });
    pingStanza.attr.type = "get";
    pingStanza.attr.node_to = node;
    pingStanza:tag("ping", {
        xmlns = "urn:xmpp:ping"
    });
    pingStanza.attr.id = uuid_gen();

    module:fire_event("cluster/send", {
        node = node,
        host = node,
        stanza = pingStanza
    });

end

-- Verify connection timer
function timerConnectRemote()

    module:log("debug", "mod_cluster_client timerConnectRemote");

    for key, srv in pairs(remote_servers) do

        local host_, port_ = splitHostAndPort(srv)

        local conn = conns[host_];
        if conn == nil then
            module:log("debug", "connecting to node " .. host_ .. " port:" .. port_);
            local err;
            conn, err = connect(host_, port_);
            if not conn then
                module:log("info", "Cluster couldn't connect to node " .. host_ .. " :" .. err);
            else
                conns[host_] = conn;
                queue[host_] = {};

            end
        else
            module:log("debug", "Not reconnecting, node connected:" .. host_);
            -- node connected, send ping
            sendPing(host_);
            -- conn:write(' ');
        end

    end

    return 30; -- 30 seconds

end

--Calcula hash do array de usuarios remotos de um nó específico
local function calculateRemoteNodeXORHash(target_node)
    local hash_accumulator = 0;
    local count = 0;
    
    for jid, user_nodes in pairs(cluster_users) do
        for fulljid, node in pairs(user_nodes) do
            if node == target_node then
                -- Hash simples do JID completo
                local jid_hash = 0;
                for i = 1, #fulljid do
                    jid_hash = jid_hash + fulljid:byte(i);
                end
                hash_accumulator = bit.bxor(hash_accumulator, jid_hash); -- XOR CORRETO
                count = count + 1;
            end
        end
    end
    
    return string.format("%x", hash_accumulator), count;
end

-- Função para calcular hash dos usuários locais
local function calculateLocalUsersXORHash()
    local hash_accumulator = 0;
    local count = 0;
    
    for jid, user_sessions in pairs(bare_sessions) do
        if user_sessions.sessions then
            for key, session in pairs(user_sessions.sessions) do
                -- Hash simples do JID
                local jid_hash = 0;
                for i = 1, #session.full_jid do
                    jid_hash = jid_hash + session.full_jid:byte(i);
                end
                hash_accumulator = bit.bxor(hash_accumulator, jid_hash); -- XOR CORRETO
                count = count + 1;
            end
        end
    end
    
    return string.format("%x", hash_accumulator), count;
end

-- Funcao do timer para verificação de consistência com nós remotos
function timerVerifyRemoteConsistency()
    module:log("info", "Verificando consistência com nós remotos");

    -- Para cada servidor remoto, requisitar hash dos usuários locais dele
    for key, srv in pairs(remote_servers) do
        local host, port = splitHostAndPort(srv);

        -- Verificar se existe conexão ativa com este servidor
        local has_connection = true;
        local conn = conns[host];
        if conn == nil then
            has_connection = false;
        end

        if has_connection then
            -- Calcular nosso hash local dos usuários que temos deste nó
            local our_remote_hash, our_remote_count = calculateRemoteNodeXORHash(host);

            module:log("info", "Solicitando hash remoto do nó %s (conectado - temos %d usuários, hash: %s)", host,
                our_remote_count, our_remote_hash);

            -- Criar stanza para requisitar hash remoto
            local hashRequestStanza = st.stanza("cluster", {
                xmlns = 'urn:xmpp:cluster'
            });
            hashRequestStanza.attr.type = "hash_request";
            hashRequestStanza.attr.node_from = node_name;
            hashRequestStanza.attr.node_to = host;
            hashRequestStanza.attr.our_hash = our_remote_hash;
            hashRequestStanza.attr.our_count = our_remote_count;

            module:fire_event("cluster/send", {
                node = host,
                host = host,
                stanza = hashRequestStanza
            });
        else
            module:log("warn", "Nó %s não está conectado - pulando verificação de hash", host);

            -- Opcional: limpar dados antigos deste nó se não está conectado há muito tempo
            -- local nodes_info = {};
            -- for jid, user_nodes in pairs(cluster_users) do
            --     for fulljid, node in pairs(user_nodes) do
            --         if node == host then
            --             if not nodes_info[node] then
            --                 nodes_info[node] = 0;
            --             end
            --             nodes_info[node] = nodes_info[node] + 1;
            --         end
            --     end
            -- end

            if has_connection[host] and cluster_users[host] > 0 then
                module:log("warn", "Nó %s desconectado mas ainda temos %d usuários na lista local", host,
                    cluster_users[host]);
            end
        end
    end

    return 600; -- 600 - 10 minutos
end

function handle_start(event)

    module:log("debug", "Module cluster_client server-started");

    -- Timer to verify remote connection -- 30 seconds
    timer.add_task(30, timerConnectRemote);
    -- Connect as soon as possible!
    timerConnectRemote();

    -- Timer para verificar consistencia hash das listas de usuários
    timer.add_task(600, timerVerifyRemoteConsistency);

end

-- Função para processar resposta de hash e decidir se precisa re-sincronizar
local function handleHashResponse(event)
    local remoteNode = event.node;
    local remoteHash = event.remote_hash;
    local remoteCount = event.remote_count;
    local ourHash = event.our_hash;
    local ourCount = event.our_count;
    module:log("info", "Comparação de hash com nó %s:", remoteNode);
    module:log("info", "  Remoto: %d usuários (hash: %s)", remoteCount, remoteHash);
    module:log("info", "  Local:  %d usuários (hash: %s)", ourCount, ourHash);
    
    if remoteHash ~= ourHash or remoteCount ~= ourCount then
        module:log("info", "Inconsistência detectada com nó %s! Re-sincronizando...", remoteNode);
        
        -- Limpar dados antigos deste nó
        -- Solicitar lista atualizada
        --TODO Melhorar resincronizacao - responder o probe com todos os users em vez de um por um
        requisitarUsuariosRemotos(remoteNode);  
        module:log("info", "Probe enviado para nó %s para re-sincronização", remoteNode);
    else
        module:log("info", "Hash consistente com o nó %s", remoteNode);
    end
end

module:hook_global("cluster/send", handle_send, 1000);
module:hook_global("cluster/hash_response", handleHashResponse);
module:hook_global("server-started", handle_start);


    
