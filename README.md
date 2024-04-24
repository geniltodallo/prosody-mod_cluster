# prosody-mod_cluster
Module cluster for Prosody IM for HA Purposes.

This is a cluster module for Prosody.IM

Based on old s2s module.
The module sends presence messages, chat messages and group chat.

It keeps a global local list of remote users. When local user send one message, if the user is not on local session, then module look on this remote users list, if one user is on it, then the module redirect the message to the cluster node of the user.

Example configuration for 3 clusters:

cluster_server_interfaces = { "*" } -- listen on all interfaces

Prosody configuration on cluster1:

cluster_node_name = "sv01.yourcluster.com";
cluster_servers = { "sv02.yourcluster.com:7473", "sv03.yourcluster.com:7473"};
cluster_server_port = 7473;

Prosody configuration on cluster2:

cluster_node_name = "sv02.yourcluster.com";
cluster_servers = { "sv01.yourcluster.com:7473", "sv03.yourcluster.com:7473"};
cluster_server_port = 7473;

Prosody configuration on cluster3:

cluster_node_name = "sv03.yourcluster.com";
cluster_servers = { "sv01.yourcluster.com:7473", "sv02.yourcluster.com:7473"};
cluster_server_port = 7473;

Cenário utilizado: 
Servidores mysql em replication master/master entre os nodes do prosody, usando o mesmo banco de dados.
Mysql servers in master/master replication between prosody nodes, using the same database.

Tested in prosody version 0.12.3 


