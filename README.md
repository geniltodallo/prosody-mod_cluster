# prosody-mod_cluster
Module cluster for Prosody IM

This is a cluster module for Prosody.IM

Based on old s2s module.
The module sends presence messages, chat messages and group chat.

Example configuration for 3 clusters:

Prosody configuration on cluster1:
cluster_name = "sv01.yourcluster.com";
cluster_servers = { "sv02.yourcluster.com:7473", "sv03.yourcluster.com:7473"};

Prosody configuration on cluster2:
cluster_name = "sv02.yourcluster.com";
cluster_servers = { "sv01.yourcluster.com:7473", "sv03.yourcluster.com:7473"};

Prosody configuration on cluster3:
cluster_name = "sv03.yourcluster.com";
cluster_servers = { "sv01.yourcluster.com:7473", "sv02.yourcluster.com:7473"};



