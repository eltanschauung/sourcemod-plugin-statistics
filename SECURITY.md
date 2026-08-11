# Security

Report security issues privately to the repository owner instead of opening a public issue.

The daemon is intended to listen on loopback. If SourceMod and the daemon run on different hosts, restrict the listener with a firewall, configure `PLUGIN_STATS_AUTH_TOKEN`, and terminate transport encryption before traffic reaches the daemon.
