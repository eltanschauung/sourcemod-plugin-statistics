#pragma semicolon 1
#pragma newdecls required

#include <sourcemod>
#include <plugin_statistics>

public Plugin myinfo =
{
    name = "Plugin Statistics Example",
    author = "Hombre",
    description = "Records player connections",
    version = "1.0.0",
    url = ""
};

public void OnClientPostAdminCheck(int client)
{
    if (!IsFakeClient(client))
    {
        PluginStats_Record("client_ready");
    }
}
