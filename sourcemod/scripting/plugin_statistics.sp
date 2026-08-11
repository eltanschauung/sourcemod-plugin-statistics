#pragma semicolon 1
#pragma newdecls required

#include <sourcemod>

#include <socket>

#define PLUGIN_STATS_VERSION "1.0.0"
#define PLUGIN_STATS_DEFAULT_FLUSH_INTERVAL 5.0
#define PLUGIN_STATS_DEFAULT_TICK_SAMPLE_INTERVAL 10.0
#define PLUGIN_STATS_EVENT_NAME_MAX 64
#define PLUGIN_STATS_MESSAGE_MAX 512
#define PLUGIN_STATS_SOURCE_MAX 64
#define PLUGIN_STATS_EVENT_ID_MAX 128
#define PLUGIN_STATS_SESSION_KEY_MAX 128
#define PLUGIN_STATS_SERVER_ID_MAX 128
#define PLUGIN_STATS_BATCH_JSON_MAX 32768
#define PLUGIN_STATS_RECV_BUFFER_MAX 32768
#define PLUGIN_STATS_RECV_LINE_MAX 2048

enum PluginStatsRecordType
{
    PluginStatsRecord_Event = 0,
    PluginStatsRecord_SessionStart,
    PluginStatsRecord_SessionEnd,
    PluginStatsRecord_TickSample
}

enum struct PluginStatsRecord
{
    PluginStatsRecordType Type;
    int OccurredAt;
    int HostPort;
    int ServerTick;
    int SessionStartedAt;
    int SessionEndedAt;
    int SessionSampleCount;
    float TickInterval;
    float ExpectedTickrate;
    float ObservedTickrate;
    float SessionAverageTickrate;
    float SessionMinimumTickrate;
    char EventId[PLUGIN_STATS_EVENT_ID_MAX];
    char SessionKey[PLUGIN_STATS_SESSION_KEY_MAX];
    char MapName[128];
    char Gamemode[64];
    char SourcePlugin[PLUGIN_STATS_SOURCE_MAX];
    char EventName[PLUGIN_STATS_EVENT_NAME_MAX];
    char Message[PLUGIN_STATS_MESSAGE_MAX];
    char EndReason[32];
}

public Plugin myinfo =
{
    name = "SourceMod Plugin Statistics",
    author = "Hombre",
    description = "Defers structured SourceMod statistics to a Rust database writer",
    version = PLUGIN_STATS_VERSION,
    url = "https://github.com/eltanschauung/sourcemod-plugin-statistics"
};

ArrayList g_Queue = null;
ArrayList g_Inflight = null;
Handle g_FlushTimer = null;
Handle g_TickSampleTimer = null;
Handle g_ReconnectTimer = null;
Socket g_Socket = null;

ConVar g_Enabled = null;
ConVar g_Host = null;
ConVar g_Port = null;
ConVar g_ServerId = null;
ConVar g_AuthToken = null;
ConVar g_QueueMax = null;
ConVar g_BatchMax = null;
ConVar g_FlushInterval = null;
ConVar g_TickSampleInterval = null;
ConVar g_Debug = null;

bool g_Connecting = false;
bool g_Connected = false;
bool g_AwaitingAck = false;
bool g_SessionActive = false;
int g_PendingBatchId = 0;
int g_NextBatchId = 1;
int g_NextEventId = 1;
int g_DroppedEvents = 0;
int g_RecvBufferLength = 0;
int g_HostPort = 0;
int g_SessionStartedAt = 0;
int g_FrameCount = 0;
int g_TickSampleCount = 0;
float g_FrameWindowStartedAt = 0.0;
float g_LastObservedTickrate = 0.0;
float g_TickrateTotal = 0.0;
float g_MinimumTickrate = 0.0;
char g_SessionKey[PLUGIN_STATS_SESSION_KEY_MAX];
char g_MapName[128];
char g_Gamemode[64];
char g_RecvBuffer[PLUGIN_STATS_RECV_BUFFER_MAX];

public APLRes AskPluginLoad2(Handle myself, bool late, char[] error, int maxlen)
{
    RegPluginLibrary("plugin_statistics");
    CreateNative("PluginStats_Record", Native_Record);
    return APLRes_Success;
}

public void OnPluginStart()
{
    g_Queue = new ArrayList(sizeof(PluginStatsRecord));
    g_Inflight = new ArrayList(sizeof(PluginStatsRecord));

    g_Enabled = CreateConVar("sm_plugin_statistics_enabled", "1", "Enable SourceMod plugin statistics.", _, true, 0.0, true, 1.0);
    g_Host = CreateConVar("sm_plugin_statistics_host", "127.0.0.1", "Rust statistics listener host.");
    g_Port = CreateConVar("sm_plugin_statistics_port", "28019", "Rust statistics listener port.", _, true, 1.0, true, 65535.0);
    g_ServerId = CreateConVar("sm_plugin_statistics_server_id", "", "Stable server identifier. Defaults to hostname:hostport.");
    g_AuthToken = CreateConVar("sm_plugin_statistics_auth_token", "", "Optional Rust protocol authentication token.", FCVAR_PROTECTED);
    g_QueueMax = CreateConVar("sm_plugin_statistics_queue_max", "5000", "Maximum queued statistics records.", _, true, 100.0, true, 50000.0);
    g_BatchMax = CreateConVar("sm_plugin_statistics_batch_max", "128", "Maximum records in one Rust batch.", _, true, 1.0, true, 512.0);
    g_FlushInterval = CreateConVar("sm_plugin_statistics_flush_interval", "5.0", "Seconds between queue flushes.", _, true, 1.0, true, 60.0);
    g_TickSampleInterval = CreateConVar("sm_plugin_statistics_tick_sample_interval", "10.0", "Seconds between standalone tickrate samples. Set to 0 to disable.", _, true, 0.0, true, 300.0);
    g_Debug = CreateConVar("sm_plugin_statistics_debug", "0", "Enable verbose statistics transport logging.", _, true, 0.0, true, 1.0);

    HookConVarChange(g_Enabled, OnTransportConVarChanged);
    HookConVarChange(g_Host, OnTransportConVarChanged);
    HookConVarChange(g_Port, OnTransportConVarChanged);
    HookConVarChange(g_ServerId, OnTransportConVarChanged);
    HookConVarChange(g_AuthToken, OnTransportConVarChanged);
    HookConVarChange(g_FlushInterval, OnTimerConVarChanged);
    HookConVarChange(g_TickSampleInterval, OnTimerConVarChanged);

    RegAdminCmd("sm_plugin_statistics_status", Command_Status, ADMFLAG_ROOT,
        "Shows the plugin statistics transport and queue state.");
    RegAdminCmd("sm_plugin_statistics_test", Command_Test, ADMFLAG_ROOT,
        "Queues a diagnostic statistics event.");

    AutoExecConfig(true, "plugin_statistics");
    RefreshServerContext();
    BeginMapSession("plugin_load");
    RecreateTimers();
}

public void OnConfigsExecuted()
{
    RefreshServerContext();
    RecreateTimers();
    ConnectSocket();
}

public void OnMapStart()
{
    if (g_SessionActive)
    {
        EndMapSession("superseded");
    }

    RefreshServerContext();
    BeginMapSession("map_start");
    RecreateTimers();
    ConnectSocket();
}

public void OnMapEnd()
{
    EndMapSession("map_end");
    FlushQueue();
    StopMapTimers();
}

public void OnPluginEnd()
{
    EndMapSession("plugin_unload");
    FlushQueue();
    StopMapTimers();
    CancelTimer(g_ReconnectTimer);
    DisconnectSocket();
    delete g_Queue;
    delete g_Inflight;
}

public void OnGameFrame()
{
    if (!g_SessionActive)
    {
        return;
    }

    float now = GetEngineTime();
    if (g_FrameWindowStartedAt <= 0.0)
    {
        g_FrameWindowStartedAt = now;
        g_FrameCount = 0;
    }

    g_FrameCount++;
    float elapsed = now - g_FrameWindowStartedAt;
    if (elapsed < 1.0)
    {
        return;
    }

    g_LastObservedTickrate = float(g_FrameCount) / elapsed;
    g_TickrateTotal += g_LastObservedTickrate;
    g_TickSampleCount++;
    if (g_MinimumTickrate <= 0.0 || g_LastObservedTickrate < g_MinimumTickrate)
    {
        g_MinimumTickrate = g_LastObservedTickrate;
    }

    g_FrameCount = 0;
    g_FrameWindowStartedAt = now;
}

public any Native_Record(Handle plugin, int numParams)
{
    char eventName[PLUGIN_STATS_EVENT_NAME_MAX];
    char message[PLUGIN_STATS_MESSAGE_MAX];
    GetNativeString(1, eventName, sizeof(eventName));
    if (numParams >= 2)
    {
        GetNativeString(2, message, sizeof(message));
    }

    TrimString(eventName);
    if (!IsSafeEventName(eventName))
    {
        ThrowNativeError(SP_ERROR_PARAM, "event name must contain only lowercase letters, numbers, and underscores");
        return false;
    }

    char sourcePlugin[PLATFORM_MAX_PATH];
    GetPluginFilename(plugin, sourcePlugin, sizeof(sourcePlugin));
    NormalizePluginName(sourcePlugin, sizeof(sourcePlugin));
    return QueueRecord(PluginStatsRecord_Event, sourcePlugin, eventName, message, "");
}

public Action Command_Status(int client, int args)
{
    ReplyToCommand(
        client,
        "[PluginStats] enabled=%d connected=%d acknowledged=%d queued=%d inflight=%d dropped=%d session=%s map=%s observed_tickrate=%.3f",
        g_Enabled.BoolValue,
        g_Connected,
        !g_AwaitingAck,
        g_Queue.Length,
        g_Inflight.Length,
        g_DroppedEvents,
        g_SessionKey,
        g_MapName,
        g_LastObservedTickrate);
    return Plugin_Handled;
}

public Action Command_Test(int client, int args)
{
    char message[128];
    Format(message, sizeof(message), "caller=%d", client);

    if (!QueueRecord(PluginStatsRecord_Event, "plugin_statistics", "diagnostic_test", message, ""))
    {
        ReplyToCommand(client, "[PluginStats] Diagnostic event was not queued.");
        return Plugin_Handled;
    }

    FlushQueue();
    ReplyToCommand(client, "[PluginStats] Diagnostic event queued.");
    return Plugin_Handled;
}

public void OnTransportConVarChanged(ConVar convar, const char[] oldValue, const char[] newValue)
{
    DisconnectSocket();
    if (g_Enabled.BoolValue)
    {
        ConnectSocket();
    }
}

public void OnTimerConVarChanged(ConVar convar, const char[] oldValue, const char[] newValue)
{
    RecreateTimers();
}

#include "plugin_statistics/runtime.sp"
#include "plugin_statistics/transport.sp"
