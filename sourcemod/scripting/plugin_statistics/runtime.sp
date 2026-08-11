void RefreshServerContext()
{
    ConVar hostPort = FindConVar("hostport");
    g_HostPort = hostPort == null ? 27015 : hostPort.IntValue;
    GetCurrentMap(g_MapName, sizeof(g_MapName));
    DeriveGamemode(g_MapName, g_Gamemode, sizeof(g_Gamemode));
}

void BeginMapSession(const char[] reason)
{
    g_SessionStartedAt = GetTime();
    Format(g_SessionKey, sizeof(g_SessionKey), "%d_%04X", g_SessionStartedAt, GetRandomInt(0, 65535));
    g_SessionActive = true;
    g_FrameCount = 0;
    g_TickSampleCount = 0;
    g_FrameWindowStartedAt = GetEngineTime();
    g_LastObservedTickrate = ExpectedTickrate();
    g_TickrateTotal = 0.0;
    g_MinimumTickrate = 0.0;

    char message[128];
    Format(message, sizeof(message), "reason=%s", reason);
    QueueRecord(PluginStatsRecord_SessionStart, "plugin_statistics", "map_session_start", message, "");
}

void EndMapSession(const char[] reason)
{
    if (!g_SessionActive)
    {
        return;
    }

    QueueRecord(PluginStatsRecord_SessionEnd, "plugin_statistics", "map_session_end", "", reason);
    g_SessionActive = false;
}

bool QueueRecord(
    PluginStatsRecordType type,
    const char[] sourcePlugin,
    const char[] eventName,
    const char[] message,
    const char[] endReason)
{
    if (g_Queue == null || g_Enabled == null || !g_Enabled.BoolValue)
    {
        return false;
    }

    int queueMax = g_QueueMax == null ? 5000 : g_QueueMax.IntValue;
    while (g_Queue.Length >= queueMax)
    {
        g_Queue.Erase(0);
        g_DroppedEvents++;
    }

    PluginStatsRecord record;
    record.Type = type;
    record.OccurredAt = GetTime();
    record.HostPort = g_HostPort;
    record.ServerTick = GetGameTickCount();
    record.SessionStartedAt = g_SessionStartedAt;
    record.SessionEndedAt = type == PluginStatsRecord_SessionEnd ? record.OccurredAt : 0;
    record.SessionSampleCount = g_TickSampleCount;
    record.TickInterval = GetTickInterval();
    record.ExpectedTickrate = record.TickInterval > 0.0 ? 1.0 / record.TickInterval : 0.0;
    record.ObservedTickrate = g_LastObservedTickrate > 0.0 ? g_LastObservedTickrate : record.ExpectedTickrate;
    record.SessionAverageTickrate =
        g_TickSampleCount > 0 ? g_TickrateTotal / float(g_TickSampleCount) : record.ObservedTickrate;
    record.SessionMinimumTickrate =
        g_MinimumTickrate > 0.0 ? g_MinimumTickrate : record.ObservedTickrate;
    strcopy(record.SessionKey, sizeof(record.SessionKey), g_SessionKey);
    strcopy(record.MapName, sizeof(record.MapName), g_MapName);
    strcopy(record.Gamemode, sizeof(record.Gamemode), g_Gamemode);
    strcopy(record.SourcePlugin, sizeof(record.SourcePlugin), sourcePlugin);
    strcopy(record.EventName, sizeof(record.EventName), eventName);
    strcopy(record.Message, sizeof(record.Message), message);
    strcopy(record.EndReason, sizeof(record.EndReason), endReason);
    BuildEventId(record.EventId, sizeof(record.EventId), record);
    g_Queue.PushArray(record);
    return true;
}

void BuildEventId(char[] output, int maxlen, PluginStatsRecord record)
{
    int sequence = g_NextEventId++;
    if (g_NextEventId <= 0)
    {
        g_NextEventId = 1;
    }

    Format(
        output,
        maxlen,
        "%d-%d-%d-%d-%d",
        record.HostPort,
        record.SessionStartedAt,
        record.OccurredAt,
        view_as<int>(record.Type),
        sequence);
}

bool IsSafeEventName(const char[] value)
{
    if (!value[0])
    {
        return false;
    }

    for (int i = 0; value[i]; i++)
    {
        char c = value[i];
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_'))
        {
            return false;
        }
    }
    return true;
}

void NormalizePluginName(char[] value, int maxlen)
{
    int start = 0;
    for (int i = 0; value[i]; i++)
    {
        if (value[i] == '/' || value[i] == '\\')
        {
            start = i + 1;
        }
    }

    if (start > 0)
    {
        int write = 0;
        for (int read = start; value[read] && write < maxlen - 1; read++)
        {
            value[write++] = value[read];
        }
        value[write] = '\0';
    }

    int length = strlen(value);
    if (length > 4 && StrEqual(value[length - 4], ".smx", false))
    {
        value[length - 4] = '\0';
    }

    for (int i = 0; value[i]; i++)
    {
        char c = value[i];
        if (c >= 'A' && c <= 'Z')
        {
            value[i] = c + 32;
        }
        else if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_'))
        {
            value[i] = '_';
        }
    }
}

void DeriveGamemode(const char[] mapName, char[] output, int maxlen)
{
    static const char prefixes[][] =
    {
        "arena", "cp", "ctf", "koth", "mvm", "pass", "pd", "plr", "pl", "rd", "sd", "tc", "vsh", "zi"
    };

    for (int i = 0; i < sizeof(prefixes); i++)
    {
        int length = strlen(prefixes[i]);
        if (strncmp(mapName, prefixes[i], length, false) == 0 && mapName[length] == '_')
        {
            strcopy(output, maxlen, prefixes[i]);
            return;
        }
    }
    strcopy(output, maxlen, "other");
}

float ExpectedTickrate()
{
    float interval = GetTickInterval();
    return interval > 0.0 ? 1.0 / interval : 0.0;
}

void RecreateTimers()
{
    CancelTimer(g_FlushTimer);
    CancelTimer(g_TickSampleTimer);

    float flushInterval =
        g_FlushInterval == null ? PLUGIN_STATS_DEFAULT_FLUSH_INTERVAL : g_FlushInterval.FloatValue;
    g_FlushTimer =
        CreateTimer(flushInterval, Timer_Flush, _, TIMER_REPEAT | TIMER_FLAG_NO_MAPCHANGE);

    float sampleInterval = g_TickSampleInterval == null
        ? PLUGIN_STATS_DEFAULT_TICK_SAMPLE_INTERVAL
        : g_TickSampleInterval.FloatValue;
    if (sampleInterval > 0.0)
    {
        g_TickSampleTimer =
            CreateTimer(sampleInterval, Timer_TickSample, _, TIMER_REPEAT | TIMER_FLAG_NO_MAPCHANGE);
    }
}

void StopMapTimers()
{
    CancelTimer(g_FlushTimer);
    CancelTimer(g_TickSampleTimer);
}

void CancelTimer(Handle &timer)
{
    if (timer != null)
    {
        delete timer;
        timer = null;
    }
}

public Action Timer_Flush(Handle timer)
{
    FlushQueue();
    return Plugin_Continue;
}

public Action Timer_TickSample(Handle timer)
{
    if (g_SessionActive)
    {
        QueueRecord(PluginStatsRecord_TickSample, "plugin_statistics", "tick_sample", "", "");
    }
    return Plugin_Continue;
}
