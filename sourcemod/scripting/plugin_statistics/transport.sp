void ConnectSocket()
{
    if (g_Enabled == null || !g_Enabled.BoolValue || g_Connected || g_Connecting)
    {
        return;
    }
    if (GetFeatureStatus(FeatureType_Native, "Socket.Connect") != FeatureStatus_Available)
    {
        LogError("Socket extension is unavailable; statistics will remain queued");
        ScheduleReconnect();
        return;
    }

    CancelTimer(g_ReconnectTimer);
    DisconnectSocket();

    char host[128];
    g_Host.GetString(host, sizeof(host));
    int port = g_Port.IntValue;

    g_Socket = new Socket(SOCKET_TCP, Socket_OnError);
    if (g_Socket == null)
    {
        ScheduleReconnect();
        return;
    }

    g_Socket.SetOption(SocketKeepAlive, 1);
    g_Socket.SetOption(SocketSendBuffer, 65536);
    g_Socket.SetOption(SocketReceiveBuffer, 65536);
    g_Connecting = true;
    g_Socket.Connect(Socket_OnConnected, Socket_OnReceive, Socket_OnDisconnected, host, port);
}

void DisconnectSocket()
{
    g_Connecting = false;
    g_Connected = false;
    g_AwaitingAck = false;
    g_PendingBatchId = 0;
    g_RecvBufferLength = 0;
    g_RecvBuffer[0] = '\0';
    if (g_Socket != null)
    {
        delete g_Socket;
        g_Socket = null;
    }
}

void ScheduleReconnect()
{
    if (g_ReconnectTimer == null && g_Enabled != null && g_Enabled.BoolValue)
    {
        g_ReconnectTimer = CreateTimer(2.0, Timer_Reconnect);
    }
}

public Action Timer_Reconnect(Handle timer)
{
    g_ReconnectTimer = null;
    ConnectSocket();
    return Plugin_Stop;
}

public void Socket_OnConnected(Socket socket, any data)
{
    g_Connecting = false;
    g_Connected = true;
    g_AwaitingAck = false;

    char serverId[PLUGIN_STATS_SERVER_ID_MAX];
    BuildServerId(serverId, sizeof(serverId));
    char escapedServerId[256];
    JsonEscape(serverId, escapedServerId, sizeof(escapedServerId));

    char auth[256];
    char escapedAuth[512];
    char authField[544];
    authField[0] = '\0';
    g_AuthToken.GetString(auth, sizeof(auth));
    TrimString(auth);
    if (auth[0])
    {
        JsonEscape(auth, escapedAuth, sizeof(escapedAuth));
        Format(authField, sizeof(authField), ",\"auth\":\"%s\"", escapedAuth);
    }

    char serverName[256];
    char escapedServerName[512];
    ConVar hostname = FindConVar("hostname");
    if (hostname != null)
    {
        hostname.GetString(serverName, sizeof(serverName));
    }
    JsonEscape(serverName, escapedServerName, sizeof(escapedServerName));

    char hello[1536];
    int length = Format(
        hello,
        sizeof(hello),
        "{\"type\":\"hello\",\"service\":\"plugin_statistics\",\"proto\":2,\"server_id\":\"%s\",\"server_name\":\"%s\",\"ts\":%d%s}\n",
        escapedServerId,
        escapedServerName,
        GetTime(),
        authField);
    socket.Send(hello, length);
    FlushQueue();
}

public void Socket_OnDisconnected(Socket socket, any data)
{
    RequeueInflight();
    DisconnectSocket();
    ScheduleReconnect();
}

public void Socket_OnError(Socket socket, const int errorType, const int errorNum, any data)
{
    LogError("statistics socket error type=%d errno=%d", errorType, errorNum);
    RequeueInflight();
    DisconnectSocket();
    ScheduleReconnect();
}

public void Socket_OnReceive(
    Socket socket,
    const char[] receiveData,
    const int dataSize,
    any data)
{
    if (dataSize <= 0)
    {
        return;
    }
    if (g_RecvBufferLength + dataSize >= sizeof(g_RecvBuffer))
    {
        LogError("statistics receive buffer overflow");
        g_RecvBufferLength = 0;
        return;
    }

    for (int i = 0; i < dataSize; i++)
    {
        g_RecvBuffer[g_RecvBufferLength++] = receiveData[i];
    }
    g_RecvBuffer[g_RecvBufferLength] = '\0';
    ParseResponseLines();
}

void ParseResponseLines()
{
    int start = 0;
    for (int i = 0; i < g_RecvBufferLength; i++)
    {
        if (g_RecvBuffer[i] != '\n')
        {
            continue;
        }

        int lineLength = i - start;
        char line[PLUGIN_STATS_RECV_LINE_MAX];
        if (lineLength >= sizeof(line))
        {
            lineLength = sizeof(line) - 1;
        }
        for (int j = 0; j < lineLength; j++)
        {
            line[j] = g_RecvBuffer[start + j];
        }
        line[lineLength] = '\0';
        HandleResponse(line);
        start = i + 1;
    }

    if (start > 0)
    {
        int remaining = g_RecvBufferLength - start;
        for (int i = 0; i < remaining; i++)
        {
            g_RecvBuffer[i] = g_RecvBuffer[start + i];
        }
        g_RecvBufferLength = remaining;
        g_RecvBuffer[remaining] = '\0';
    }
}

void HandleResponse(const char[] line)
{
    if (StrContains(line, "\"type\":\"ack\"") != -1)
    {
        int batchId;
        int dbErrors;
        if (!ExtractJsonInt(line, "\"batch_id\":", batchId) || batchId != g_PendingBatchId)
        {
            LogError("statistics ACK batch mismatch: %s", line);
            RequeueInflight();
            return;
        }
        if (ExtractJsonInt(line, "\"db_errors\":", dbErrors) && dbErrors > 0)
        {
            LogError("statistics backend reported %d database errors", dbErrors);
            RequeueInflight();
            return;
        }

        g_AwaitingAck = false;
        g_PendingBatchId = 0;
        g_Inflight.Clear();
        FlushQueue();
    }
    else if (StrContains(line, "\"type\":\"error\"") != -1)
    {
        LogError("statistics backend error: %s", line);
        RequeueInflight();
    }
}

void FlushQueue()
{
    if (!g_Connected || g_Socket == null || g_AwaitingAck || g_Queue == null)
    {
        ConnectSocket();
        return;
    }

    if (g_DroppedEvents > 0)
    {
        char message[128];
        Format(
            message,
            sizeof(message),
            "dropped=%d|queue_limit=%d",
            g_DroppedEvents,
            g_QueueMax.IntValue);
        g_DroppedEvents = 0;
        QueueRecord(
            PluginStatsRecord_Event,
            "plugin_statistics",
            "statistics_dropped",
            message,
            "");
    }
    if (g_Queue.Length == 0)
    {
        return;
    }

    char output[PLUGIN_STATS_BATCH_JSON_MAX];
    int batchId = g_NextBatchId++;
    int position = Format(
        output,
        sizeof(output),
        "{\"type\":\"stats_batch\",\"batch_id\":%d,\"sent_at\":%d,\"events\":[",
        batchId,
        GetTime());
    int batchMax = g_BatchMax.IntValue;
    int sent = 0;
    g_Inflight.Clear();

    PluginStatsRecord record;
    for (int i = 0; i < g_Queue.Length && sent < batchMax; i++)
    {
        if (position + 2300 >= sizeof(output))
        {
            break;
        }
        g_Queue.GetArray(i, record);
        if (sent > 0)
        {
            output[position++] = ',';
            output[position] = '\0';
        }
        position += AppendRecordJson(output[position], sizeof(output) - position, record);
        g_Inflight.PushArray(record);
        sent++;
    }

    if (sent == 0)
    {
        return;
    }

    position += Format(output[position], sizeof(output) - position, "]}\n");
    g_Socket.Send(output, position);
    g_AwaitingAck = true;
    g_PendingBatchId = batchId;
    for (int i = 0; i < sent; i++)
    {
        g_Queue.Erase(0);
    }

    if (g_Debug.BoolValue)
    {
        LogMessage(
            "sent statistics batch=%d records=%d bytes=%d remaining=%d",
            batchId,
            sent,
            position,
            g_Queue.Length);
    }
}

int AppendRecordJson(char[] output, int maxlen, PluginStatsRecord record)
{
    char recordType[16];
    char eventId[256];
    char sessionKey[256];
    char mapName[256];
    char gamemode[128];
    char sourcePlugin[128];
    char eventName[128];
    char message[1024];
    char endReason[96];
    JsonEscape(record.EventId, eventId, sizeof(eventId));
    JsonEscape(record.SessionKey, sessionKey, sizeof(sessionKey));
    JsonEscape(record.MapName, mapName, sizeof(mapName));
    JsonEscape(record.Gamemode, gamemode, sizeof(gamemode));
    JsonEscape(record.SourcePlugin, sourcePlugin, sizeof(sourcePlugin));
    JsonEscape(record.EventName, eventName, sizeof(eventName));
    JsonEscape(record.Message, message, sizeof(message));
    JsonEscape(record.EndReason, endReason, sizeof(endReason));
    GetRecordTypeName(record.Type, recordType, sizeof(recordType));

    return Format(
        output,
        maxlen,
        "{\"record_type\":\"%s\",\"event_id\":\"%s\",\"occurred_at\":%d,\"host_port\":%d,\"map_session_id\":\"%s\",\"map_name\":\"%s\",\"gamemode\":\"%s\",\"source_plugin\":\"%s\",\"event_name\":\"%s\",\"message\":\"%s\",\"server_tick\":%d,\"tick_interval_seconds\":%.9f,\"expected_tickrate\":%.3f,\"observed_tickrate\":%.3f,\"session_started_at\":%d,\"session_ended_at\":%d,\"session_sample_count\":%d,\"session_average_tickrate\":%.3f,\"session_minimum_tickrate\":%.3f,\"end_reason\":\"%s\"}",
        recordType,
        eventId,
        record.OccurredAt,
        record.HostPort,
        sessionKey,
        mapName,
        gamemode,
        sourcePlugin,
        eventName,
        message,
        record.ServerTick,
        record.TickInterval,
        record.ExpectedTickrate,
        record.ObservedTickrate,
        record.SessionStartedAt,
        record.SessionEndedAt,
        record.SessionSampleCount,
        record.SessionAverageTickrate,
        record.SessionMinimumTickrate,
        endReason);
}

void GetRecordTypeName(PluginStatsRecordType type, char[] output, int maxlen)
{
    switch (type)
    {
        case PluginStatsRecord_SessionStart:
        {
            strcopy(output, maxlen, "session_start");
            return;
        }
        case PluginStatsRecord_SessionEnd:
        {
            strcopy(output, maxlen, "session_end");
            return;
        }
        case PluginStatsRecord_TickSample:
        {
            strcopy(output, maxlen, "tick_sample");
            return;
        }
    }
    strcopy(output, maxlen, "event");
}

void RequeueInflight()
{
    if (g_Inflight == null || g_Queue == null)
    {
        return;
    }

    PluginStatsRecord record;
    for (int i = g_Inflight.Length - 1; i >= 0; i--)
    {
        g_Inflight.GetArray(i, record);
        g_Queue.ShiftUp(0);
        g_Queue.SetArray(0, record);
    }
    g_Inflight.Clear();
    g_AwaitingAck = false;
    g_PendingBatchId = 0;
}

void BuildServerId(char[] output, int maxlen)
{
    g_ServerId.GetString(output, maxlen);
    TrimString(output);
    if (output[0])
    {
        return;
    }

    char hostname[96];
    ConVar hostnameCvar = FindConVar("hostname");
    if (hostnameCvar == null)
    {
        strcopy(hostname, sizeof(hostname), "unknown");
    }
    else
    {
        hostnameCvar.GetString(hostname, sizeof(hostname));
    }
    Format(output, maxlen, "%s:%d", hostname, g_HostPort);
}

void JsonEscape(const char[] input, char[] output, int maxlen)
{
    int write = 0;
    for (int read = 0; input[read] && write < maxlen - 1; read++)
    {
        int value = input[read];
        if (value < 0)
        {
            value += 256;
        }
        if (value == '"' || value == '\\')
        {
            if (write + 2 >= maxlen)
            {
                break;
            }
            output[write++] = '\\';
            output[write++] = input[read];
        }
        else if (value == '\n' || value == '\r' || value == '\t')
        {
            if (write + 2 >= maxlen)
            {
                break;
            }
            output[write++] = '\\';
            output[write++] = value == '\n' ? 'n' : (value == '\r' ? 'r' : 't');
        }
        else if (value >= 32)
        {
            output[write++] = input[read];
        }
    }
    output[write] = '\0';
}

bool ExtractJsonInt(const char[] line, const char[] key, int &value)
{
    int position = StrContains(line, key, false);
    if (position == -1)
    {
        return false;
    }
    position += strlen(key);
    while (line[position] == ' ')
    {
        position++;
    }

    bool negative = line[position] == '-';
    if (negative)
    {
        position++;
    }
    if (line[position] < '0' || line[position] > '9')
    {
        return false;
    }

    value = 0;
    while (line[position] >= '0' && line[position] <= '9')
    {
        value = value * 10 + line[position] - '0';
        position++;
    }
    if (negative)
    {
        value = -value;
    }
    return true;
}
