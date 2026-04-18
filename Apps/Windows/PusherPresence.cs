using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace WikiRaceBot;

/// <summary>Subscribes to the Pusher presence channel so the bot appears in the browser lobby.</summary>
class PusherPresence : IAsyncDisposable
{
    private const string AppKey  = "932edcd098e03d77349f";
    private const string WssUrl  = $"wss://ws.wiki-race.com/app/{AppKey}?protocol=7&client=cs-bot&version=7.0.3&flash=false";

    private readonly WikiRaceClient _client;
    private readonly string         _gameId;
    private readonly SessionInfo    _session;
    private readonly CancellationTokenSource _cts = new();
    private Task? _task;

    public PusherPresence(WikiRaceClient client, string gameId, SessionInfo session)
    {
        _client  = client;
        _gameId  = gameId;
        _session = session;
    }

    public void Start() => _task = Task.Run(RunAsync);

    private async Task RunAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            try { await ConnectOnceAsync(); }
            catch (Exception ex) { Console.WriteLine($"[pusher] error: {ex.Message}"); }
            if (!_cts.IsCancellationRequested)
                await Task.Delay(5000, _cts.Token).ContinueWith(_ => { });
        }
    }

    private async Task ConnectOnceAsync()
    {
        using var ws = new ClientWebSocket();
        await ws.ConnectAsync(new Uri(WssUrl), _cts.Token);

        // ── handshake ──────────────────────────────────────────────────
        var hMsg  = await ReceiveJsonAsync(ws);
        if (hMsg?["event"]?.GetValue<string>() != "pusher:connection_established")
            return;

        var data     = JsonNode.Parse(hMsg["data"]!.GetValue<string>())!;
        var socketId = data["socket_id"]!.GetValue<string>();
        var channel  = $"presence-game-{_gameId}";
        Console.WriteLine($"[pusher] connected  socket_id={socketId}");

        // ── auth ───────────────────────────────────────────────────────
        var authData = await _client.PusherAuthAsync(_gameId, _session, socketId, channel);

        // ── subscribe ──────────────────────────────────────────────────
        await SendJsonAsync(ws, new
        {
            @event = "pusher:subscribe",
            data   = new
            {
                auth         = authData["auth"]?.GetValue<string>() ?? "",
                channel_data = authData["channel_data"]?.GetValue<string>() ?? "",
                channel,
            }
        });

        // ── keep-alive ─────────────────────────────────────────────────
        while (ws.State == WebSocketState.Open && !_cts.IsCancellationRequested)
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
            timeoutCts.CancelAfter(35_000);

            var msg = await ReceiveJsonAsync(ws, timeoutCts.Token);
            if (msg == null) break;

            var evt = msg["event"]?.GetValue<string>();
            if (evt == "pusher:subscription_succeeded")
                Console.WriteLine($"[pusher] subscribed — bot now visible in lobby");
            else if (evt == "pusher:ping")
                await SendJsonAsync(ws, new { @event = "pusher:pong", data = new { } });
            else if (evt == "pusher:error")
                Console.WriteLine($"[pusher] subscription error: {msg}");
        }
    }

    private static async Task<JsonObject?> ReceiveJsonAsync(
        ClientWebSocket ws, CancellationToken ct = default)
    {
        var buf = new byte[65536];
        var sb  = new StringBuilder();
        WebSocketReceiveResult result;
        do
        {
            result = await ws.ReceiveAsync(buf, ct);
            if (result.MessageType == WebSocketMessageType.Close) return null;
            sb.Append(Encoding.UTF8.GetString(buf, 0, result.Count));
        } while (!result.EndOfMessage);
        return JsonNode.Parse(sb.ToString())?.AsObject();
    }

    private static async Task SendJsonAsync(ClientWebSocket ws, object payload)
    {
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload));
        await ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_task != null) await _task.ContinueWith(_ => { });
    }
}
