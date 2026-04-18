using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace WikiRaceBot;

class WikiRaceClient : IDisposable
{
    private readonly HttpClient _http;
    private const string BaseUrl = "https://wiki-race.com";

    public WikiRaceClient()
    {
        _http = new HttpClient();
        _http.DefaultRequestHeaders.Add("User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36");
        _http.DefaultRequestHeaders.Add("Accept", "application/json");
        _http.Timeout = TimeSpan.FromSeconds(20);
    }

    public async Task<(string GameId, SessionInfo Session, string PlayerName)> JoinGameAsync(
        string gameId, string playerName)
    {
        var resp = await PostAsync("/api/game/join", new { gameId, playerName });
        var session = ParseSession(resp);
        var actualName = resp["playerName"]?.GetValue<string>() ?? playerName;
        return (resp["gameId"]!.GetValue<string>(), session, actualName);
    }

    public async Task<(string GameId, SessionInfo Session)> CreateGameAsync(string playerName)
    {
        var resp = await PostAsync("/api/game", new { playerName });
        return (resp["gameId"]!.GetValue<string>(), ParseSession(resp));
    }

    public async Task StartGameAsync(string gameId, SessionInfo session, GameSettings settings)
    {
        await PostAsync("/api/game/start", new
        {
            gameId,
            session = session.ToPayload(),
            settings = new
            {
                language = settings.Language,
                start    = new { title = settings.Start.Title,       pageid = settings.Start.PageId },
                destination = new { title = settings.Destination.Title, pageid = settings.Destination.PageId },
            }
        });
    }

    public async Task SubmitPathAsync(string gameId, SessionInfo session, IEnumerable<PageRef> path)
    {
        await PostAsync("/api/game/location", new
        {
            gameId,
            session = session.ToPayload(),
            path    = path.Select(p => p.ToPathEntry()).ToArray(),
        });
    }

    public async Task SurrenderAsync(string gameId, SessionInfo session) =>
        await PostAsync("/api/game/surrender", new { gameId, session = session.ToPayload() });

    public async Task ContinueAsync(string gameId, SessionInfo session) =>
        await PostAsync("/api/game/continue", new { gameId, session = session.ToPayload() });

    // Pusher auth — form-encoded POST body
    public async Task<JsonObject> PusherAuthAsync(
        string gameId, SessionInfo session, string socketId, string channelName)
    {
        var form = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["socket_id"]    = socketId,
            ["channel_name"] = channelName,
            ["sessionId"]    = session.Id,
            ["secretToken"]  = session.SecretToken,
            ["gameId"]       = gameId,
        });
        var r = await _http.PostAsync($"{BaseUrl}/api/game/pusher/auth", form);
        r.EnsureSuccessStatusCode();
        var json = await r.Content.ReadAsStringAsync();
        return JsonNode.Parse(json)!.AsObject();
    }

    // Poll game state from __NEXT_DATA__
    public async Task<JsonObject> FetchSnapshotAsync(string gameId, SessionInfo session)
    {
        var url = $"{BaseUrl}/game?gameId={gameId}&sessionId={session.Id}&secretToken={session.SecretToken}";
        var html = await _http.GetStringAsync(url);
        var m = System.Text.RegularExpressions.Regex.Match(
            html, @"<script id=""__NEXT_DATA__"" type=""application/json"">(.*?)</script>",
            System.Text.RegularExpressions.RegexOptions.Singleline);
        if (!m.Success) throw new Exception("__NEXT_DATA__ not found");
        var root = JsonNode.Parse(m.Groups[1].Value)!;
        return root["props"]!["pageProps"]!.AsObject();
    }

    public async Task<JsonObject> WaitForStateAsync(
        string gameId, SessionInfo session, string desiredState,
        int timeoutSeconds = 600, int pollMs = 2000)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        string? last = null;
        while (true)
        {
            var snap = await FetchSnapshotAsync(gameId, session);
            var state = snap["game"]?["state"]?.GetValue<string>();
            if (state != last) { Console.WriteLine($"[lobby] state={state}"); last = state; }
            if (state == desiredState) return snap;
            if (DateTime.UtcNow > deadline) throw new TimeoutException($"Timed out waiting for {desiredState}");
            await Task.Delay(pollMs);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────

    private async Task<JsonObject> PostAsync(string path, object payload)
    {
        var resp = await _http.PostAsJsonAsync($"{BaseUrl}{path}", payload);
        var body = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
            throw new Exception($"POST {path} → HTTP {(int)resp.StatusCode}: {body}");
        return JsonNode.Parse(body)!.AsObject();
    }

    private static SessionInfo ParseSession(JsonObject resp)
    {
        var s = resp["session"]!.AsObject();
        return new SessionInfo(s["id"]!.GetValue<string>(), s["secretToken"]!.GetValue<string>());
    }

    public void Dispose() => _http.Dispose();
}
