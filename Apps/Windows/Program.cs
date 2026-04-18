using WikiRaceBot;

// ── CLI args ──────────────────────────────────────────────────────────────────
string? joinCode  = null;
string  name      = "WikiBot";
string  dbPath    = @"..\..\crawl_output\graph.db"; // relative to exe, adjust if needed
string  lang      = "en";
bool    stay      = false;

for (int i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "--join":   joinCode = args[++i]; break;
        case "--name":   name     = args[++i]; break;
        case "--db":     dbPath   = args[++i]; break;
        case "--lang":   lang     = args[++i]; break;
        case "--stay":   stay     = true;      break;
    }
}

// ── Setup ─────────────────────────────────────────────────────────────────────
using var client = new WikiRaceClient();

// Join / create
string    gameId;
SessionInfo session;
string    playerName;

if (joinCode != null)
{
    (gameId, session, playerName) = await client.JoinGameAsync(joinCode, name);
    Console.WriteLine($"[lobby] joined  id={gameId}  as '{playerName}'");
}
else
{
    (gameId, session) = await client.CreateGameAsync(name);
    playerName = name;
    Console.WriteLine($"[lobby] created id={gameId}");
    Console.WriteLine($"[lobby] share:  https://wiki-race.com/?lobbyCode={gameId}");
}

// Pusher presence — makes bot visible in browser
await using var pusher = new PusherPresence(client, gameId, session);
pusher.Start();
await Task.Delay(1500); // let Pusher connect

// Load graph
GraphDb? graph = null;
if (File.Exists(dbPath))
{
    graph = new GraphDb(dbPath);
    Console.WriteLine($"[graph] opened: {dbPath}");
}
else
{
    Console.WriteLine($"[graph] not found at {dbPath} — cannot play without graph");
    Console.WriteLine("Pass --db <path-to-graph.db>");
    return 1;
}

int roundsPlayed = 0;

while (true)
{
    // ── Wait for game start ───────────────────────────────────────────────
    Console.WriteLine("[lobby] waiting for game to start …");
    var snap = await client.WaitForStateAsync(gameId, session, "in_progress");

    var gameNode = snap["game"]!.AsObject();
    var settingsNode = gameNode["settings"]!.AsObject();
    var startNode = settingsNode["start"]!.AsObject();
    var destNode  = settingsNode["destination"]!.AsObject();

    var startTitle = startNode["title"]!.GetValue<string>();
    var destTitle  = destNode["title"]!.GetValue<string>();
    var startId    = startNode["pageid"]?.GetValue<long?>();
    var destId     = destNode["pageid"]?.GetValue<long?>();
    var gameLang   = settingsNode["language"]?.GetValue<string>() ?? lang;

    Console.WriteLine($"[game] '{startTitle}' → '{destTitle}'  (lang={gameLang})");

    var startRef = new PageRef(gameLang, startTitle, startId);
    var destRef  = new PageRef(gameLang, destTitle, destId);

    // ── Presence ping ─────────────────────────────────────────────────────
    try { await client.SubmitPathAsync(gameId, session, [startRef]); } catch { }

    // ── BFS ───────────────────────────────────────────────────────────────
    var t0   = DateTime.UtcNow;
    var nodes = graph.ShortestPath(gameLang, startTitle, destTitle);
    var dt   = (DateTime.UtcNow - t0).TotalSeconds;

    if (nodes == null)
    {
        Console.WriteLine($"[bot] no graph path found ({dt:F1}s) — surrendering");
        await client.SurrenderAsync(gameId, session);
    }
    else
    {
        var path = nodes.Select((n, i) =>
        {
            if (i == 0) return startRef;
            if (i == nodes.Count - 1) return destRef;
            return new PageRef(n.Lang, n.Title, graph.GetPageId(n.Lang, n.Title));
        }).ToList();

        var route = string.Join(" → ", path.Select(p => p.Title));
        Console.WriteLine($"[bot] path ({path.Count - 1} hops, {dt:F1}s): {route}");

        await client.SubmitPathAsync(gameId, session, path);
        Console.WriteLine($"[result] submitted {path.Count - 1} hop(s)");
    }

    roundsPlayed++;

    if (!stay) break;

    Console.WriteLine("[lobby] staying — waiting for next round …");
    try { await client.ContinueAsync(gameId, session); } catch { }
}

graph.Dispose();
return 0;
