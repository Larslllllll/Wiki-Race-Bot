using Microsoft.Data.Sqlite;

namespace WikiRaceBot;

/// <summary>BFS over the SQLite graph built by the Python crawler.</summary>
class GraphDb : IDisposable
{
    private readonly SqliteConnection _conn;

    public GraphDb(string dbPath)
    {
        _conn = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly;Cache=Shared");
        _conn.Open();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "PRAGMA cache_size=-524288; PRAGMA mmap_size=4294967296; PRAGMA temp_store=MEMORY;";
        cmd.ExecuteNonQuery();
    }

    public bool ContainsNode(string lang, string title)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM nodes WHERE lang=@l AND title=@t LIMIT 1";
        cmd.Parameters.AddWithValue("@l", lang);
        cmd.Parameters.AddWithValue("@t", title);
        return cmd.ExecuteScalar() != null;
    }

    public List<(string Lang, string Title)> Neighbors(string lang, string title)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT to_lang, to_title FROM edges WHERE from_lang=@l AND from_title=@t";
        cmd.Parameters.AddWithValue("@l", lang);
        cmd.Parameters.AddWithValue("@t", title);
        var result = new List<(string, string)>();
        using var r = cmd.ExecuteReader();
        while (r.Read()) result.Add((r.GetString(0), r.GetString(1)));
        return result;
    }

    public long? GetPageId(string lang, string title)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT page_id FROM nodes WHERE lang=@l AND title=@t LIMIT 1";
        cmd.Parameters.AddWithValue("@l", lang);
        cmd.Parameters.AddWithValue("@t", title);
        var v = cmd.ExecuteScalar();
        return v is long l ? l : null;
    }

    /// <summary>BFS — returns null if no path found within limits.</summary>
    public List<(string Lang, string Title)>? ShortestPath(
        string lang, string startTitle, string destTitle,
        int maxDepth = 8, int maxNodes = 200_000, int timeoutMs = 10_000)
    {
        var start = (lang, startTitle);
        var dest  = (lang, destTitle);
        if (start == dest) return [start];

        var parents  = new Dictionary<(string, string), (string, string)?> { [start] = null };
        var frontier = new List<(string, string)> { start };
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);

        for (int depth = 0; depth < maxDepth; depth++)
        {
            if (frontier.Count == 0 || parents.Count > maxNodes || DateTime.UtcNow > deadline)
                break;

            var next = new List<(string, string)>();
            foreach (var node in frontier)
            {
                foreach (var nbr in Neighbors(node.Item1, node.Item2))
                {
                    if (parents.ContainsKey(nbr)) continue;
                    parents[nbr] = node;
                    if (nbr == dest)
                    {
                        var path = new List<(string, string)>();
                        (string, string)? cur = dest;
                        while (cur != null) { path.Add(cur.Value); cur = parents[cur.Value]; }
                        path.Reverse();
                        return path;
                    }
                    next.Add(nbr);
                }
            }
            frontier = next;
        }
        return null;
    }

    public void Dispose() => _conn.Dispose();
}
