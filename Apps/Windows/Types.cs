namespace WikiRaceBot;

record SessionInfo(string Id, string SecretToken)
{
    public static SessionInfo FromDict(Dictionary<string, object> d) =>
        new((string)d["id"], (string)d["secretToken"]);

    public Dictionary<string, object> ToPayload() =>
        new() { ["id"] = Id, ["secretToken"] = SecretToken };
}

record PageRef(string Lang, string Title, long? PageId = null)
{
    public Dictionary<string, object?> ToPathEntry() =>
        new() { ["title"] = Title, ["pageid"] = (object?)PageId };
}

record GameSettings(string Language, PageRef Start, PageRef Destination);
