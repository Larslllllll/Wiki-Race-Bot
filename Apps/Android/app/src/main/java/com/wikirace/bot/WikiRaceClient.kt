package com.wikirace.bot

import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject
import org.json.JSONArray

data class SessionInfo(val id: String, val secretToken: String)
data class PageRef(val lang: String, val title: String, val pageid: Long? = null)

class WikiRaceClient {
    private val BASE = "https://wiki-race.com"
    private val JSON = "application/json".toMediaType()

    private val http = OkHttpClient.Builder()
        .addInterceptor { chain ->
            chain.proceed(chain.request().newBuilder()
                .header("User-Agent", "Mozilla/5.0 (Linux; Android 14) Chrome/120.0.0.0 Mobile Safari/537.36")
                .build())
        }
        .build()

    data class JoinResult(val gameId: String, val session: SessionInfo, val playerName: String)

    fun joinGame(gameId: String, playerName: String): JoinResult {
        val body = JSONObject().put("gameId", gameId).put("playerName", playerName)
        val resp = post("/api/game/join", body)
        return JoinResult(
            gameId     = resp.getString("gameId"),
            session    = parseSession(resp),
            playerName = resp.optString("playerName", playerName),
        )
    }

    fun createGame(playerName: String): JoinResult {
        val body = JSONObject().put("playerName", playerName)
        val resp = post("/api/game", body)
        return JoinResult(
            gameId     = resp.getString("gameId"),
            session    = parseSession(resp),
            playerName = playerName,
        )
    }

    fun submitPath(gameId: String, session: SessionInfo, path: List<PageRef>) {
        val pathArr = JSONArray().also { arr ->
            path.forEach { p ->
                arr.put(JSONObject().put("title", p.title).apply {
                    if (p.pageid != null) put("pageid", p.pageid) else put("pageid", JSONObject.NULL)
                })
            }
        }
        post("/api/game/location", JSONObject()
            .put("gameId", gameId)
            .put("session", sessionJson(session))
            .put("path", pathArr))
    }

    fun surrender(gameId: String, session: SessionInfo) =
        post("/api/game/surrender", JSONObject().put("gameId", gameId).put("session", sessionJson(session)))

    fun continueGame(gameId: String, session: SessionInfo) =
        post("/api/game/continue", JSONObject().put("gameId", gameId).put("session", sessionJson(session)))

    fun pusherAuth(gameId: String, session: SessionInfo, socketId: String, channelName: String): JSONObject {
        val formBody = FormBody.Builder()
            .add("socket_id", socketId)
            .add("channel_name", channelName)
            .add("sessionId", session.id)
            .add("secretToken", session.secretToken)
            .add("gameId", gameId)
            .build()
        val req = Request.Builder().url("$BASE/api/game/pusher/auth").post(formBody).build()
        http.newCall(req).execute().use { resp ->
            val bodyStr = resp.body?.string() ?: "{}"
            if (!resp.isSuccessful) throw Exception("Pusher auth failed ${resp.code}: $bodyStr")
            return JSONObject(bodyStr)
        }
    }

    fun fetchSnapshot(gameId: String, session: SessionInfo): JSONObject {
        val url = "$BASE/game?gameId=$gameId&sessionId=${session.id}&secretToken=${session.secretToken}"
        val req = Request.Builder().url(url).header("Accept", "text/html").build()
        http.newCall(req).execute().use { resp ->
            val html = resp.body?.string() ?: ""
            val m = Regex("""<script id="__NEXT_DATA__" type="application/json">(.*?)</script>""",
                RegexOption.DOT_MATCHES_ALL).find(html)
                ?: throw Exception("__NEXT_DATA__ not found")
            val root = JSONObject(m.groupValues[1])
            return root.getJSONObject("props").getJSONObject("pageProps")
        }
    }

    fun waitForState(gameId: String, session: SessionInfo, state: String, timeoutMs: Long = 600_000): JSONObject {
        val deadline = System.currentTimeMillis() + timeoutMs
        var last: String? = null
        while (true) {
            val snap = fetchSnapshot(gameId, session)
            val cur = snap.optJSONObject("game")?.optString("state")
            if (cur != last) { log("[lobby] state=$cur"); last = cur }
            if (cur == state) return snap
            if (System.currentTimeMillis() > deadline) throw Exception("Timeout waiting for $state")
            Thread.sleep(2000)
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private fun post(path: String, body: JSONObject): JSONObject {
        val req = Request.Builder()
            .url("$BASE$path")
            .post(body.toString().toRequestBody(JSON))
            .header("Content-Type", "application/json")
            .build()
        http.newCall(req).execute().use { resp ->
            val str = resp.body?.string() ?: "{}"
            if (!resp.isSuccessful) throw Exception("POST $path → ${resp.code}: $str")
            return JSONObject(str)
        }
    }

    private fun parseSession(resp: JSONObject): SessionInfo {
        val s = resp.getJSONObject("session")
        return SessionInfo(s.getString("id"), s.getString("secretToken"))
    }

    private fun sessionJson(s: SessionInfo) =
        JSONObject().put("id", s.id).put("secretToken", s.secretToken)

    // Overridable for UI logging
    open fun log(msg: String) = println(msg)
}
