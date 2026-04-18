package com.wikirace.bot

import okhttp3.*
import org.json.JSONObject
import java.util.concurrent.atomic.AtomicBoolean

class PusherPresence(
    private val client: WikiRaceClient,
    private val gameId: String,
    private val session: SessionInfo,
    private val onLog: (String) -> Unit,
) {
    private val KEY  = "932edcd098e03d77349f"
    private val URL  = "wss://ws.wiki-race.com/app/$KEY?protocol=7&client=android-bot&version=7.0.3&flash=false"

    private val stopped = AtomicBoolean(false)
    private var ws: WebSocket? = null

    private val http = OkHttpClient()

    fun start() = Thread(::runLoop, "pusher").also { it.isDaemon = true; it.start() }
    fun stop()  { stopped.set(true); ws?.close(1000, null) }

    private fun runLoop() {
        while (!stopped.get()) {
            try { connect() } catch (e: Exception) { onLog("[pusher] error: ${e.message}") }
            if (!stopped.get()) Thread.sleep(5000)
        }
    }

    private fun connect() {
        val latch = java.util.concurrent.CountDownLatch(1)
        var socketId: String? = null

        val listener = object : WebSocketListener() {
            override fun onOpen(ws: WebSocket, resp: Response) {}

            override fun onMessage(ws: WebSocket, text: String) {
                val msg = JSONObject(text)
                when (msg.optString("event")) {
                    "pusher:connection_established" -> {
                        val data = JSONObject(msg.getString("data"))
                        socketId = data.getString("socket_id")
                        onLog("[pusher] connected  socket_id=$socketId")

                        // Auth + subscribe on a background thread
                        Thread {
                            try {
                                val channel  = "presence-game-$gameId"
                                val authData = client.pusherAuth(gameId, session, socketId!!, channel)
                                ws.send(JSONObject()
                                    .put("event", "pusher:subscribe")
                                    .put("data", JSONObject()
                                        .put("auth", authData.optString("auth"))
                                        .put("channel_data", authData.optString("channel_data"))
                                        .put("channel", channel))
                                    .toString())
                            } catch (e: Exception) {
                                onLog("[pusher] auth error: ${e.message}")
                            }
                        }.start()
                    }
                    "pusher:subscription_succeeded" ->
                        onLog("[pusher] subscribed — bot now visible in lobby")
                    "pusher:error" ->
                        onLog("[pusher] error: $msg")
                    "pusher:ping" ->
                        ws.send(JSONObject().put("event", "pusher:pong").put("data", JSONObject()).toString())
                }
            }

            override fun onClosing(ws: WebSocket, code: Int, reason: String) { latch.countDown() }
            override fun onFailure(ws: WebSocket, t: Throwable, r: Response?) {
                onLog("[pusher] failure: ${t.message}")
                latch.countDown()
            }
        }

        val req = Request.Builder().url(URL).build()
        ws = http.newWebSocket(req, listener)
        latch.await()
    }
}
