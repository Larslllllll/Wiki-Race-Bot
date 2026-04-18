package com.wikirace.bot

import android.os.Bundle
import android.widget.ScrollView
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.wikirace.bot.databinding.ActivityMainBinding
import kotlinx.coroutines.*
import org.json.JSONObject

class MainActivity : AppCompatActivity() {

    private lateinit var b: ActivityMainBinding
    private var botJob: Job? = null
    private var pusher: PusherPresence? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        b = ActivityMainBinding.inflate(layoutInflater)
        setContentView(b.root)

        b.btnPlay.setOnClickListener { startBot() }
        b.btnStop.setOnClickListener { stopBot() }
    }

    private fun log(msg: String) = runOnUiThread {
        b.tvLog.append("$msg\n")
        (b.tvLog.parent as? ScrollView)?.fullScroll(ScrollView.FOCUS_DOWN)
    }

    private fun startBot() {
        val name      = b.etName.text.toString().ifBlank { "WikiBot" }
        val lobbyCode = b.etLobbyCode.text.toString().trim().uppercase()
        val stay      = b.cbStay.isChecked

        b.btnPlay.isEnabled = false
        b.btnStop.isEnabled = true
        b.tvLog.text = ""

        botJob = lifecycleScope.launch(Dispatchers.IO) {
            runBot(name, lobbyCode, stay)
            runOnUiThread { b.btnPlay.isEnabled = true; b.btnStop.isEnabled = false }
        }
    }

    private fun stopBot() {
        botJob?.cancel()
        pusher?.stop()
        b.btnPlay.isEnabled = true
        b.btnStop.isEnabled = false
        log("[bot] stopped")
    }

    private suspend fun runBot(name: String, lobbyCode: String, stay: Boolean) {
        val client    = WikiRaceClient()
        val navigator = WikiNavigator(::log)

        // Join / create
        val (gameId, session, playerName) = try {
            if (lobbyCode.length == 5) {
                val r = client.joinGame(lobbyCode, name)
                log("[lobby] joined  id=${r.gameId}  as '${r.playerName}'")
                r
            } else {
                val r = client.createGame(name)
                log("[lobby] created id=${r.gameId}")
                log("[lobby] share: https://wiki-race.com/?lobbyCode=${r.gameId}")
                r
            }
        } catch (e: Exception) {
            log("[error] ${e.message}")
            return
        }

        // Pusher presence
        pusher = PusherPresence(client, gameId, session, ::log)
        pusher!!.start()
        delay(1500)

        var roundsPlayed = 0

        while (isActive) {
            log("[lobby] waiting for game to start …")
            val snap = try {
                withContext(Dispatchers.IO) { client.waitForState(gameId, session, "in_progress") }
            } catch (e: Exception) { log("[error] ${e.message}"); break }

            val settings = snap.getJSONObject("game").getJSONObject("settings")
            val startTitle = settings.getJSONObject("start").getString("title")
            val destTitle  = settings.getJSONObject("destination").getString("title")
            val lang       = settings.optString("language", "en")
            log("[game] '$startTitle' → '$destTitle'  (lang=$lang)")

            // Presence ping
            try { client.submitPath(gameId, session, listOf(PageRef(lang, startTitle))) } catch (_: Exception) {}

            // Navigate
            val t0 = System.currentTimeMillis()
            val path = try {
                withContext(Dispatchers.IO) { navigator.findPath(lang, startTitle, destTitle) }
            } catch (e: Exception) {
                log("[error] nav failed: ${e.message}")
                try { client.surrender(gameId, session) } catch (_: Exception) {}
                if (!stay) break else continue
            }
            val dt = (System.currentTimeMillis() - t0) / 1000.0

            val route = path.joinToString(" → ") { it.title }
            log("[bot] ${path.size - 1} hops in ${String.format("%.1f", dt)}s: $route")

            try {
                client.submitPath(gameId, session, path)
                log("[result] submitted ${path.size - 1} hop(s)")
            } catch (e: Exception) {
                log("[warn] submit failed: ${e.message}")
            }

            roundsPlayed++
            if (!stay) break

            log("[lobby] staying — waiting for next round …")
            try { client.continueGame(gameId, session) } catch (_: Exception) {}
        }
    }
}
