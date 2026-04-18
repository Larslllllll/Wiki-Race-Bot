package com.wikirace.bot

import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject

/**
 * Wikipedia-API-based navigation for Android (no local graph.db).
 *
 * Strategy:
 *  1. Pre-fetch backlinks of destination (pages that link TO it).
 *  2. Each hop:
 *     a. Destination directly on current page? → done.
 *     b. Any link on page is a known backlink? → go there.
 *     c. Otherwise pick the link whose title shares the most tokens with destination.
 */
class WikiNavigator(private val onLog: (String) -> Unit) {

    private val http = OkHttpClient()

    fun findPath(
        lang: String,
        startTitle: String,
        destTitle: String,
        maxHops: Int = 30,
    ): List<PageRef> {
        // Pre-fetch backlinks
        val backlinks = fetchBacklinks(lang, destTitle, 500)
        onLog("[nav] ${backlinks.size} backlinks of '$destTitle'")

        val path    = mutableListOf(PageRef(lang, startTitle))
        val visited = mutableSetOf(startTitle.lowercase())

        repeat(maxHops) {
            val current = path.last()
            if (current.title.equals(destTitle, ignoreCase = true)) return path

            val links = fetchLinks(lang, current.title)
            val candidates = links.filter { it.title.lowercase() !in visited }
                .ifEmpty { links } // allow revisit if all visited

            // a. Direct link to destination?
            candidates.firstOrNull { it.title.equals(destTitle, ignoreCase = true) }?.let {
                path.add(it); return path
            }

            // b. Known backlink?
            val blHit = candidates.filter { it.title in backlinks }
            if (blHit.isNotEmpty()) {
                val best = blHit.maxByOrNull { tokenOverlap(it.title, destTitle) }!!
                onLog("[nav] backlink hit → '${best.title}'")
                path.add(best)
                visited.add(best.title.lowercase())
                return@repeat
            }

            // c. Best token overlap
            val best = candidates.maxByOrNull { tokenOverlap(it.title, destTitle) }!!
            path.add(best)
            visited.add(best.title.lowercase())
        }
        return path
    }

    private fun fetchLinks(lang: String, title: String): List<PageRef> {
        val url = "https://$lang.wikipedia.org/w/api.php" +
            "?action=query&prop=links&titles=${encode(title)}" +
            "&pllimit=500&format=json&redirects=1"
        val json = getJson(url) ?: return emptyList()
        val pages = json.getJSONObject("query").getJSONObject("pages")
        val page  = pages.keys().next().let { pages.getJSONObject(it) }
        val arr   = page.optJSONArray("links") ?: return emptyList()
        return (0 until arr.length()).map { i ->
            val o = arr.getJSONObject(i)
            PageRef(lang, o.getString("title"))
        }.filter { it.title.startsWith("Wikipedia:").not() && it.title.startsWith("Template:").not() }
    }

    private fun fetchBacklinks(lang: String, title: String, limit: Int): Set<String> {
        val url = "https://$lang.wikipedia.org/w/api.php" +
            "?action=query&list=backlinks&bltitle=${encode(title)}" +
            "&bllimit=$limit&blnamespace=0&format=json"
        val json = getJson(url) ?: return emptySet()
        val arr  = json.getJSONObject("query").getJSONArray("backlinks")
        return (0 until arr.length()).map { arr.getJSONObject(it).getString("title") }.toSet()
    }

    private fun getJson(url: String): JSONObject? {
        return try {
            val req = Request.Builder().url(url)
                .header("User-Agent", "WikiRaceBot/1.0 (Android)").build()
            http.newCall(req).execute().use { resp ->
                resp.body?.string()?.let { JSONObject(it) }
            }
        } catch (e: Exception) { null }
    }

    private fun encode(s: String) = java.net.URLEncoder.encode(s, "UTF-8")

    private fun tokenOverlap(a: String, b: String): Int {
        val ta = a.lowercase().split(Regex("\\W+")).toSet()
        val tb = b.lowercase().split(Regex("\\W+")).toSet()
        return (ta intersect tb).size
    }
}
