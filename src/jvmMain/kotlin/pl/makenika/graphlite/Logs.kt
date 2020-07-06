package pl.makenika.graphlite

import java.util.logging.Level
import java.util.logging.Logger

actual object Logs {
    private val logger = Logger.getLogger("GraphLite")

    actual fun d(tag: String?, msg: String) {
        val t = tag ?: ""
        logger.log(Level.FINE, "$t $msg")
    }
}
