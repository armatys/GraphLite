package pl.makenika.graphlite

import android.util.Log

actual object Logs {
    actual fun d(tag: String?, msg: String) {
        Log.d(tag, msg)
    }
}
