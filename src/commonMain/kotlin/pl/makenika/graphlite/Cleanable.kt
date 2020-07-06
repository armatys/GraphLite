package pl.makenika.graphlite

expect interface Cleanable {
    fun close()
}

inline fun <T : Cleanable?, R> T.use(block: (T) -> R): R {
    return try {
        block(this)
    } finally {
        this?.close()
    }
}
