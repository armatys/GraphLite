package pl.makenika.graphlite.sql

sealed class SqlValue {
    data class SqlBlob(val value: ByteArray) : SqlValue() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as SqlBlob
            if (!value.contentEquals(other.value)) return false
            return true
        }

        override fun hashCode(): Int {
            return value.contentHashCode()
        }
    }

    data class SqlInt(val value: Int) : SqlValue()
    object SqlNull : SqlValue()
    data class SqlReal(val value: Double) : SqlValue()
    data class SqlString(val value: String) : SqlValue()
}

actual class SqlContentValues actual constructor() {
    private val keys = mutableSetOf<String>()
    private val byteArrays = mutableMapOf<String, ByteArray>()
    private val doubles = mutableMapOf<String, Double>()
    private val ints = mutableMapOf<String, Int>()
    private val strings = mutableMapOf<String, String>()

    actual fun put(key: String, value: ByteArray?) {
        if (value != null) {
            byteArrays[key] = value
        }
        keys.add(key)
    }

    actual fun put(key: String, value: Double?) {
        if (value != null) {
            doubles[key] = value
        }
        keys.add(key)
    }

    actual fun put(key: String, value: Int?) {
        if (value != null) {
            ints[key] = value
        }
        keys.add(key)
    }

    actual fun put(key: String, value: String?) {
        if (value != null) {
            strings[key] = value
        }
        keys.add(key)
    }

    fun keySet(): Set<String> {
        return keys
    }

    fun get(key: String): SqlValue {
        byteArrays[key]?.let { return SqlValue.SqlBlob(it) }
        doubles[key]?.let { return SqlValue.SqlReal(it) }
        ints[key]?.let { return SqlValue.SqlInt(it) }
        strings[key]?.let { return SqlValue.SqlString(it) }
        return SqlValue.SqlNull
    }

    override fun toString(): String {
        return keySet().joinToString(", ") { "$it=${get(it)}" }
    }
}