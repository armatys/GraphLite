package pl.makenika.graphlite.sql

expect class SqlContentValues() {
    fun put(key: String, value: ByteArray?)
    fun put(key: String, value: Double?)
    fun put(key: String, value: Int?)
    fun put(key: String, value: String?)
}
