package pl.makenika.graphlite.sql

import pl.makenika.graphlite.Cleanable

expect class SqliteCursorFacade : Cleanable {
    fun findBlob(columnName: String): ByteArray?
    fun findDouble(columnName: String): Double?
    fun findInt(columnName: String): Int?
    fun findString(columnName: String): String?

    fun getBlob(columnName: String): ByteArray
    fun getDouble(columnName: String): Double
    fun getInt(columnName: String): Int
    fun getString(columnName: String): String

    fun moveToNext(): Boolean
}
