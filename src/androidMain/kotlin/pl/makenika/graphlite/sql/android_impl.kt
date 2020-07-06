package pl.makenika.graphlite.sql

import android.content.ContentValues
import android.database.Cursor
import pl.makenika.graphlite.Cleanable

actual typealias SqlContentValues = ContentValues

actual class SqliteCursorFacade(private val cursor: Cursor) : Cleanable {
    actual fun findBlob(columnName: String): ByteArray? {
        return getValue(columnName, cursor::getBlob)
    }

    actual fun findDouble(columnName: String): Double? {
        return getValue(columnName, cursor::getDouble)
    }

    actual fun findInt(columnName: String): Int? {
        return getValue(columnName, cursor::getInt)
    }

    actual fun findString(columnName: String): String? {
        return getValue(columnName, cursor::getString)
    }

    actual fun getBlob(columnName: String): ByteArray {
        return findBlob(columnName) ?: error("Column $columnName is null.")
    }

    actual fun getDouble(columnName: String): Double {
        return findDouble(columnName) ?: error("Column $columnName is null.")
    }

    actual fun getInt(columnName: String): Int {
        return findInt(columnName) ?: error("Column $columnName is null.")
    }

    actual fun getString(columnName: String): String {
        return findString(columnName) ?: error("Column $columnName is null.")
    }

    actual fun moveToNext(): Boolean {
        return cursor.moveToNext()
    }

    override fun close() {
        cursor.close()
    }

    private fun <T : Any> getValue(columnName: String, fn: (Int) -> T): T? {
        val index = cursor.getColumnIndexOrThrow(columnName)
        return if (cursor.isNull(index)) {
            null
        } else {
            fn(index)
        }
    }
}
