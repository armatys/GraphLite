/*
 * Copyright (c) 2020 Mateusz Armatys
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
