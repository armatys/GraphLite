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

import pl.makenika.graphlite.Cleanable
import java.sql.ResultSet

actual class SqliteCursorFacade(private val resultSet: ResultSet) : Cleanable {
    actual fun findBlob(columnName: String): ByteArray? {
        return resultSet.getBytes(columnName)
    }

    actual fun findDouble(columnName: String): Double? {
        val value = resultSet.getDouble(columnName)
        return if (resultSet.wasNull()) {
            null
        } else {
            value
        }
    }

    actual fun findInt(columnName: String): Int? {
        val value = resultSet.getInt(columnName)
        return if (resultSet.wasNull()) {
            null
        } else {
            value
        }
    }

    actual fun findString(columnName: String): String? {
        return resultSet.getString(columnName)
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
        return resultSet.next()
    }

    override fun close() {
        resultSet.close()
    }
}
