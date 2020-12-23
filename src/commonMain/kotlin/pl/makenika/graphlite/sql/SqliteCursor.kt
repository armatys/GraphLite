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

internal expect class SqliteCursorFacade : Cleanable {
    fun findBlob(columnName: String): ByteArray?
    fun findDouble(columnName: String): Double?
    fun findLong(columnName: String): Long?
    fun findString(columnName: String): String?

    fun moveToNext(): Boolean
}

internal fun SqliteCursorFacade.findBoolean(columnName: String): Boolean? {
    return findLong(columnName)?.let { it == 1L }
}

internal fun SqliteCursorFacade.getBoolean(columnName: String): Boolean {
    return findBoolean(columnName) ?: error("Column $columnName is null.")
}

internal fun SqliteCursorFacade.getBlob(columnName: String): ByteArray {
    return findBlob(columnName) ?: error("Column $columnName is null.")
}

internal fun SqliteCursorFacade.getDouble(columnName: String): Double {
    return findDouble(columnName) ?: error("Column $columnName is null.")
}

internal fun SqliteCursorFacade.getLong(columnName: String): Long {
    return findLong(columnName) ?: error("Column $columnName is null.")
}

internal fun SqliteCursorFacade.getString(columnName: String): String {
    return findString(columnName) ?: error("Column $columnName is null.")
}
