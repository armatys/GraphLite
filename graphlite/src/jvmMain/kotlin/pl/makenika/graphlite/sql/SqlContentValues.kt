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

internal sealed class SqlValue {
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

    data class SqlLong(val value: Long) : SqlValue()
    object SqlNull : SqlValue()
    data class SqlReal(val value: Double) : SqlValue()
    data class SqlString(val value: String) : SqlValue()
}

public actual class SqlContentValues actual constructor() {
    private val keys = mutableSetOf<String>()
    private val byteArrays = mutableMapOf<String, ByteArray>()
    private val doubles = mutableMapOf<String, Double>()
    private val longs = mutableMapOf<String, Long>()
    private val strings = mutableMapOf<String, String>()

    internal actual fun put(key: String, value: ByteArray?) {
        if (value != null) {
            byteArrays[key] = value
        }
        keys.add(key)
    }

    internal actual fun put(key: String, value: Double?) {
        if (value != null) {
            doubles[key] = value
        }
        keys.add(key)
    }

    internal actual fun put(key: String, value: Long?) {
        if (value != null) {
            longs[key] = value
        }
        keys.add(key)
    }

    internal actual fun put(key: String, value: String?) {
        if (value != null) {
            strings[key] = value
        }
        keys.add(key)
    }

    internal fun keySet(): Set<String> {
        return keys
    }

    internal fun get(key: String): SqlValue {
        byteArrays[key]?.let { return SqlValue.SqlBlob(it) }
        doubles[key]?.let { return SqlValue.SqlReal(it) }
        longs[key]?.let { return SqlValue.SqlLong(it) }
        strings[key]?.let { return SqlValue.SqlString(it) }
        return SqlValue.SqlNull
    }

    override fun toString(): String {
        return keySet().joinToString(", ") { "$it=${get(it)}" }
    }
}
