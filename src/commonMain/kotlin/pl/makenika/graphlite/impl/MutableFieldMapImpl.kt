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

package pl.makenika.graphlite.impl

import pl.makenika.graphlite.Field
import pl.makenika.graphlite.FieldMap
import pl.makenika.graphlite.MutableFieldMap
import pl.makenika.graphlite.Schema

internal data class MutableFieldMapImpl<S : Schema>(
    private val fieldValueMap: MutableMap<Field<S, *>, Any?>,
    private val schema: S
) : MutableFieldMap<S> {
    constructor(schema: S) : this(mutableMapOf<Field<S, *>, Any?>(), schema)

    override fun contains(field: Field<S, *>): Boolean {
        return fieldValueMap.contains(field)
    }

    override fun edit(fn: S.(MutableFieldMap<S>) -> Unit): FieldMap<S> {
        val mutableFieldMap = toMutableFieldMap()
        fn(schema, mutableFieldMap)
        return mutableFieldMap
    }

    override fun <F : Field<S, T>, T> get(field: F): T {
        @Suppress("UNCHECKED_CAST")
        return fieldValueMap[field] as T
    }

    override fun <F : Field<S, T>, T> invoke(field: S.() -> F): T {
        return get(field(schema))
    }

    override fun schema(): S {
        return schema
    }

    override fun <T> set(field: Field<S, T>, value: T) {
        fieldValueMap[field] = value as Any?
    }

    override fun toMap(): Map<String, Any?> {
        return fieldValueMap.mapKeys { it.key.handle.value }
    }

    override fun toMutableFieldMap(): MutableFieldMap<S> {
        return MutableFieldMapImpl(fieldValueMap, schema)
    }
}
