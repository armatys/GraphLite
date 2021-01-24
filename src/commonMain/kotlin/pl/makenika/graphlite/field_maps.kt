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

package pl.makenika.graphlite

import pl.makenika.graphlite.impl.MutableFieldMapImpl

public interface FieldMap<S : Schema> {
    public fun contains(field: Field<S, *>): Boolean
    public fun edit(fn: S.(MutableFieldMap<S>) -> Unit): FieldMap<S>
    public operator fun <F : Field<S, T>, T> get(field: F): T
    public operator fun <F : Field<S, T>, T> invoke(field: S.() -> F): T
    public fun schema(): S
    public fun toMap(): Map<String, Any?>
    public fun toMutableFieldMap(): MutableFieldMap<S>
}

public interface MutableFieldMap<S : Schema> : FieldMap<S> {
    public operator fun <T> set(field: Field<S, T>, value: T)
}

public class FieldMapBuilder<S : Schema>(private val schema: S) {
    private val fieldMap = MutableFieldMapImpl(schema)

    public operator fun <T> set(field: Field<S, T>, value: T): FieldMapBuilder<S> {
        fieldMap[field] = value
        return this
    }

    public fun build(): FieldMap<S> {
        for ((_, field) in schema.getFields<S>()) {
            if (!fieldMap.contains(field)) {
                if (field.type.optional) {
                    fieldMap[field] = null
                } else {
                    error("Cannot create FieldMap: field ${field.handle} is missing.")
                }
            }
        }
        return fieldMap
    }
}
