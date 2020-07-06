package pl.makenika.graphlite

import pl.makenika.graphlite.impl.MutableFieldMapImpl
import pl.makenika.graphlite.impl.isFieldTypeNullable

interface FieldMap<S : Schema> {
    fun contains(field: Field<S, *>): Boolean
    fun edit(fn: S.(MutableFieldMap<S>) -> Unit): FieldMap<S>
    operator fun <T> get(field: Field<S, T>): T
    operator fun <T> invoke(field: S.() -> Field<S, T>): T
    fun schema(): S
    fun toMap(): Map<String, Any?>
    fun toMutableFieldMap(): MutableFieldMap<S>
}

interface MutableFieldMap<S : Schema> : FieldMap<S> {
    operator fun <T> set(field: Field<S, T>, value: T)
}

class FieldMapBuilder<S : Schema>(private val schema: S) {
    private val fieldMap = MutableFieldMapImpl(schema)

    operator fun <T> set(field: Field<S, T>, value: T): FieldMapBuilder<S> {
        fieldMap[field] = value
        return this
    }

    fun build(): FieldMap<S> {
        for ((_, field) in schema.getFields<S>()) {
            if (!fieldMap.contains(field)) {
                if (isFieldTypeNullable(field.type)) {
                    fieldMap[field] = null
                } else {
                    error("Cannot create FieldMap: field ${field.name} is missing.")
                }
            }
        }
        return fieldMap
    }
}
