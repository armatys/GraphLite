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

    override fun <T> get(field: Field<S, T>): T {
        @Suppress("UNCHECKED_CAST")
        return fieldValueMap[field] as T
    }

    override fun <T> invoke(field: S.() -> Field<S, T>): T {
        return get(field(schema))
    }

    override fun schema(): S {
        return schema
    }

    override fun <T> set(field: Field<S, T>, value: T) {
        fieldValueMap[field] = value as Any?
    }

    override fun toMap(): Map<String, Any?> {
        return fieldValueMap.mapKeys { it.key.name }
    }

    override fun toMutableFieldMap(): MutableFieldMap<S> {
        return MutableFieldMapImpl(fieldValueMap, schema)
    }
}
