package pl.makenika.graphlite.impl

import pl.makenika.graphlite.*

internal fun isFieldTypeNullable(type: String): Boolean {
    return type.endsWith("?")
}

internal fun baseFieldTypeName(type: String): String {
    return type.replace("?", "")
}

internal data class FieldImpl<S : Schema, T>(override val name: String, override val type: String) : Field<S, T>

internal data class IndexableFieldImpl<S : Schema, T>(override val name: String, override val type: String) :
    IndexableField<S, T>

internal data class IndexableScalarFieldImpl<S : Schema, T>(
    override val name: String,
    override val type: String
) : IndexableScalarField<S, T>
