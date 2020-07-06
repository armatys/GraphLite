package pl.makenika.graphlite

abstract class Schema(val schemaName: String, val schemaVersion: Int) {
    private val fields = mutableMapOf<String, Field<*, *>>()
    private var isFrozen = false

    internal fun <F : Field<S, *>, S : Schema> addField(field: F): F {
        if (isFrozen) error("Schema is already frozen.")
        check(fields.put(field.name, field) == null)
        return field
    }

    internal fun freeze() {
        isFrozen = true
    }

    @Suppress("UNCHECKED_CAST")
    internal fun <S : Schema> getFields(): Map<String, Field<S, Any?>> = fields as Map<String, Field<S, Any?>>

    protected fun <S : Schema> S.optional(): OptionalFieldBuilder<S> {
        return OptionalFieldBuilder(this)
    }

    protected fun <S : Schema> S.blobField(name: String): Field<S, ByteArray> {
        return addField(Field.blob(name))
    }

    protected fun <S : Schema> S.geoField(name: String): IndexableField<S, GeoCoordinates> {
        return addField(Field.geo(name))
    }

    protected fun <S : Schema> S.intField(name: String): IndexableScalarField<S, Int> {
        return addField(Field.int(name))
    }

    protected fun <S : Schema> S.realField(name: String): IndexableScalarField<S, Double> {
        return addField(Field.real(name))
    }

    protected fun <S : Schema> S.textField(name: String): IndexableScalarField<S, String> {
        return addField(Field.text(name))
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Schema) return false

        if (schemaName != other.schemaName) return false
        if (schemaVersion != other.schemaVersion) return false
        if (fields != other.fields) return false

        return true
    }

    override fun hashCode(): Int {
        var result = schemaName.hashCode()
        result = 31 * result + schemaVersion
        result = 31 * result + fields.hashCode()
        return result
    }

    override fun toString(): String {
        return "Schema(name='$schemaName', version=$schemaVersion)"
    }
}

fun <S : Schema> S.fieldMap(builderFn: (S.(FieldMapBuilder<S>) -> Unit)? = null): FieldMap<S> {
    val b = FieldMapBuilder(this)
    builderFn?.invoke(this, b)
    return b.build()
}

operator fun <S : Schema> S.invoke(builderFn: (S.(FieldMapBuilder<S>) -> Unit)? = null): FieldMap<S> =
    fieldMap(builderFn)
