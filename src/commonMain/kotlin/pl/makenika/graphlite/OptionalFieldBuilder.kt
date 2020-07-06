package pl.makenika.graphlite

class OptionalFieldBuilder<S : Schema> internal constructor(private val schema: S) {
    fun blobField(name: String): Field<S, ByteArray?> {
        return schema.addField(Field.blobOptional(name))
    }

    fun geoField(name: String): IndexableField<S, GeoCoordinates?> {
        return schema.addField(Field.geoOptional(name))
    }

    fun intField(name: String): IndexableScalarField<S, Int?> {
        return schema.addField(Field.intOptional(name))
    }

    fun realField(name: String): IndexableScalarField<S, Double?> {
        return schema.addField(Field.realOptional(name))
    }

    fun textField(name: String): IndexableScalarField<S, String?> {
        return schema.addField(Field.textOptional(name))
    }
}
