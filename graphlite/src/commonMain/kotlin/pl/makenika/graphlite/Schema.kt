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

internal typealias FieldValidator<S, T> = FieldMap<out S>.(T) -> Boolean

public abstract class Schema(
    public val schemaHandle: SchemaHandle,
    public val schemaVersion: Long
) {
    public constructor(handleValue: String, version: Long) : this(
        SchemaHandle(handleValue),
        version
    )

    private val fields = mutableMapOf<FieldHandle, Field<*, *>>()
    private val fieldValidators = mutableMapOf<FieldHandle, FieldValidator<*, *>>()
    private var isFrozen = false

    internal fun <F : Field<S, *>, S : Schema> addField(field: F): F {
        if (isFrozen) error("Schema is already frozen.")
        check(fields.put(field.handle, field) == null) {
            "Field with name \"${field.handle.value}\" has been already added."
        }
        return field
    }

    internal fun freeze() {
        isFrozen = true
    }

    @Suppress("UNCHECKED_CAST")
    internal fun <S : Schema> getFields(): Map<String, Field<S, Any?>> =
        fields as Map<String, Field<S, Any?>>

    @Suppress("UNCHECKED_CAST")
    internal fun <S : Schema, T> getValidator(field: Field<S, T>): FieldValidator<S, T>? {
        return fieldValidators[field.handle] as FieldValidator<S, T>?
    }

    protected fun <S : Schema> S.optional(): OptionalFieldBuilder<S> {
        return OptionalFieldBuilder(this)
    }

    protected fun <S : Schema> S.blobField(name: String): Field<S, ByteArray> {
        return addField(Field.blob(name))
    }

    protected fun <S : Schema> S.doubleField(name: String): IndexableScalarField<S, Double> {
        return addField(Field.double(name))
    }

    protected fun <S : Schema> S.geoField(name: String): IndexableField<S, GeoBounds> {
        return addField(Field.geo(name))
    }

    protected fun <S : Schema> S.longField(name: String): IndexableScalarField<S, Long> {
        return addField(Field.long(name))
    }

    protected fun <S : Schema> S.textField(
        name: String,
        fts: Boolean = true
    ): IndexableScalarField<S, String> {
        return if (fts) {
            addField(Field.textFts(name))
        } else {
            addField(Field.text(name))
        }
    }

    protected fun <F : Field<S, T>, S : Schema, T> F.onValidate(validator: FieldValidator<S, T>): F {
        @Suppress("UNCHECKED_CAST")
        check(fieldValidators.put(handle, validator as FieldValidator<*, *>) == null) {
            "Validator for field \"${handle.value}\" has been already added."
        }
        return this
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null) return false
        if (other !is Schema) return false

        if (schemaHandle != other.schemaHandle) return false
        if (schemaVersion != other.schemaVersion) return false
        if (fields != other.fields) return false

        return true
    }

    override fun hashCode(): Int {
        var result = schemaHandle.hashCode()
        result = 31 * result + schemaVersion.hashCode()
        result = 31 * result + fields.hashCode()
        return result
    }

    override fun toString(): String {
        return "Schema(handle='$schemaHandle', version=$schemaVersion)"
    }
}

public fun <S : Schema> S.fieldMap(builderFn: (S.(FieldMapBuilder<S>) -> Unit)? = null): FieldMap<S> {
    val b = FieldMapBuilder(this)
    builderFn?.invoke(this, b)
    return b.build()
}

public operator fun <S : Schema> S.invoke(builderFn: (S.(FieldMapBuilder<S>) -> Unit)? = null): FieldMap<S> =
    fieldMap(builderFn)

internal class SchemaWithId(
    val schemaId: String,
    schemaHandle: SchemaHandle,
    schemaVersion: Long
) : Schema(schemaHandle, schemaVersion)
