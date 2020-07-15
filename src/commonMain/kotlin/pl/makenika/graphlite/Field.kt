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

import pl.makenika.graphlite.impl.FieldImpl
import pl.makenika.graphlite.impl.IndexableFieldImpl
import pl.makenika.graphlite.impl.IndexableScalarFieldImpl

private enum class BaseFieldCode(val value: String) {
    Blb("blb"),
    Dbl("dbl"),
    Geo("geo"),
    Lng("lng"),
    Txt("txt")
}

sealed class FieldType(private val baseFieldCode: BaseFieldCode, val optional: Boolean) {
    class Blob(optional: Boolean) : FieldType(BaseFieldCode.Blb, optional)
    class DoubleFloat(optional: Boolean) : FieldType(BaseFieldCode.Dbl, optional)
    class Geo(optional: Boolean) : FieldType(BaseFieldCode.Geo, optional)
    class LongInt(optional: Boolean) : FieldType(BaseFieldCode.Lng, optional)
    class Text(optional: Boolean) : FieldType(BaseFieldCode.Txt, optional)

    val code: String
        get() = baseFieldCode.value + if (optional) "?" else ""

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null) return false
        if (other !is FieldType) return false

        if (baseFieldCode != other.baseFieldCode) return false
        if (optional != other.optional) return false

        return true
    }

    override fun hashCode(): Int {
        var result = baseFieldCode.hashCode()
        result = 31 * result + optional.hashCode()
        return result
    }

    override fun toString(): String {
        return "FieldType(code=$code)"
    }

    companion object {
        fun fromCode(code: String): FieldType {
            val optional = code.endsWith('?')
            val baseCode = code.removeSuffix("?")
            val fieldCode = BaseFieldCode.values().find { it.value == baseCode }
                ?: error("Unknown field value code: $baseCode")
            return when (fieldCode) {
                BaseFieldCode.Blb -> Blob(optional)
                BaseFieldCode.Dbl -> DoubleFloat(optional)
                BaseFieldCode.Geo -> Geo(optional)
                BaseFieldCode.Lng -> LongInt(optional)
                BaseFieldCode.Txt -> Text(optional)
            }
        }
    }
}

interface Field<in S : Schema, T> {
    val handle: FieldHandle
    val type: FieldType

    companion object {
        internal fun <S : Schema> blob(name: String): Field<S, ByteArray> =
            make(name, FieldType.Blob(false))

        internal fun <S : Schema> blobOptional(name: String): Field<S, ByteArray?> =
            make(name, FieldType.Blob(true))

        internal fun <S : Schema> geo(name: String): IndexableField<S, GeoBounds> =
            makeIndexed(name, FieldType.Geo(false))

        internal fun <S : Schema> geoOptional(name: String): IndexableField<S, GeoBounds?> =
            makeIndexed(name, FieldType.Geo(true))

        internal fun <S : Schema> long(name: String): IndexableScalarField<S, Long> =
            makeIndexedScalar(name, FieldType.LongInt(false))

        internal fun <S : Schema> longOptional(name: String): IndexableScalarField<S, Long?> =
            makeIndexedScalar(name, FieldType.LongInt(true))

        internal fun <S : Schema> double(name: String): IndexableScalarField<S, Double> =
            makeIndexedScalar(name, FieldType.DoubleFloat(false))

        internal fun <S : Schema> doubleOptional(name: String): IndexableScalarField<S, Double?> =
            makeIndexedScalar(name, FieldType.DoubleFloat(true))

        internal fun <S : Schema> text(name: String): IndexableScalarField<S, String> =
            makeIndexedScalar(name, FieldType.Text(false))

        internal fun <S : Schema> textOptional(name: String): IndexableScalarField<S, String?> =
            makeIndexedScalar(name, FieldType.Text(true))

        private fun <S : Schema, T> make(
            name: String,
            type: FieldType
        ): Field<S, T> =
            FieldImpl(FieldHandle(name), type)

        private fun <S : Schema, T> makeIndexed(
            name: String,
            type: FieldType
        ): IndexableField<S, T> = IndexableFieldImpl(FieldHandle(name), type)

        private fun <S : Schema, T> makeIndexedScalar(
            name: String,
            type: FieldType
        ): IndexableScalarField<S, T> = IndexableScalarFieldImpl(FieldHandle(name), type)
    }
}

interface IndexableField<in S : Schema, T> : Field<S, T>

interface IndexableScalarField<in S : Schema, T> : IndexableField<S, T>
