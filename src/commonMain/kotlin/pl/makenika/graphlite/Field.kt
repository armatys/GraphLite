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

interface Field<in S : Schema, T> {
    val name: String
    val type: String

    companion object {
        internal const val FIELD_TYPE_BLOB = "blb"
        internal const val FIELD_TYPE_GEO = "geo"
        internal const val FIELD_TYPE_LONG_INT = "lng"
        internal const val FIELD_TYPE_DOUBLE_FLOAT = "dbl"
        internal const val FIELD_TYPE_TEXT = "txt"

        internal fun <S : Schema> blob(name: String): Field<S, ByteArray> =
            make(name, FIELD_TYPE_BLOB)

        internal fun <S : Schema> blobOptional(name: String): Field<S, ByteArray?> =
            makeOpt(name, FIELD_TYPE_BLOB)

        internal fun <S : Schema> geo(name: String): IndexableField<S, GeoBounds> =
            makeIndexed(name, FIELD_TYPE_GEO)

        internal fun <S : Schema> geoOptional(name: String): IndexableField<S, GeoBounds?> =
            makeIndexedOpt(name, FIELD_TYPE_GEO)

        internal fun <S : Schema> long(name: String): IndexableScalarField<S, Long> =
            makeIndexedScalar(name, FIELD_TYPE_LONG_INT)

        internal fun <S : Schema> longOptional(name: String): IndexableScalarField<S, Long?> =
            makeIndexedScalarOpt(name, FIELD_TYPE_LONG_INT)

        internal fun <S : Schema> double(name: String): IndexableScalarField<S, Double> =
            makeIndexedScalar(name, FIELD_TYPE_DOUBLE_FLOAT)

        internal fun <S : Schema> doubleOptional(name: String): IndexableScalarField<S, Double?> =
            makeIndexedScalarOpt(name, FIELD_TYPE_DOUBLE_FLOAT)

        internal fun <S : Schema> text(name: String): IndexableScalarField<S, String> =
            makeIndexedScalar(name, FIELD_TYPE_TEXT)

        internal fun <S : Schema> textOptional(name: String): IndexableScalarField<S, String?> =
            makeIndexedScalarOpt(name, FIELD_TYPE_TEXT)

        private fun <S : Schema, T> make(name: String, type: String): Field<S, T> =
            FieldImpl(name, type)

        private fun <S : Schema, T> makeOpt(name: String, type: String): Field<S, T?> =
            FieldImpl(name, "$type?")

        private fun <S : Schema, T> makeIndexed(name: String, type: String): IndexableField<S, T> =
            IndexableFieldImpl(name, type)

        private fun <S : Schema, T> makeIndexedOpt(
            name: String,
            type: String
        ): IndexableField<S, T?> =
            IndexableFieldImpl(name, "$type?")

        private fun <S : Schema, T> makeIndexedScalar(
            name: String,
            type: String
        ): IndexableScalarField<S, T> =
            IndexableScalarFieldImpl(name, type)

        private fun <S : Schema, T> makeIndexedScalarOpt(
            name: String,
            type: String
        ): IndexableScalarField<S, T?> =
            IndexableScalarFieldImpl(name, "$type?")
    }
}

interface IndexableField<in S : Schema, T> : Field<S, T>

interface IndexableScalarField<in S : Schema, T> : IndexableField<S, T>
