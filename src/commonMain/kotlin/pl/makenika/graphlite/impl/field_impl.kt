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
