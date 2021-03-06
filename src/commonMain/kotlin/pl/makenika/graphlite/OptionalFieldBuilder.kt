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

public class OptionalFieldBuilder<S : Schema> internal constructor(private val schema: S) {
    public fun blobField(name: String): Field<S, ByteArray?> {
        return schema.addField(Field.blobOptional(name))
    }

    public fun doubleField(name: String): IndexableScalarField<S, Double?> {
        return schema.addField(Field.doubleOptional(name))
    }

    public fun geoField(name: String): IndexableField<S, GeoBounds?> {
        return schema.addField(Field.geoOptional(name))
    }

    public fun longField(name: String): IndexableScalarField<S, Long?> {
        return schema.addField(Field.longOptional(name))
    }

    public fun textField(name: String, fts: Boolean = true): IndexableScalarField<S, String?> {
        return if (fts) {
            schema.addField(Field.textFtsOptional(name))
        } else {
            schema.addField(Field.textOptional(name))
        }
    }
}
