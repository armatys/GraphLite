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
import pl.makenika.graphlite.sql.*

internal object GraphSqlUtils {
    fun getElementId(driver: SqliteDriver, elementName: String): ElementId? {
        val (id, type) = driver.query(
            "SELECT id, type FROM Element WHERE name = ?",
            arrayOf(elementName)
        ).use {
            if (!it.moveToNext()) return null
            val id = it.getString("id")
            val type = it.getString("type")
            Pair(id, type)
        }
        return when (type) {
            ElementType.Edge.code -> EdgeIdImpl(id)
            ElementType.Node.code -> NodeIdImpl(id)
            else -> error("Unknown element type $type for node id=$id")
        }
    }

    fun getElementName(driver: SqliteDriver, elementId: ElementId): String? {
        return driver.query(
            "SELECT name FROM Element WHERE id = ?",
            arrayOf(elementId.toString())
        ).use {
            if (it.moveToNext()) {
                it.getString("name")
            } else {
                null
            }
        }
    }

    fun getFieldId(driver: SqliteDriver, field: Field<*, *>, schemaId: String): FieldId {
        return driver.query(
            "SELECT id FROM Field WHERE schemaId = ? AND name = ?",
            arrayOf(schemaId, field.name)
        ).use {
            if (it.moveToNext()) {
                FieldIdImpl(it.getString("id"))
            } else {
                error("Could not find field $field for schemaId $schemaId")
            }
        }
    }

    fun getFieldId(driver: SqliteDriver, field: Field<*, *>, schema: Schema): FieldId {
        return driver.query(
            "SELECT id FROM Field WHERE schemaId = (SELECT id FROM Schema WHERE name = ? AND version = ? LIMIT 1) AND name = ?",
            arrayOf(schema.schemaName, schema.schemaVersion.toString(), field.name)
        ).use {
            if (it.moveToNext()) {
                FieldIdImpl(it.getString("id"))
            } else {
                error("Could not find field $field for schema $schema")
            }
        }
    }

    fun getSchemaFields(driver: SqliteDriver, schemaId: String): Sequence<Field<*, *>> {
        return sequence {
            driver.query("SELECT name, type from Field WHERE schemaId = ?", arrayOf(schemaId))
                .use { fieldCursor ->
                    while (fieldCursor.moveToNext()) {
                        val fieldName = fieldCursor.getString("name")
                        val fieldTypeCode = fieldCursor.getString("type")
                        val field: Field<Schema, Any> =
                            when (val fieldType = FieldType.fromCode(fieldTypeCode)) {
                                is FieldType.Blob -> FieldImpl<Schema, Any>(fieldName, fieldType)
                                is FieldType.Geo -> IndexableFieldImpl<Schema, Any>(
                                    fieldName,
                                    fieldType
                                )
                                else -> IndexableScalarFieldImpl<Schema, Any>(fieldName, fieldType)
                            }
                        yield(field)
                    }
                }
        }
    }
}
