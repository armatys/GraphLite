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
    fun getElementId(driver: SqliteDriver, handle: ElementHandle): String? {
        return driver.query(
            "SELECT id, type FROM Element WHERE handle = ?",
            arrayOf(handle.value)
        ).use {
            if (!it.moveToNext()) {
                null
            } else {
                it.getString("id")
            }
        }
    }

    fun getFieldId(driver: SqliteDriver, field: Field<*, *>, schemaId: String): String {
        return driver.query(
            "SELECT id FROM Field WHERE schemaId = ? AND handle = ?",
            arrayOf(schemaId, field.handle.value)
        ).use {
            if (it.moveToNext()) {
                it.getString("id")
            } else {
                error("Could not find field $field for schemaId $schemaId")
            }
        }
    }

    fun getFieldId(driver: SqliteDriver, field: Field<*, *>, schema: Schema): String {
        return driver.query(
            "SELECT id FROM Field WHERE schemaId = (SELECT id FROM Schema WHERE handle = ? AND version = ? LIMIT 1) AND handle = ?",
            arrayOf(schema.schemaHandle.value, schema.schemaVersion.toString(), field.handle.value)
        ).use {
            if (it.moveToNext()) {
                it.getString("id")
            } else {
                error("Could not find field $field for schema $schema")
            }
        }
    }

    fun getSchemaFields(driver: SqliteDriver, schemaId: String): Sequence<Field<*, *>> {
        return sequence {
            driver.query("SELECT handle, type from Field WHERE schemaId = ?", arrayOf(schemaId))
                .use { fieldCursor ->
                    while (fieldCursor.moveToNext()) {
                        val fieldHandleValue = fieldCursor.getString("handle")
                        val fieldTypeCode = fieldCursor.getString("type")
                        val field: Field<Schema, Any> =
                            when (val fieldType = FieldType.fromCode(fieldTypeCode)) {
                                is FieldType.Blob -> FieldImpl<Schema, Any>(
                                    FieldHandle(fieldHandleValue),
                                    fieldType
                                )
                                is FieldType.Geo -> IndexableFieldImpl<Schema, Any>(
                                    FieldHandle(fieldHandleValue),
                                    fieldType
                                )
                                else -> IndexableScalarFieldImpl<Schema, Any>(
                                    FieldHandle(fieldHandleValue),
                                    fieldType
                                )
                            }
                        yield(field)
                    }
                }
        }
    }
}
