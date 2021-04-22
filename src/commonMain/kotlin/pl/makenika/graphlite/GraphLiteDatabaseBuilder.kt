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

import com.benasher44.uuid.uuid4
import pl.makenika.graphlite.impl.GraphLiteDatabaseImpl
import pl.makenika.graphlite.impl.GraphSqlUtils.getSchemaFields
import pl.makenika.graphlite.impl.SchemaInternal
import pl.makenika.graphlite.sql.*

public class GraphLiteDatabaseBuilder(private val driver: SqliteDriver) {
    private val migrations = LinkedHashMap<SchemaHandle, MutableMap<Long, Migration>>()
    private val registeredSchemas = mutableMapOf<SchemaHandle, Schema>()
    private var shouldDeleteOnSchemaConflict = false

    public fun deleteOnSchemaConflict(shouldDelete: Boolean): GraphLiteDatabaseBuilder {
        shouldDeleteOnSchemaConflict = shouldDelete
        return this
    }

    public fun register(schema: Schema): GraphLiteDatabaseBuilder {
        schema.freeze()
        check(registeredSchemas.put(schema.schemaHandle, schema) == null)
        return this
    }

    public fun <O : Schema, N : Schema> migration(
        oldSchema: O,
        newSchema: N,
        fn: Migration
    ): GraphLiteDatabaseBuilder {
        oldSchema.freeze()
        newSchema.freeze()

        require(newSchema.schemaHandle == oldSchema.schemaHandle)
        require(newSchema.schemaVersion == oldSchema.schemaVersion + 1)

        val targetVersionMigrations =
            migrations.getOrPut(newSchema.schemaHandle, { mutableMapOf() })
        check(targetVersionMigrations.put(newSchema.schemaVersion, fn) == null)
        return this
    }

    public suspend fun open(): GraphLiteDatabase {
        val helper = GraphDriverHelper(driver, SQL_SCHEMA_VERSION)
        val db = GraphLiteDatabaseImpl(helper.open())

        val schemasFromDb: Map<SchemaHandle, SchemaInternal> = loadSchemasFromDb()
        val migrationsToRun = mutableListOf<MigrationRequest>()
        val schemasToDelete = schemasFromDb.minus(registeredSchemas.keys).values.toMutableList()
        val schemasToInsert = mutableListOf<Schema>()
        val schemasToUpdate = mutableListOf<Pair<SchemaInternal, Schema>>()

        for ((schemaHandleValue, registeredSchema) in registeredSchemas) {
            val dbSchema = schemasFromDb[schemaHandleValue]
            if (dbSchema == null) {
                schemasToInsert.add(registeredSchema)
            } else if (dbSchema.schemaVersion != registeredSchema.schemaVersion) {
                check(registeredSchema.schemaVersion > dbSchema.schemaVersion)
                schemasToUpdate.add(Pair(dbSchema, registeredSchema))
                migrationsToRun.add(MigrationRequest(dbSchema, registeredSchema))
            } else if (dbSchema.getFields<Schema>() != registeredSchema.getFields<Schema>()) {
                if (shouldDeleteOnSchemaConflict) {
                    schemasToInsert.add(registeredSchema)
                    schemasToDelete.add(dbSchema)
                } else {
                    error("Schema for $schemaHandleValue with version ${registeredSchema.schemaVersion} has a conflict.")
                }
            }
        }

        // check we have all necessary migrations
        for (migrationRequest in migrationsToRun) {
            val targetVersionMigrations =
                migrations[migrationRequest.newSchema.schemaHandle] ?: mutableMapOf()
            for (i in (migrationRequest.oldSchema.schemaVersion + 1)..migrationRequest.newSchema.schemaVersion) {
                if (targetVersionMigrations[i] == null) {
                    error("Missing migration for ${migrationRequest.newSchema.schemaHandle} from version ${migrationRequest.oldSchema.schemaVersion} to version ${migrationRequest.newSchema.schemaVersion}")
                }
            }
        }

        driver.transaction {
            schemasToInsert.forEach { insertSchema(it) }
            schemasToUpdate.forEach { updateSchema(it.first, it.second) }
            migrationsToRun.forEach { performMigration(db, it) }
            schemasToDelete.forEach { deleteSchema(it) }
        }

        return db
    }

    private fun loadSchemasFromDb(): Map<SchemaHandle, SchemaInternal> {
        val schemas = mutableMapOf<SchemaHandle, SchemaInternal>()
        driver.query("SELECT * FROM Schema ORDER BY clockValue ASC").use { schemaCursor ->
            while (schemaCursor.moveToNext()) {
                val schemaId = schemaCursor.getString("id")
                val clockValue = schemaCursor.getLong("clockValue")
                val handleValue = schemaCursor.getString("handle")
                val parentId = schemaCursor.findString("parentId")
                val version = schemaCursor.getLong("version")
                val schemaHandle = SchemaHandle(handleValue)
                val schema = SchemaInternal(
                    schemaId,
                    clockValue,
                    parentId,
                    schemaHandle,
                    version
                )

                getSchemaFields(driver, schemaId).forEach {
                    schema.addField(it)
                }

                schema.freeze()
                check(schemas.put(schemaHandle, schema) == null)
            }
        }
        return schemas
    }

    private fun insertSchema(schema: Schema, parentId: String? = null) {
        val schemaId = uuid4().toString()
        val schemaValues = SqlContentValues().apply {
            put("id", schemaId)
            put("handle", schema.schemaHandle.value)
            if (parentId != null) put("parentId", parentId)
            put("version", schema.schemaVersion)
        }
        driver.insertOrAbortAndThrow("Schema", schemaValues)

        for ((_, field) in schema.getFields<Schema>()) {
            val fieldId = uuid4().toString()
            val fieldValues = SqlContentValues().apply {
                put("id", fieldId)
                put("handle", field.handle.value)
                put("schemaId", schemaId)
                put("type", field.type.code)
            }
            driver.insertOrAbortAndThrow("Field", fieldValues)

            val isNullable = field.type.optional
            val createTableStatements = when (field.type) {
                is FieldType.Blob -> createTableFieldValueBlob(fieldId, isNullable)
                is FieldType.Geo -> createTableFieldValueGeo(fieldId, isNullable)
                is FieldType.LongInt -> createTableFieldValueLongInt(fieldId, isNullable)
                is FieldType.DoubleFloat -> createTableFieldValueDoubleFloat(fieldId, isNullable)
                is FieldType.Text -> createTableFieldValueText(
                    fieldId,
                    isValueOptional = isNullable,
                    fts = false
                )
                is FieldType.TextFts -> createTableFieldValueText(
                    fieldId,
                    isValueOptional = isNullable,
                    fts = true
                )
            }
            createTableStatements.forEach {
                driver.execute(it)
            }
        }
    }

    private suspend fun performMigration(
        db: GraphLiteDatabase,
        migrationRequest: MigrationRequest
    ) {
        val targetVersionMigrations: Map<Long, Migration> =
            migrations[migrationRequest.newSchema.schemaHandle] ?: emptyMap()
        for (v in (migrationRequest.oldSchema.schemaVersion + 1)..migrationRequest.newSchema.schemaVersion) {
            val migration = targetVersionMigrations[v]
                ?: error("Missing migration for ${migrationRequest.newSchema.schemaHandle} from version ${migrationRequest.oldSchema.schemaVersion} to version ${migrationRequest.newSchema.schemaVersion}.")
            migration(db)
        }
    }

    private fun deleteSchema(schema: Schema) {
        driver.delete(
            "Schema",
            "handle = ? AND version = ?",
            arrayOf(schema.schemaHandle.value, schema.schemaVersion.toString())
        )
    }

    private fun updateSchema(oldSchema: SchemaInternal, newSchema: Schema) {
        insertSchema(newSchema, parentId = oldSchema.id)
    }

    internal companion object {
        private const val SQL_SCHEMA_VERSION = 1L
    }
}

private typealias Migration = suspend (GraphLiteDatabase) -> Unit

private class MigrationRequest(val oldSchema: Schema, val newSchema: Schema)

private class GraphDriverHelper(driver: SqliteDriver, version: Long) :
    SqliteDriverHelper(driver, version) {
    override fun onCreate(driver: SqliteDriver) {
        sqlTablesV1.forEach { driver.execute(it) }
    }

    override fun onUpgrade(driver: SqliteDriver, oldVersion: Long, newVersion: Long) {
    }
}
