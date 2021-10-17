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
import pl.makenika.graphlite.sql.*
import kotlin.math.abs

private const val SQL_SCHEMA_VERSION = 1L
private const val GRAPH_DB_META_TABLE_NAME = "_GraphDbMeta"
private const val COL_GRAPH_DB_VERSION = "graph_db_vesion"

public typealias DbVersion = Long

public class GraphLiteDatabaseBuilder(
    private val driver: SqliteDriver,
    private val dbVersion: DbVersion
) {
    /** Map from target db version to a [Migration]. */
    private val upgradeMigrations = mutableMapOf<DbVersion, Migration>()
    private val downgradeMigrations = mutableMapOf<DbVersion, Migration>()

    private val registeredSchemas = mutableMapOf<DbVersion, MutableMap<SchemaHandle, Schema>>()
    private var shouldDeleteOnSchemaConflict = false

    public fun deleteOnSchemaConflict(shouldDelete: Boolean): GraphLiteDatabaseBuilder {
        shouldDeleteOnSchemaConflict = shouldDelete
        return this
    }

    public fun register(dbVersion: DbVersion, vararg schemas: Schema): GraphLiteDatabaseBuilder {
        val schemasForGivenDbVersion = registeredSchemas.getOrPut(dbVersion) { mutableMapOf() }
        schemas.forEach { schema ->
            schema.freeze()
            require(schemasForGivenDbVersion.put(schema.schemaHandle, schema) == null) {
                "Schema $schema was already registered (dbVersion=$dbVersion)."
            }
        }
        return this
    }

    public fun migration(
        oldDbVersion: DbVersion,
        newDbVersion: DbVersion,
        migration: Migration
    ): GraphLiteDatabaseBuilder {
        require(abs(oldDbVersion - newDbVersion) == 1L) { "Only migrations between subsequent versions are supported." }
        val migrations = if (newDbVersion > oldDbVersion) upgradeMigrations else downgradeMigrations
        require(migrations.put(newDbVersion, migration) == null) {
            "Migration from version $oldDbVersion to version $newDbVersion was already added."
        }
        return this
    }

    public fun open(): GraphLiteDatabase {
        SqliteDriverHelper(driver, SQL_SCHEMA_VERSION, onCreate = { sqliteDriver ->
            schemaV1.forEach { sqliteDriver.execute(it) }
            sqliteDriver.execute("CREATE TABLE IF NOT EXISTS $GRAPH_DB_META_TABLE_NAME ($COL_GRAPH_DB_VERSION INT)")
            sqliteDriver.execute("INSERT INTO $GRAPH_DB_META_TABLE_NAME VALUES (NULL)")
        })
        val currentDbVersion: DbVersion = loadCurrentDbVersion() ?: dbVersion
        val db = GraphLiteDatabaseImpl(driver)

        val schemasFromDb: Map<SchemaHandle, Schema> = loadSchemasFromDb()
        val currentSchemas = registeredSchemas.getOrDefault(dbVersion, emptyMap()).values
        val schemasToDelete = registeredSchemas.filter { it.key != dbVersion }.values
            .flatMap { it.values }
            .toSet()
            .minus(currentSchemas)
        val schemasToInsert = currentSchemas.minus(schemasFromDb.values)

        val dbVersionDelta = dbVersion - currentDbVersion
        val migrationsToRun: List<Migration> = when {
            dbVersionDelta == 0L -> emptyList()
            dbVersionDelta > 0 -> {
                ((currentDbVersion + 1)..dbVersion).map {
                    upgradeMigrations[it] ?: error("Missing upgrade migration to version $it")
                }
            }
            else -> {
                ((currentDbVersion - 1)..dbVersion).map {
                    downgradeMigrations[it] ?: error("Missing downgrade migration to version $it")
                }
            }
        }

        driver.transaction {
            updateCurrentDbVersion()
            schemasToInsert.forEach { insertSchema(it) }
            migrationsToRun.forEach { it(db) }
            schemasToDelete.forEach { deleteSchema(it) }
        }

        // TODO after migrations, check if all the values are associated with the newest schemas

        return db
    }

    private fun loadCurrentDbVersion(): DbVersion? {
        val v = driver.query("SELECT $COL_GRAPH_DB_VERSION FROM $GRAPH_DB_META_TABLE_NAME").use {
            if (it.moveToNext()) {
                it.findLong(COL_GRAPH_DB_VERSION)
            } else {
                null
            }
        }
        return v
    }

    private fun updateCurrentDbVersion() {
        driver.execute("update $GRAPH_DB_META_TABLE_NAME set $COL_GRAPH_DB_VERSION = $dbVersion")
    }

    private fun loadSchemasFromDb(): Map<SchemaHandle, Schema> {
        val schemas = mutableMapOf<SchemaHandle, Schema>()
        driver.query("SELECT * FROM Schema").use { schemaCursor ->
            while (schemaCursor.moveToNext()) {
                val schemaId = schemaCursor.getString("id")
                val handleValue = schemaCursor.getString("handle")
                val version = schemaCursor.getLong("version")
                val schemaHandle = SchemaHandle(handleValue)
                val schema = object : Schema(schemaHandle, version) {}

                getSchemaFields(driver, schemaId).forEach {
                    schema.addField(it)
                }

                schema.freeze()
                check(schemas.put(schemaHandle, schema) == null)
            }
        }
        return schemas
    }

    private fun insertSchema(schema: Schema) {
        val schemaId = uuid4().toString()
        val schemaValues = SqlContentValues().apply {
            put("id", schemaId)
            put("handle", schema.schemaHandle.value)
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

    private fun deleteSchema(schema: Schema) {
        driver.delete(
            "Schema",
            "handle = ? AND version = ?",
            arrayOf(schema.schemaHandle.value, schema.schemaVersion.toString())
        )
    }
}

public typealias Migration = (GraphLiteDatabase) -> Unit
