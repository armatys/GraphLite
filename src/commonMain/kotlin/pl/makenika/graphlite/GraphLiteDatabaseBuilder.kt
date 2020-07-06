package pl.makenika.graphlite

import com.benasher44.uuid.uuid4
import pl.makenika.graphlite.impl.GraphLiteDatabaseImpl
import pl.makenika.graphlite.impl.GraphSqlUtils.getSchemaFields
import pl.makenika.graphlite.impl.baseFieldTypeName
import pl.makenika.graphlite.impl.isFieldTypeNullable
import pl.makenika.graphlite.sql.*

class GraphLiteDatabaseBuilder(private val driver: SqliteDriver) {
    private val migrations = LinkedHashMap<String, MutableMap<Int, Migration>>()
    private val registeredSchemas = mutableMapOf<String, Schema>()
    private var shouldDeleteOnSchemaConflict = false

    fun deleteOnSchemaConflict(shouldDelete: Boolean): GraphLiteDatabaseBuilder {
        shouldDeleteOnSchemaConflict = shouldDelete
        return this
    }

    fun register(schema: Schema): GraphLiteDatabaseBuilder {
        schema.freeze()
        check(registeredSchemas.put(schema.schemaName, schema) == null)
        return this
    }

    fun <O : Schema, N : Schema> migration(
        oldSchema: O,
        newSchema: N,
        fn: (GraphLiteDatabase) -> Unit
    ): GraphLiteDatabaseBuilder {
        oldSchema.freeze()
        newSchema.freeze()

        require(newSchema.schemaName == oldSchema.schemaName)
        require(newSchema.schemaVersion == oldSchema.schemaVersion + 1)

        val targetVersionMigrations = migrations.getOrPut(newSchema.schemaName, { mutableMapOf() })
        check(targetVersionMigrations.put(newSchema.schemaVersion, fn) == null)
        return this
    }

    fun open(): GraphLiteDatabase {
        val helper = GraphDriverHelper(driver, SQL_SCHEMA_VERSION)
        val db = GraphLiteDatabaseImpl(helper.open())

        val schemasFromDb: Map<String, Schema> = loadSchemasFromDb()
        val migrationsToRun = mutableListOf<MigrationRequest>()
        val schemasToDelete = schemasFromDb.minus(registeredSchemas.keys).values.toMutableList()
        val schemasToInsert = mutableListOf<Schema>()

        for ((schemaName, registeredSchema) in registeredSchemas) {
            val dbSchema = schemasFromDb[schemaName]
            if (dbSchema == null) {
                schemasToInsert.add(registeredSchema)
            } else if (dbSchema.schemaVersion != registeredSchema.schemaVersion) {
                check(registeredSchema.schemaVersion > dbSchema.schemaVersion)
                schemasToInsert.add(registeredSchema)
                schemasToDelete.add(dbSchema)
                migrationsToRun.add(MigrationRequest(dbSchema, registeredSchema))
            } else if (dbSchema.getFields<Schema>() != registeredSchema.getFields<Schema>()) {
                if (shouldDeleteOnSchemaConflict) {
                    schemasToInsert.add(registeredSchema)
                    schemasToDelete.add(dbSchema)
                } else {
                    error("Schema for $schemaName with version ${registeredSchema.schemaVersion} has a conflict.")
                }
            }
        }

        // check we have all necessary migrations
        for (migrationRequest in migrationsToRun) {
            val targetVersionMigrations = migrations[migrationRequest.newSchema.schemaName] ?: mutableMapOf()
            for (i in (migrationRequest.oldSchema.schemaVersion + 1)..migrationRequest.newSchema.schemaVersion) {
                if (targetVersionMigrations[i] == null) {
                    error("Missing migration for ${migrationRequest.newSchema.schemaName} from version ${migrationRequest.oldSchema.schemaVersion} to version ${migrationRequest.newSchema.schemaVersion}")
                }
            }
        }

        driver.transaction {
            schemasToInsert.forEach { insertSchema(it) }
            migrationsToRun.forEach { performMigration(db, it) }
            schemasToDelete.forEach { deleteSchema(it) }
        }

        return db
    }

    private fun loadSchemasFromDb(): Map<String, Schema> {
        val schemas = mutableMapOf<String, Schema>()
        driver.query("SELECT * FROM Schema").use { schemaCursor ->
            while (schemaCursor.moveToNext()) {
                val schemaId = schemaCursor.getString("id")
                val name = schemaCursor.getString("name")
                val version = schemaCursor.getInt("version")
                val schema = object : Schema(name, version) {}

                getSchemaFields(driver, schemaId).forEach {
                    schema.addField(it)
                }

                schema.freeze()
                check(schemas.put(name, schema) == null)
            }
        }
        return schemas
    }

    private fun insertSchema(schema: Schema) {
        val schemaId = uuid4().toString()
        val schemaValues = SqlContentValues().apply {
            put("id", schemaId)
            put("name", schema.schemaName)
            put("version", schema.schemaVersion)
        }
        driver.insertOrAbortAndThrow("Schema", schemaValues)

        for ((_, field) in schema.getFields<Schema>()) {
            val fieldId = FieldIdImpl(uuid4().toString())
            val fieldValues = SqlContentValues().apply {
                put("id", fieldId.toString())
                put("name", field.name)
                put("schemaId", schemaId)
                put("type", field.type)
            }
            driver.insertOrAbortAndThrow("Field", fieldValues)

            val isNullable = isFieldTypeNullable(field.type)
            val createTableStatements = when (val typeName = baseFieldTypeName(field.type)) {
                Field.FIELD_TYPE_BLOB -> createTableFieldValueBlob(fieldId, isNullable)
                Field.FIELD_TYPE_GEO -> createTableFieldValueGeo(fieldId, isNullable)
                Field.FIELD_TYPE_INT -> createTableFieldValueInt(fieldId, isNullable)
                Field.FIELD_TYPE_REAL -> createTableFieldValueReal(fieldId, isNullable)
                Field.FIELD_TYPE_TEXT -> createTableFieldValueText(fieldId, isNullable)
                else -> error("Unknown field type: $typeName")
            }
            createTableStatements.forEach {
                driver.execute(it)
            }
        }
    }

    private fun performMigration(db: GraphLiteDatabase, migrationRequest: MigrationRequest) {
        val targetVersionMigrations = migrations[migrationRequest.newSchema.schemaName] ?: emptyMap<Int, Migration>()
        for (v in (migrationRequest.oldSchema.schemaVersion + 1)..migrationRequest.newSchema.schemaVersion) {
            val migration = targetVersionMigrations[v]
                ?: error("Missing migration for ${migrationRequest.newSchema.schemaName} from version ${migrationRequest.oldSchema.schemaVersion} to version ${migrationRequest.newSchema.schemaVersion}.")
            migration(db)
        }
    }

    private fun deleteSchema(schema: Schema) {
        driver.delete("Schema", "name = ? AND version = ?", arrayOf(schema.schemaName, schema.schemaVersion.toString()))
    }

    companion object {
        private const val SQL_SCHEMA_VERSION = 1
    }
}

private typealias Migration = (GraphLiteDatabase) -> Unit

private class MigrationRequest(val oldSchema: Schema, val newSchema: Schema)

private class GraphDriverHelper(driver: SqliteDriver, version: Int) : SqliteDriverHelper(driver, version) {
    override fun onCreate(driver: SqliteDriver) {
        schemaV1.forEach { driver.execute(it) }
    }

    override fun onUpgrade(driver: SqliteDriver, oldVersion: Int, newVersion: Int) {
    }
}
