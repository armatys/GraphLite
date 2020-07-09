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

package pl.makenika.graphlite.sql

import org.sqlite.SQLiteException
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Types
import java.util.concurrent.atomic.AtomicInteger

class JvmSqliteDriver private constructor(private val connection: Connection) : SqliteDriver {
    private val transactions = AtomicInteger(0)
    private val successful = AtomicInteger(0)

    init {
        connection.autoCommit = true
        connection.prepareStatement("PRAGMA foreign_keys = ON").execute()
    }

    override fun close() {
        connection.close()
    }

    override fun beginTransaction() {
        if (transactions.getAndIncrement() == 0) {
            connection.autoCommit = false
            successful.set(0)
        }
        successful.incrementAndGet()
    }

    override fun endTransaction() {
        val transactionCount = transactions.decrementAndGet()
        if (transactionCount == 0) {
            if (successful.get() != transactionCount) {
                connection.rollback()
                transactions.set(0)
                connection.autoCommit = true
            } else {
                connection.commit()
            }
            connection.autoCommit = true
        }
    }

    override fun setTransactionSuccessful() {
        successful.decrementAndGet()
    }

    override fun delete(table: String, whereClause: String, whereArgs: Array<String>): Boolean {
        val stmt = connection.prepareStatement("DELETE FROM $table WHERE $whereClause")
        whereArgs.forEachIndexed { i, arg ->
            stmt.setString(i + 1, arg)
        }
        return stmt.executeUpdate() > 0
    }

    override fun execute(sql: String) {
        connection.createStatement().execute(sql)
    }

    override fun insertOrAbortAndThrow(table: String, values: SqlContentValues) {
        val columnNames = values.keySet().joinToString(", ")
        val valuesPlaceholder = values.keySet().joinToString(", ") { "?" }
        val sql = "INSERT OR ABORT INTO $table ($columnNames) VALUES ($valuesPlaceholder)"
        val stmt = connection.prepareStatement(sql)
        values.keySet().forEachIndexed { i, key ->
            when (val value = values.get(key)) {
                is SqlValue.SqlBlob -> stmt.setBytes(i + 1, value.value)
                is SqlValue.SqlLong -> stmt.setLong(i + 1, value.value)
                SqlValue.SqlNull -> stmt.setNull(i + 1, Types.NULL)
                is SqlValue.SqlReal -> stmt.setDouble(i + 1, value.value)
                is SqlValue.SqlString -> stmt.setString(i + 1, value.value)
            }
        }
        try {
            stmt.executeUpdate()
        } catch (e: SQLiteException) {
            throw RollbackException(e)
        }
    }

    override fun query(sql: String, selectionArgs: Array<String>?): SqliteCursorFacade {
        val stmt = connection.prepareStatement(sql)
        selectionArgs?.forEachIndexed { i, arg ->
            stmt.setString(i + 1, arg)
        }
        val resultSet = stmt.executeQuery()
        return SqliteCursorFacade(resultSet)
    }

    override fun updateOrReplace(
        table: String,
        values: SqlContentValues,
        whereClause: String,
        whereArgs: Array<String>
    ) {
        val columnValues = values.keySet().joinToString(", ") { "$it = ?" }
        val sql = "UPDATE OR REPLACE $table SET $columnValues WHERE $whereClause"
        val stmt = connection.prepareStatement(sql)
        values.keySet().forEachIndexed { i, key ->
            when (val value = values.get(key)) {
                is SqlValue.SqlBlob -> stmt.setBytes(i + 1, value.value)
                is SqlValue.SqlLong -> stmt.setLong(i + 1, value.value)
                SqlValue.SqlNull -> stmt.setNull(i + 1, Types.NULL)
                is SqlValue.SqlReal -> stmt.setDouble(i + 1, value.value)
                is SqlValue.SqlString -> stmt.setString(i + 1, value.value)
            }
        }
        val valueCount = values.keySet().size
        whereArgs.forEachIndexed { i, arg ->
            stmt.setString(valueCount + i + 1, arg)
        }
        stmt.execute()
    }

    companion object {
        fun newInstance(database: File): JvmSqliteDriver {
            val connection = DriverManager.getConnection("jdbc:sqlite:${database.absolutePath}")
            return JvmSqliteDriver(connection)
        }

        fun newInstanceInMemory(): JvmSqliteDriver {
            val connection = DriverManager.getConnection("jdbc:sqlite::memory:")
            return JvmSqliteDriver(connection)
        }
    }
}
