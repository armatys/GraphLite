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

import pl.makenika.graphlite.use

public expect class ThreadLocalState<T> constructor() {
    public fun get(): T?
    public fun remove()
    public fun set(value: T)
}

public abstract class SqliteDriver {
    internal fun getSqlSchemaVersion(): Long? {
        if (!isInitialized()) {
            return null
        }

        return query("SELECT $COL_SQL_SCHEMA_VERSION from $SQL_DRIVER_META_TABLE_NAME").use { cursor ->
            if (cursor.moveToNext()) {
                cursor.getLong(COL_SQL_SCHEMA_VERSION)
            } else {
                null
            }
        }
    }

    internal fun initialize(version: Long) {
        //language=SQLITE-SQL
        execute("create table $SQL_DRIVER_META_TABLE_NAME ($COL_SQL_SCHEMA_VERSION integer)")
        //language=SQLITE-SQL
        execute("insert into $SQL_DRIVER_META_TABLE_NAME values ($version)")
    }

    private fun isInitialized(): Boolean {
        return query("SELECT name FROM sqlite_master WHERE type='table' AND name='$SQL_DRIVER_META_TABLE_NAME'").use {
            it.moveToNext()
        }
    }

    internal fun updateSqlSchemaVersion(version: Long) {
        //language=SQLITE-SQL
        execute("update $SQL_DRIVER_META_TABLE_NAME set $COL_SQL_SCHEMA_VERSION = $version")
    }

    internal abstract fun close()

    protected abstract fun beginTransaction()
    protected abstract fun endTransaction()
    protected abstract fun setTransactionSuccessful()

    private val transactionFlag = ThreadLocalState<Unit>()

    private fun isInTransaction() = transactionFlag.get() != null

    internal fun <T> transaction(fn: () -> T): T {
        return if (isInTransaction()) {
            fn()
        } else {
            transactionFlag.set(Unit)
            beginTransaction()
            try {
                val result = fn()
                setTransactionSuccessful()
                result
            } finally {
                endTransaction()
                transactionFlag.remove()
            }
        }
    }

    internal fun <T> transactionWithRollback(fn: () -> T): TransactionResult<T> {
        return if (isInTransaction()) {
            TransactionResult.Ok(fn())
        } else {
            beginTransaction()
            try {
                val result = fn()
                setTransactionSuccessful()
                TransactionResult.Ok(result)
            } catch (e: RollbackException) {
                TransactionResult.Fail(e)
            } finally {
                endTransaction()
                transactionFlag.remove()
            }
        }
    }

    internal abstract fun delete(
        table: String,
        whereClause: String,
        whereArgs: Array<String>
    ): Boolean

    internal abstract fun execute(sql: String)
    internal abstract fun insertOrAbortAndThrow(table: String, values: SqlContentValues)
    internal abstract fun query(
        sql: String,
        selectionArgs: Array<String>? = null
    ): SqliteCursorFacade

    internal abstract fun updateOrReplace(
        table: String,
        values: SqlContentValues,
        whereClause: String?,
        whereArgs: Array<String>?
    )

    internal companion object {
        private const val SQL_DRIVER_META_TABLE_NAME = "_SqlDriverMeta"
        private const val COL_SQL_SCHEMA_VERSION = "sql_schema_version"
    }
}
