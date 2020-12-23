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

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import pl.makenika.graphlite.use
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

public abstract class SqliteDriver {
    internal fun getDbVersion(): Long? {
        if (!isInitialized()) {
            return null
        }

        return query("SELECT $VERSION_COL from $VERSION_TABLE").use { cursor ->
            if (cursor.moveToNext()) {
                cursor.getLong(VERSION_COL)
            } else {
                null
            }
        }
    }

    internal fun initialize(version: Long) {
        //language=SQLITE-SQL
        execute("create table $VERSION_TABLE (version integer)")
        //language=SQLITE-SQL
        execute("insert into $VERSION_TABLE values ($version)")
    }

    private fun isInitialized(): Boolean {
        return query("SELECT name FROM sqlite_master WHERE type='table' AND name='$VERSION_TABLE'").use {
            it.moveToNext()
        }
    }

    internal fun updateVersion(version: Long) {
        //language=SQLITE-SQL
        execute("update $VERSION_TABLE set $VERSION_COL = $version")
    }

    internal abstract fun close()

    protected abstract fun beginTransaction()
    protected abstract fun endTransaction()
    protected abstract fun setTransactionSuccessful()

    private val transactionMutex = Mutex(false)

    private class TransactionContext : AbstractCoroutineContextElement(TransactionContext) {
        companion object Key : CoroutineContext.Key<TransactionContext>
    }

    internal suspend fun <T> transaction(fn: suspend () -> T): T {
        val inTransaction = coroutineContext[TransactionContext] != null
        return if (inTransaction) {
            fn()
        } else {
            withContext(TransactionContext()) {
                transactionMutex.withLock {
                    beginTransaction()
                    try {
                        val result = fn()
                        setTransactionSuccessful()
                        result
                    } finally {
                        endTransaction()
                    }
                }
            }
        }
    }

    internal suspend fun <T> transactionWithRollback(fn: () -> T): TransactionResult<T> {
        val inTransaction = coroutineContext[TransactionContext] != null
        return if (inTransaction) {
            TransactionResult.Ok(fn())
        } else {
            withContext(TransactionContext()) {
                transactionMutex.withLock {
                    beginTransaction()
                    try {
                        val result = fn()
                        setTransactionSuccessful()
                        TransactionResult.Ok(result)
                    } catch (e: RollbackException) {
                        TransactionResult.Fail(e)
                    } finally {
                        endTransaction()
                    }
                }
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
        whereClause: String,
        whereArgs: Array<String>
    )

    internal companion object {
        private const val VERSION_TABLE = "_SqlDriverSchemaVersion"
        private const val VERSION_COL = "version"
    }
}
