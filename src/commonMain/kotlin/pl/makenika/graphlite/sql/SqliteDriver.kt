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

interface SqliteDriver {
    fun getDbVersion(): Int? {
        if (!isInitialized()) {
            return null
        }

        return query("SELECT $VERSION_COL from $VERSION_TABLE").use { cursor ->
            if (cursor.moveToNext()) {
                cursor.getInt(VERSION_COL)
            } else {
                null
            }
        }
    }

    fun initialize(version: Int) {
        //language=SQLITE-SQL
        execute("create table $VERSION_TABLE (version integer)")
        //language=SQLITE-SQL
        execute("insert into $VERSION_TABLE values ($version)")
    }

    fun isInitialized(): Boolean {
        return query("SELECT name FROM sqlite_master WHERE type='table' AND name='$VERSION_TABLE'").use {
            it.moveToNext()
        }
    }

    fun updateVersion(version: Int) {
        //language=SQLITE-SQL
        execute("update $VERSION_TABLE set $VERSION_COL = $version")
    }

    fun close()

    fun beginTransaction()
    fun endTransaction()
    fun setTransactionSuccessful()

    fun <T> transaction(fn: () -> T): T {
        beginTransaction()
        try {
            val result = fn()
            setTransactionSuccessful()
            return result
        } finally {
            endTransaction()
        }
    }

    fun <T> transactionWithRollback(fn: () -> T): TransactionResult<T> {
        beginTransaction()
        return try {
            val result = fn()
            setTransactionSuccessful()
            TransactionResult.Ok(result)
        } catch (e: RollbackException) {
            TransactionResult.Fail(e)
        } finally {
            endTransaction()
        }
    }

    fun delete(table: String, whereClause: String, whereArgs: Array<String>): Boolean
    fun execute(sql: String)
    fun insertOrAbortAndThrow(table: String, values: SqlContentValues)
    fun query(sql: String, selectionArgs: Array<String>? = null): SqliteCursorFacade
    fun updateOrReplace(table: String, values: SqlContentValues, whereClause: String, whereArgs: Array<String>)

    companion object {
        private const val VERSION_TABLE = "_SqlDriverSchemaVersion"
        private const val VERSION_COL = "version"
    }
}
