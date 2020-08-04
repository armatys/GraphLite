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

import android.content.Context
import org.sqlite.database.DatabaseUtils
import org.sqlite.database.SQLException
import org.sqlite.database.sqlite.SQLiteDatabase
import org.sqlite.database.sqlite.SQLiteOpenHelper
import java.lang.StringBuilder

class AndroidSqliteDriver private constructor(private val db: SQLiteDatabase) : SqliteDriver() {
    override fun beginTransaction() = db.beginTransaction()

    override fun endTransaction() = db.endTransaction()

    override fun setTransactionSuccessful() = db.setTransactionSuccessful()

    override fun close() = db.close()

    override fun delete(table: String, whereClause: String, whereArgs: Array<String>): Boolean {
        return db.delete(table, whereClause, whereArgs) > 0
    }

    override fun execute(sql: String) = db.execSQL(sql)

    override fun insertOrAbortAndThrow(table: String, values: SqlContentValues) {
        try {
            db.insertWithOnConflict(table, null, values, SQLiteDatabase.CONFLICT_ABORT)
        } catch (e: SQLException) {
            throw RollbackException(e)
        }
    }

    override fun query(sql: String, selectionArgs: Array<String>?): SqliteCursorFacade {
        val cursor = db.rawQuery(sql, selectionArgs ?: emptyArray())
        return SqliteCursorFacade(cursor)
    }

    override fun updateOrReplace(
        table: String,
        values: SqlContentValues,
        whereClause: String,
        whereArgs: Array<String>
    ) {
        db.updateWithOnConflict(
            table,
            values,
            whereClause,
            whereArgs,
            SQLiteDatabase.CONFLICT_REPLACE
        )
    }

    companion object {
        init {
            System.loadLibrary("sqliteX")
        }

        fun newInstance(
            context: Context,
            dbPath: String,
            password: String? = null
        ): AndroidSqliteDriver {
            val sqliteDb = AndroidOpenHelper(context, dbPath, password).writableDatabase
            return AndroidSqliteDriver(sqliteDb)
        }

        fun newInstanceInMemory(context: Context, password: String? = null): AndroidSqliteDriver {
            val sqliteDb = AndroidOpenHelper(context, null, password).writableDatabase
            return AndroidSqliteDriver(sqliteDb)
        }
    }
}

private class AndroidOpenHelper(context: Context, dbName: String?, private val password: String?) :
    SQLiteOpenHelper(context, dbName, null, 1) {
    override fun onCreate(db: SQLiteDatabase) {}
    override fun onConfigure(db: SQLiteDatabase) {
        super.onConfigure(db)
        if (password != null) {
            val query = StringBuilder("PRAGMA key = ")
            DatabaseUtils.appendEscapedSQLString(query, password)
            db.execSQL(query.toString())
        }
        db.disableWriteAheadLogging()
        db.setForeignKeyConstraintsEnabled(true)
    }

    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {}
}
