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

internal abstract class SqliteDriverHelper constructor(private val driver: SqliteDriver, private val version: Long) {
    fun open(): SqliteDriver {
        onConfigure(driver)
        when (val currentDbVersion = driver.getDbVersion()) {
            null -> {
                driver.initialize(version)
                onCreate(driver)
            }
            else -> {
                migrateIfNeeded(driver, currentDbVersion, version)
                driver.updateVersion(version)
            }
        }
        onOpen(driver)
        return driver
    }

    private fun migrateIfNeeded(driver: SqliteDriver, currentDbVersion: Long, newVersion: Long) {
        if (currentDbVersion == newVersion) {
            return
        }
        if (newVersion < currentDbVersion) {
            error("Requested database version ($newVersion) is older than the current version ($currentDbVersion).")
        }

        for (v in (currentDbVersion + 1)..newVersion) {
            onUpgrade(driver, v - 1, v)
        }
    }

    open fun onConfigure(driver: SqliteDriver) {}
    abstract fun onCreate(driver: SqliteDriver)
    abstract fun onUpgrade(driver: SqliteDriver, oldVersion: Long, newVersion: Long)
    open fun onOpen(driver: SqliteDriver) {}
}
