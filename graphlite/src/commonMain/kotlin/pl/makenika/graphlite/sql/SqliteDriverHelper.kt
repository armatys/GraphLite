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

internal object SqliteDriverHelper {
    operator fun invoke(
        driver: SqliteDriver,
        version: Long,
        onConfigure: ((SqliteDriver) -> Unit)? = null,
        onCreate: ((SqliteDriver) -> Unit)? = null,
        onOpen: ((SqliteDriver) -> Unit)? = null,
        onUpgrade: ((SqliteDriver, Long, Long) -> Unit)? = null
    ) {
        onConfigure?.invoke(driver)
        when (val currentDbVersion = driver.getSqlSchemaVersion()) {
            null -> {
                driver.initialize(version)
                onCreate?.invoke(driver)
            }
            else -> {
                migrateIfNeeded(driver, currentDbVersion, version, onUpgrade)
                driver.updateSqlSchemaVersion(version)
            }
        }
        onOpen?.invoke(driver)
    }

    private fun migrateIfNeeded(
        driver: SqliteDriver,
        currentDbVersion: Long,
        newVersion: Long,
        onUpgrade: ((SqliteDriver, Long, Long) -> Unit)?
    ) {
        if (currentDbVersion == newVersion) {
            return
        }
        if (newVersion < currentDbVersion) {
            error("Requested database version ($newVersion) is older than the current version ($currentDbVersion).")
        }

        for (v in (currentDbVersion + 1)..newVersion) {
            onUpgrade?.invoke(driver, v - 1, v)
        }
    }
}
