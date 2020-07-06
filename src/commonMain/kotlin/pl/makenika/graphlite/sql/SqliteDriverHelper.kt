package pl.makenika.graphlite.sql

internal abstract class SqliteDriverHelper constructor(private val driver: SqliteDriver, private val version: Int) {
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

    private fun migrateIfNeeded(driver: SqliteDriver, currentDbVersion: Int, newVersion: Int) {
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
    abstract fun onUpgrade(driver: SqliteDriver, oldVersion: Int, newVersion: Int)
    open fun onOpen(driver: SqliteDriver) {}
}
