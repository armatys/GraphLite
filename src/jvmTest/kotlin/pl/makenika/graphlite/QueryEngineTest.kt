package pl.makenika.graphlite

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runBlockingTest
import pl.makenika.graphlite.sql.JvmSqliteDriver
import pl.makenika.graphlite.sql.SqliteDriver

class QueryEngineTest : BaseQueryEngineTest() {
    @OptIn(ExperimentalCoroutinesApi::class)
    override fun blocking(fn: suspend () -> Unit) {
        runBlockingTest { fn() }
    }

    override fun makeDriver(): SqliteDriver {
        return JvmSqliteDriver.newInstanceInMemory()
    }
}
