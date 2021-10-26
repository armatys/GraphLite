package pl.makenika.graphlite

import pl.makenika.graphlite.sql.JvmSqliteDriver
import pl.makenika.graphlite.sql.SqliteDriver

class GraphLiteDatabaseBuilderTest : BaseGraphLiteDatabaseBuilderTest() {
    override fun makeDriver(): SqliteDriver {
        return JvmSqliteDriver.newInstanceInMemory()
    }
}
