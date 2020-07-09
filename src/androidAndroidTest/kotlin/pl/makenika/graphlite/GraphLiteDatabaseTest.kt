package pl.makenika.graphlite

import android.app.Application
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import pl.makenika.graphlite.sql.AndroidSqliteDriver
import pl.makenika.graphlite.sql.SqliteDriver
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
internal class GraphLiteDatabaseTest : BaseGraphLiteDatabaseTest() {
    override fun makeDriver(): SqliteDriver {
        val context = ApplicationProvider.getApplicationContext<Application>()
        return AndroidSqliteDriver.newInstanceInMemory(context)
    }
}
