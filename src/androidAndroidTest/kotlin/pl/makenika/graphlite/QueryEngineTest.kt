package pl.makenika.graphlite

import android.app.Application
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runBlockingTest
import pl.makenika.graphlite.sql.AndroidSqliteDriver
import pl.makenika.graphlite.sql.SqliteDriver
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class QueryEngineTest : BaseQueryEngineTest() {
    @OptIn(ExperimentalCoroutinesApi::class)
    override fun blocking(fn: suspend () -> Unit) {
        runBlockingTest { fn() }
    }

    override fun makeDriver(): SqliteDriver {
        val context = ApplicationProvider.getApplicationContext<Application>()
        return AndroidSqliteDriver.newInstanceInMemory(context)
    }
}
