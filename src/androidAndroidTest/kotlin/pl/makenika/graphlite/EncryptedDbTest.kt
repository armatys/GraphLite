package pl.makenika.graphlite

import android.app.Application
import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import pl.makenika.graphlite.sql.AndroidSqliteDriver
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.sqlite.database.sqlite.SQLiteDatabaseCorruptException

@RunWith(AndroidJUnit4::class)
class EncryptedDbTest {
    companion object {
        private const val dbName = "test.sqlite3"
    }

    private lateinit var context: Context

    @Before
    fun setUp() {
        context = ApplicationProvider.getApplicationContext<Application>()
        context.deleteDatabase(dbName)
    }

    @After
    fun tearDown() {
        context.deleteDatabase(dbName)
    }

    @Test
    fun correctPassword() {
        val a = makeGraphDb("test-password'")
        a.close()
        val b = makeGraphDb("test-password'")
        b.close()
    }

    @Test(expected = SQLiteDatabaseCorruptException::class)
    fun incorrectPassword() {
        val a = makeGraphDb("mySecret")
        a.close()
        val b = makeGraphDb("myNoSecret")
        b.close()
    }

    private fun makeGraphDb(password: String): GraphLiteDatabase {
        val dbPath = context.getDatabasePath(dbName).path
        val driver = AndroidSqliteDriver.newInstance(context, dbPath, password)
        GraphLiteDatabaseBuilder(driver).register(Animal).open()
    }
}
