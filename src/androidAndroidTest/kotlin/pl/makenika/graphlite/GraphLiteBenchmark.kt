package pl.makenika.graphlite

import android.app.Application
import androidx.benchmark.junit4.BenchmarkRule
import androidx.benchmark.junit4.measureRepeated
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.runBlocking
import pl.makenika.graphlite.sql.AndroidSqliteDriver
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.random.Random

@RunWith(AndroidJUnit4::class)
class GraphLiteBenchmark {
    @get:Rule
    val benchmarkRule = BenchmarkRule()

    private lateinit var tested: GraphLiteDatabase

    @Before
    fun setUp() = runBlocking {
        val context = ApplicationProvider.getApplicationContext<Application>()
        val driver = AndroidSqliteDriver.newInstanceInMemory(context)
        tested = GraphLiteDatabaseBuilder(driver)
            .register(PersonV1)
            .open()
    }

    @After
    fun tearDown() {
        tested.close()
    }

    @Test
    fun insertPeople() = benchmarkRule.measureRepeated {
        runBlocking {
            val personName = Random.nextInt().toString()
            tested.createNode(PersonV1 { it[name] = personName })
        }
    }

    @Test
    fun findByName() {
        benchmarkRule.measureRepeated {
            runBlocking {
                val i = Random.nextInt()
                tested.query(NodeMatch(PersonV1, Where.handle("$i"))).firstOrNull()
            }
        }
    }

    @Test
    fun loadNode() {
        val node = runBlocking {
            tested.createNode(PersonV1 { it[name] = "John Doe" })
        }
        benchmarkRule.measureRepeated {
            runBlocking { tested.query(NodeMatch(PersonV1, Where.handle(node.handle))).first() }
        }
    }

    @Test
    fun deleteByName() {
        benchmarkRule.measureRepeated {
            runBlocking {
                val i = Random.nextInt()
                tested.deleteNode(NodeHandle("$i"))
            }
        }
    }
}