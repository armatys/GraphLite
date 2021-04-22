package pl.makenika.graphlite

import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import pl.makenika.graphlite.sql.SqliteDriver
import kotlin.test.*

abstract class BaseGraphLiteDatabaseBuilderTest {
    private lateinit var driver: SqliteDriver
    private lateinit var tested: GraphLiteDatabaseBuilder

    protected abstract fun blocking(fn: suspend () -> Unit)
    protected abstract fun makeDriver(): SqliteDriver

    @BeforeTest
    fun setUp() {
        driver = makeDriver()
        tested = GraphLiteDatabaseBuilder(driver)
    }

    @AfterTest
    fun tearDown() {
        driver.close()
    }

    @Test
    fun opensDatabase() = blocking {
        tested.open()
    }

    @Test
    fun registersSchema() = blocking {
        tested.register(PersonV1).open()
    }

    @Test
    fun performsEmptyMigration() = blocking {
        tested.register(PersonV1).open()
        var wasMigrationRun = false
        tested = GraphLiteDatabaseBuilder(driver)
        tested
            .register(PersonV2)
            .migration(PersonV1, PersonV2) {
                wasMigrationRun = true
            }
            .open()
        assertTrue(wasMigrationRun)
    }

    @Test
    fun performsMigration() = blocking {
        val db1 = tested.register(Likes).register(PersonV1).open()
        val a1 = db1.createNode("alice", PersonV1 { it[name] = "Alice Kowalski" })!!
        val j1 = db1.createNode("john", PersonV1 { it[name] = "John Doe" })!!
        val likesEdge = db1.createEdge(Likes())
        db1.connect(likesEdge.handle, a1.handle, j1.handle, false)
        assertEquals(PersonV1, db1.findNodeSchema(a1.handle))
        assertEquals(PersonV1, db1.findNodeSchema(j1.handle))

        tested = GraphLiteDatabaseBuilder(driver)
        val db2 = tested
            .register(LikesV2)
            .register(PersonV2)
            .migration(Likes, LikesV2) { db ->
                db.query(EdgeMatch(Likes)).collect { edge ->
                    db.updateEdgeFields(edge, LikesV2 { it[level] = 100L })
                }
            }
            .migration(PersonV1, PersonV2) { db ->
                db.query(NodeMatch(PersonV1)).collect { node ->
                    val (fName, lName) = node { name }.split(" ")
                    db.updateNodeFields(
                        node,
                        PersonV2 { it[firstName] = fName; it[lastName] = lName })
                }
            }
            .open()

        assertEquals(PersonV2, db2.findNodeSchema(a1.handle))
        assertEquals(PersonV2, db2.findNodeSchema(j1.handle))

        val a2 = db2.query(NodeMatch(PersonV2, Where.handle("alice"))).first()
        assertEquals(a1.handle, a2.handle)
        assertEquals("Alice", a2 { firstName })
        assertEquals("Kowalski", a2 { lastName })

        val j2 = db2.query(NodeMatch(PersonV2, Where.handle("john"))).first()
        assertEquals(j1.handle, j2.handle)
        assertEquals("John", j2 { firstName })
        assertEquals("Doe", j2 { lastName })

        val aliceConnections = db2.getConnections(a2.handle).toList()
        assertEquals(1, aliceConnections.size)
        assertEquals(likesEdge.handle, aliceConnections.first().edgeHandle)
        assertEquals(a2.handle, aliceConnections.first().nodeHandle)
        assertEquals(null, aliceConnections.first().outgoing)

        val johnConnections = db2.getConnections(j2.handle).toList()
        assertEquals(1, johnConnections.size)
        assertEquals(likesEdge.handle, johnConnections.first().edgeHandle)
        assertEquals(j2.handle, johnConnections.first().nodeHandle)
        assertEquals(null, johnConnections.first().outgoing)
    }
}
