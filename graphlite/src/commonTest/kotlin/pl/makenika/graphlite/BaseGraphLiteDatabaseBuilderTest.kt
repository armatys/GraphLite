package pl.makenika.graphlite

import pl.makenika.graphlite.sql.SqliteDriver
import kotlin.test.*

private const val InitialDbVersion = 0L

abstract class BaseGraphLiteDatabaseBuilderTest {
    private lateinit var driver: SqliteDriver
    private lateinit var tested: GraphLiteDatabaseBuilder

    protected abstract fun makeDriver(): SqliteDriver

    @BeforeTest
    fun setUp() {
        driver = makeDriver()
        tested = GraphLiteDatabaseBuilder(driver, InitialDbVersion)
    }

    @AfterTest
    fun tearDown() {
        driver.close()
    }

    @Test
    fun opensDatabase() {
        tested.open()
    }

    @Test
    fun registersSchema() {
        tested.register(InitialDbVersion, PersonV1, Likes).open()
    }

    @Test
    fun performsEmptyMigration() {
        tested.register(InitialDbVersion, PersonV1).open()

        var wasMigrationRun = false
        tested = GraphLiteDatabaseBuilder(driver, InitialDbVersion + 1)
        tested
            .register(InitialDbVersion, PersonV1)
            .register(InitialDbVersion + 1, PersonV2)
            .migration(InitialDbVersion, InitialDbVersion + 1) {
                wasMigrationRun = true
            }
            .open()
        assertTrue(wasMigrationRun)
    }

    @Test
    fun performsEmptyDowngradeMigration() {
        tested = GraphLiteDatabaseBuilder(driver, InitialDbVersion + 1)
        tested
            .register(InitialDbVersion, PersonV1)
            .register(InitialDbVersion + 1, PersonV2)
            .open()

        var wasMigrationRun = false
        tested = GraphLiteDatabaseBuilder(driver, InitialDbVersion)
        tested
            .register(InitialDbVersion, PersonV1)
            .register(InitialDbVersion + 1, PersonV2)
            .migration(InitialDbVersion + 1, InitialDbVersion) {
                wasMigrationRun = true
            }
            .open()
        assertTrue(wasMigrationRun)
    }

    @Test
    fun performsMigration() {
        val db1 = tested.register(InitialDbVersion, Likes, PersonV1).open()
        val a1 = db1.createNode("alice", PersonV1 { it[name] = "Alice Kowalski" })!!
        val j1 = db1.createNode("john", PersonV1 { it[name] = "John Doe" })!!
        val likesEdge = db1.createEdge(Likes())
        db1.connect(likesEdge.handle, a1.handle, j1.handle, false)

        tested = GraphLiteDatabaseBuilder(driver, InitialDbVersion + 1)
        val db2 = tested
            .register(InitialDbVersion, Likes, PersonV1)
            .register(InitialDbVersion + 1, LikesV2, PersonV2)
            .migration(InitialDbVersion, InitialDbVersion + 1) { db ->
                println("Migrating from $InitialDbVersion to ${InitialDbVersion + 1}")
                db.query(EdgeMatch(Likes)).forEach { edge ->
                    db.updateEdgeFields(edge, LikesV2 { it[level] = 100L })
                }

                db.query(NodeMatch(PersonV1)).forEach { node ->
                    val (fName, lName) = node { name }.split(" ")
                    db.updateNodeFields(
                        node,
                        PersonV2 { it[firstName] = fName; it[lastName] = lName })
                }
                println("Migrated")
                db.query(NodeMatch(PersonV1)).forEach { node ->
                    println("PersonV1 node: $node")
                }
                db.query(NodeMatch(PersonV2)).forEach { node ->
                    println("PersonV2 node: $node")
                }
            }
            .open()

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

    @Test(expected = IllegalStateException::class)
    fun valuesNotMigrated() {
        val db1 = tested.register(InitialDbVersion, PersonV1).open()
        db1.createNode("alice", PersonV1 { it[name] = "Alice Kowalski" })

        tested = GraphLiteDatabaseBuilder(driver, InitialDbVersion + 1)
        tested
            .register(InitialDbVersion, PersonV1)
            .register(InitialDbVersion + 1, PersonV2)
            .migration(InitialDbVersion, InitialDbVersion + 1) {
                // no-op
            }
            .open()
    }
}
