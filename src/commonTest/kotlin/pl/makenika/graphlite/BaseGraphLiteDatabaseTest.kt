package pl.makenika.graphlite

import com.benasher44.uuid.uuid4
import pl.makenika.graphlite.sql.RollbackException
import pl.makenika.graphlite.sql.SqliteDriver
import kotlin.test.*

abstract class BaseGraphLiteDatabaseTest {
    private lateinit var tested: GraphLiteDatabase

    protected abstract fun makeDriver(): SqliteDriver

    @BeforeTest
    fun setUp() {
        val driver = makeDriver()
        tested = GraphLiteDatabaseBuilder(driver)
            .register(Animal)
            .register(Likes)
            .register(Loves)
            .register(PersonV1)
            .register(Tree)
            .open()
    }

    @AfterTest
    fun tearDown() {
        tested.close()
    }

    @Test
    fun createAndFindNode() {
        val fieldMap = PersonV1 { it[name] = "John Doe" }
        val node = tested.createNode(fieldMap)
        val resultById = tested.query(NodeMatch(PersonV1, Where.id(node.id))).first()
        val resultByName = tested.query(NodeMatch(PersonV1, Where.name(node.name))).first()
        assertNotNull(resultById)
        assertEquals(resultById, resultByName)
        assertEquals(node.id, resultById.id)
        assertEquals(node.name, resultById.name)
        assertEquals(node.fieldMap, resultById.fieldMap)
        assertEquals(node, resultById)
        assertEquals("John Doe", resultById[PersonV1.name])
    }

    @Test
    fun createNodeWithName() {
        val node = tested.createNode("test", PersonV1 { it[name] = "Alice" })
        assertNotNull(node)
        assertEquals("Alice", node { name })
    }

    @Test
    fun getOrCreateExistingNode() {
        val a = tested.createNode("test", PersonV1 { it[name] = "Jane" })
        val b = tested.getOrCreateNode(PersonV1, "test") { PersonV1 { it[name] = "Joe" } }
        assertEquals(a, b)
    }

    @Test
    fun getOrCreateMissingNode() {
        val a = tested.getOrCreateNode(PersonV1, "test") { PersonV1 { it[name] = "Joe" } }
        assertEquals("Joe", a[PersonV1.name])
    }

    @Test
    fun createDuplicateNode() {
        val fieldMap = PersonV1 { it[name] = "John Doe" }
        val a = tested.createNode("test", fieldMap)
        assertNotNull(a)

        val b = tested.createNode("test", fieldMap)
        assertNull(b)
    }

    @Test
    fun createDuplicateNodeWithDifferentSchema() {
        val a = tested.createNode("test", PersonV1 { it[name] = "John Doe" })
        assertNotNull(a)

        val b = tested.createNode("test", Animal { it[name] = "Dog Joe" })
        assertNull(b)
    }

    @Test
    fun createOrReplaceEdge() {
        val a = tested.createEdge("test", Likes {})!!
        val b = tested.createOrReplaceEdge("test", Likes {})

        assertNotEquals(a.id, b.id)

        val result = tested.query(EdgeMatch(Likes, Where.name("test"))).first()
        assertEquals(b, result)
    }

    @Test
    fun createOrReplaceNode() {
        val a = tested.createNode("test", Tree { it[name] = "a" })!!
        val b = tested.createOrReplaceNode("test", Tree { it[name] = "b" })

        assertNotEquals(a.id, b.id)

        val result = tested.query(NodeMatch(Tree, Where.name("test"))).first()
        assertEquals(b, result)
    }

    @Test
    fun createOrReplaceMaintainsConnections() {
        val a = tested.createEdge("edge", Likes {})!!
        val b = tested.createNode("node", Tree { it[name] = "a" })!!
        tested.connect(a.name, b.name)

        val c = tested.createOrReplaceEdge("edge", Likes {})
        val d = tested.createOrReplaceNode("node", Tree { it[name] = "b" })
        val connection = tested.findConnection(c.name, d.name)
        assertNotNull(connection)
        assertEquals(c.name, connection.edgeName)
        assertEquals(d.name, connection.nodeName)
    }

    @Test
    fun deleteById() {
        val fieldMap = PersonV1 { it[name] = "John Doe" }
        val node = tested.createNode(fieldMap)
        assertTrue(tested.deleteNode(node.id))
        assertNull(tested.query(NodeMatch(PersonV1, Where.id(node.id))).firstOrNull())
    }

    @Test
    fun deleteByName() {
        val fieldMap = PersonV1 { it[name] = "John Doe" }
        val node = tested.createNode("test", fieldMap)!!
        assertEquals("test", node.name)
        assertTrue(tested.deleteNode(node.name))
        assertNull(tested.query(NodeMatch(PersonV1, Where.id(node.id))).firstOrNull())
    }

    @Test
    fun connectWithMissingEdge() {
        val node = tested.createNode(PersonV1 { it[name] = "test" })
        assertThrows<RollbackException> {
            tested.connect(uuid4().toString(), node.name, null)
        }
    }

    @Test
    fun connectWithMissingNode() {
        val edge = tested.createEdge(Likes())
        assertThrows<RollbackException> {
            tested.connect(edge.name, uuid4().toString(), null)
        }
    }

    @Test
    fun connectTwice() {
        val edge = tested.createEdge(Likes())
        val node = tested.createNode(Tree {})
        tested.connect(edge.name, node.name, null)
        assertThrows<RollbackException> {
            tested.connect(edge.name, node.name, null)
        }
    }

    @Test
    fun connectOrReplace() {
        val edge = tested.createEdge(Likes())
        val node = tested.createNode(Tree {})

        tested.connectOrReplace(edge.name, node.name, null)
        val connectionA = tested.findConnection(edge.name, node.name)
        assertNotNull(connectionA)
        assertEquals(connectionA.edgeName, edge.name)
        assertEquals(connectionA.nodeName, node.name)
        assertNull(connectionA.outgoing)

        tested.connectOrReplace(edge.name, node.name, true)
        val connectionB = tested.findConnection(edge.name, node.name)
        assertNotNull(connectionB)
        assertEquals(connectionB.edgeName, edge.name)
        assertEquals(connectionB.nodeName, node.name)
        assertEquals(connectionB.outgoing, true)
    }

    @Test
    fun getOrConnect() {
        val edge = tested.createEdge(Likes())
        val node = tested.createNode(Tree {})

        val connectionA = tested.getOrConnect(edge.name, node.name)
        val connectionB = tested.getOrConnect(edge.name, node.name, true)
        assertEquals(connectionA, connectionB)
    }

    @Test
    fun connectTwoNodes() {
        val a = tested.createNode(PersonV1 { it[name] = "a" })
        val b = tested.createNode(PersonV1 { it[name] = "b" })
        val edge = tested.createEdge(Likes())
        tested.connect(edge.name, a.name, b.name, true)

        val allEdges =
            tested.query(NodeMatch(PersonV1, Where.id(a.id)).via(EdgeMatch(Likes))).toList()
        assertEquals(1, allEdges.size)
        assertEquals(edge, allEdges[0])

        val incomingEdges =
            tested.query(NodeMatch(PersonV1, Where.id(a.id)).incoming(EdgeMatch(Likes))).toList()
        assertEquals(0, incomingEdges.size)

        val outgoingEdges =
            tested.query(NodeMatch(PersonV1, Where.id(a.id)).outgoing(EdgeMatch(Likes))).toList()
        assertEquals(1, outgoingEdges.size)
        assertEquals(edge, outgoingEdges[0])
    }

    @Test
    fun disconnectUnrelatedElements() {
        val e = tested.createEdge(Likes())
        val n = tested.createNode(Tree())
        assertFalse(tested.disconnect(e.name, n.name))
    }

    @Test
    fun disconnectElements() {
        val e = tested.createEdge(Likes())
        val n = tested.createNode(Tree())
        val connection = tested.connect(e.name, n.name)
        assertEquals(e.name, connection.edgeName)
        assertEquals(n.name, connection.nodeName)
        assertNull(connection.outgoing)
        assertTrue(tested.disconnect(e.name, n.name))
    }

    @Test
    fun updateFieldValue() {
        val node = tested.createNode(PersonV1 { it[name] = "John Doe" })
        val updated = tested.updateField(node, PersonV1.name, "John F. Doe")
        assertEquals("John F. Doe", updated[PersonV1.name])
    }

    @Test
    fun updateNonExistentNode() {
        val node = Node(NodeIdImpl(uuid4().toString()), "test", PersonV1 { it[name] = "John Doe" })
        assertThrows<RollbackException> {
            tested.updateField(node, PersonV1.name, "test value")
        }
    }

    @Test
    fun updateFieldValues() {
        val node = tested.createNode(Tree { it[name] = "Oak"; it[secret] = byteArrayOf(0x7, 0x9) })
        val updated = tested.updateFields(node, node.edit { it[age] = 102; it[secret] = null })
        assertEquals("Oak", updated[Tree.name])
        assertEquals(102, updated[Tree.age])
        assertNull(updated[Tree.diameter])
        assertNull(updated[Tree.location])
        assertNull(updated[Tree.secret])
    }

    @Test
    fun changeSchema() {
        val a = tested.createEdge(Likes())
        val b = tested.updateFields(a, Loves())
        val c = tested.query(EdgeMatch(Loves, Where.id(b.id))).first()
        assertNotEquals(a.id, b.id)
        assertEquals(a.name, b.name)
        assertEquals(b, c)
    }

    @Test
    fun getConnections() {
        val a = tested.createNode(PersonV1 { it[name] = "Jane" })
        val b = tested.createNode(PersonV1 { it[name] = "Kim" })
        val c = tested.createNode(PersonV1 { it[name] = "Luke" })
        val e = tested.createEdge(Likes())
        tested.connect(e.name, a.name)
        tested.connect(e.name, b.name, true)
        tested.connect(e.name, c.name)

        val aConnections = tested.getConnections(a.name).toList()
        assertEquals(1, aConnections.size)
        assertEquals(e.name, aConnections.first().edgeName)
        assertEquals(a.name, aConnections.first().nodeName)
        assertEquals(null, aConnections.first().outgoing)

        val bConnections = tested.getConnections(b.name).toList()
        assertEquals(1, bConnections.size)
        assertEquals(e.name, bConnections.first().edgeName)
        assertEquals(b.name, bConnections.first().nodeName)
        assertEquals(true, bConnections.first().outgoing)

        val cConnections = tested.getConnections(c.name).toList()
        assertEquals(1, cConnections.size)
        assertEquals(e.name, cConnections.first().edgeName)
        assertEquals(c.name, cConnections.first().nodeName)
        assertEquals(null, cConnections.first().outgoing)

        val nodeNames = listOf(a.name, b.name, c.name)
        val outgoings = listOf(null, true, null)

        val eConnections = tested.getConnections(e.name).toList()
        assertEquals(3, eConnections.size)
        for (i in 0 until 3) {
            val eConn = eConnections.first { it.nodeName == nodeNames[i] }
            assertEquals(e.name, eConn.edgeName)
            assertEquals(nodeNames[i], eConn.nodeName)
            assertEquals(outgoings[i], eConn.outgoing)
        }
    }

    @Test
    fun findSchemaForMissingElement() {
        assertNull(tested.findEdgeSchema(EdgeIdImpl(uuid4().toString())))
        assertNull(tested.findNodeSchema(NodeIdImpl(uuid4().toString())))
    }

    @Test
    fun getSchema() {
        val a = tested.createNode(Tree { it[name] = "Oak" })
        val s = tested.findNodeSchema(a.id)
        assertEquals(Tree, s)
    }

    @Test
    fun createNodesInTransaction() {
        tested.transaction {
            createNode(Tree { it[name] = "Oak" })
            createNode(Tree { it[name] = "Willow" })
        }
        val trees = tested.query(NodeMatch(Tree)).toList().map { it { name } }.toSet()
        assertEquals(2, trees.size)
        assertEquals(setOf("Oak", "Willow"), trees)
    }

    @Test
    fun createNodesInFailedTransaction() {
        var exceptionCaught = false
        try {
            tested.transaction {
                val oak = createNode(Tree { it[name] = "Oak" })
                if (oak { name } == "Oak") error("failed")
                createNode(Tree { it[name] = "Willow" })
            }
        } catch (e: IllegalStateException) {
            exceptionCaught = true
        }
        assertTrue(exceptionCaught)

        val trees = tested.query(NodeMatch(Tree)).toSet()
        assertEquals(0, trees.size)
    }

    private inline fun <reified T : Throwable> assertThrows(fn: () -> Unit) {
        var exceptionCaught = false
        try {
            fn()
        } catch (t: Throwable) {
            if (t::class == T::class) {
                exceptionCaught = true
            } else {
                throw t
            }
        }
        assertTrue(exceptionCaught, "Expected ${T::class.qualifiedName} to be thrown.")
    }
}
