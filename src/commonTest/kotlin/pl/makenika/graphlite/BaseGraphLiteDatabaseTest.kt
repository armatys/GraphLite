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
        val resultByName = tested.query(NodeMatch(PersonV1, Where.handle(node.handle))).first()
        assertEquals(node.handle, resultByName.handle)
        assertEquals(node.fieldMap, resultByName.fieldMap)
        assertEquals(node, resultByName)
        assertEquals("John Doe", resultByName[PersonV1.name])
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
        val b = tested.getOrCreateNode("test", PersonV1 { it[name] = "Joe" })
        assertEquals(a, b)
    }

    @Test
    fun getOrCreateMissingNode() {
        val a = tested.getOrCreateNode("test", PersonV1 { it[name] = "Joe" })
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
        tested.createEdge("test", Likes {})!!
        val b = tested.createOrReplaceEdge("test", Likes {})

        val result = tested.query(EdgeMatch(Likes, Where.handle("test"))).first()
        assertEquals(b, result)
    }

    @Test
    fun createOrReplaceNode() {
        tested.createNode("test", Tree { it[name] = "a" })!!
        val b = tested.createOrReplaceNode("test", Tree { it[name] = "b" })

        val result = tested.query(NodeMatch(Tree, Where.handle("test"))).first()
        assertEquals(b, result)
    }

    @Test
    fun createOrReplaceMaintainsConnections() {
        val a = tested.createEdge("edge", Likes {})!!
        val b = tested.createNode("node", Tree { it[name] = "a" })!!
        tested.connect(a.handle, b.handle)

        val c = tested.createOrReplaceEdge("edge", Likes {})
        val d = tested.createOrReplaceNode("node", Tree { it[name] = "b" })
        val connection = tested.findConnection(c.handle, d.handle)
        assertNotNull(connection)
        assertEquals(c.handle, connection.edgeHandle)
        assertEquals(d.handle, connection.nodeHandle)
    }

    @Test
    fun deleteById() {
        val fieldMap = PersonV1 { it[name] = "John Doe" }
        val node = tested.createNode(fieldMap)
        assertTrue(tested.deleteNode(node.handle))
        assertNull(tested.query(NodeMatch(PersonV1, Where.handle(node.handle))).firstOrNull())
    }

    @Test
    fun deleteByName() {
        val fieldMap = PersonV1 { it[name] = "John Doe" }
        val node = tested.createNode("test", fieldMap)!!
        assertEquals("test", node.handle.value)
        assertTrue(tested.deleteNode(node.handle))
        assertNull(tested.query(NodeMatch(PersonV1, Where.handle(node.handle))).firstOrNull())
    }

    @Test
    fun connectWithMissingEdge() {
        val node = tested.createNode(PersonV1 { it[name] = "test" })
        assertThrows<RollbackException> {
            tested.connect(EdgeHandle(uuid4().toString()), node.handle, null)
        }
    }

    @Test
    fun connectWithMissingNode() {
        val edge = tested.createEdge(Likes())
        assertThrows<RollbackException> {
            tested.connect(edge.handle, NodeHandle(uuid4().toString()), null)
        }
    }

    @Test
    fun connectTwice() {
        val edge = tested.createEdge(Likes())
        val node = tested.createNode(Tree {})
        tested.connect(edge.handle, node.handle, null)
        assertThrows<RollbackException> {
            tested.connect(edge.handle, node.handle, null)
        }
    }

    @Test
    fun connectOrReplace() {
        val edge = tested.createEdge(Likes())
        val node = tested.createNode(Tree {})

        tested.connectOrReplace(edge.handle, node.handle, null)
        val connectionA = tested.findConnection(edge.handle, node.handle)
        assertNotNull(connectionA)
        assertEquals(connectionA.edgeHandle, edge.handle)
        assertEquals(connectionA.nodeHandle, node.handle)
        assertNull(connectionA.outgoing)

        tested.connectOrReplace(edge.handle, node.handle, true)
        val connectionB = tested.findConnection(edge.handle, node.handle)
        assertNotNull(connectionB)
        assertEquals(connectionB.edgeHandle, edge.handle)
        assertEquals(connectionB.nodeHandle, node.handle)
        assertEquals(connectionB.outgoing, true)
    }

    @Test
    fun getOrConnect() {
        val edge = tested.createEdge(Likes())
        val node = tested.createNode(Tree {})

        val connectionA = tested.getOrConnect(edge.handle, node.handle)
        val connectionB = tested.getOrConnect(edge.handle, node.handle, true)
        assertEquals(connectionA, connectionB)
    }

    @Test
    fun connectTwoNodes() {
        val a = tested.createNode(PersonV1 { it[name] = "a" })
        val b = tested.createNode(PersonV1 { it[name] = "b" })
        val edge = tested.createEdge(Likes())
        tested.connect(edge.handle, a.handle, b.handle, true)

        val allEdges =
            tested.query(NodeMatch(PersonV1, Where.handle(a.handle)).via(EdgeMatch(Likes))).toList()
        assertEquals(1, allEdges.size)
        assertEquals(edge, allEdges[0])

        val incomingEdges =
            tested.query(NodeMatch(PersonV1, Where.handle(a.handle)).incoming(EdgeMatch(Likes)))
                .toList()
        assertEquals(0, incomingEdges.size)

        val outgoingEdges =
            tested.query(NodeMatch(PersonV1, Where.handle(a.handle)).outgoing(EdgeMatch(Likes)))
                .toList()
        assertEquals(1, outgoingEdges.size)
        assertEquals(edge, outgoingEdges[0])
    }

    @Test
    fun disconnectUnrelatedElements() {
        val e = tested.createEdge(Likes())
        val n = tested.createNode(Tree())
        assertFalse(tested.disconnect(e.handle, n.handle))
    }

    @Test
    fun disconnectElements() {
        val e = tested.createEdge(Likes())
        val n = tested.createNode(Tree())
        val connection = tested.connect(e.handle, n.handle)
        assertEquals(e.handle, connection.edgeHandle)
        assertEquals(n.handle, connection.nodeHandle)
        assertNull(connection.outgoing)
        assertTrue(tested.disconnect(e.handle, n.handle))
    }

    @Test
    fun updateFieldValue() {
        val node = tested.createNode(PersonV1 { it[name] = "John Doe" })
        val updated = tested.updateNodeField(node, PersonV1.name, "John F. Doe")
        assertEquals("John F. Doe", updated[PersonV1.name])
    }

    @Test
    fun updateNonExistentNode() {
        val node = Node(NodeHandle("test"), PersonV1 { it[name] = "John Doe" })
        assertThrows<RollbackException> {
            tested.updateNodeField(node, PersonV1.name, "test value")
        }
    }

    @Test
    fun updateFieldValues() {
        val node = tested.createNode(Tree { it[name] = "Oak"; it[secret] = byteArrayOf(0x7, 0x9) })
        val updated = tested.updateNodeFields(node, node.edit { it[age] = 102; it[secret] = null })
        assertEquals("Oak", updated[Tree.name])
        assertEquals(102, updated[Tree.age])
        assertNull(updated[Tree.diameter])
        assertNull(updated[Tree.location])
        assertNull(updated[Tree.secret])
    }

    @Test
    fun updateFieldValuesByHandle() {
        val node = tested.createNode(Tree { it[name] = "Oak"; it[secret] = byteArrayOf(0x7, 0x9) })
        val updated =
            tested.updateNodeFields(node.handle, node.edit { it[age] = 102; it[secret] = null })
        assertEquals("Oak", updated[Tree.name])
        assertEquals(102, updated[Tree.age])
        assertNull(updated[Tree.diameter])
        assertNull(updated[Tree.location])
        assertNull(updated[Tree.secret])
    }

    @Test
    fun changeSchema() {
        val a = tested.createEdge(Likes())
        val b = tested.updateEdgeFields(a, Loves())
        val c = tested.query(EdgeMatch(Loves, Where.handle(b.handle))).first()
        assertEquals(a.handle, b.handle)
        assertEquals(b, c)
    }

    @Test
    fun getConnections() {
        val a = tested.createNode(PersonV1 { it[name] = "Jane" })
        val b = tested.createNode(PersonV1 { it[name] = "Kim" })
        val c = tested.createNode(PersonV1 { it[name] = "Luke" })
        val e = tested.createEdge(Likes())
        tested.connect(e.handle, a.handle)
        tested.connect(e.handle, b.handle, true)
        tested.connect(e.handle, c.handle)

        val aConnections = tested.getConnections(a.handle).toList()
        assertEquals(1, aConnections.size)
        assertEquals(e.handle, aConnections.first().edgeHandle)
        assertEquals(a.handle, aConnections.first().nodeHandle)
        assertEquals(null, aConnections.first().outgoing)

        val bConnections = tested.getConnections(b.handle).toList()
        assertEquals(1, bConnections.size)
        assertEquals(e.handle, bConnections.first().edgeHandle)
        assertEquals(b.handle, bConnections.first().nodeHandle)
        assertEquals(true, bConnections.first().outgoing)

        val cConnections = tested.getConnections(c.handle).toList()
        assertEquals(1, cConnections.size)
        assertEquals(e.handle, cConnections.first().edgeHandle)
        assertEquals(c.handle, cConnections.first().nodeHandle)
        assertEquals(null, cConnections.first().outgoing)

        val nodeNames = listOf(a.handle, b.handle, c.handle)
        val outgoings = listOf(null, true, null)

        val eConnections = tested.getConnections(e.handle).toList()
        assertEquals(3, eConnections.size)
        for (i in 0 until 3) {
            val eConn = eConnections.first { it.nodeHandle == nodeNames[i] }
            assertEquals(e.handle, eConn.edgeHandle)
            assertEquals(nodeNames[i], eConn.nodeHandle)
            assertEquals(outgoings[i], eConn.outgoing)
        }
    }

    @Test
    fun findSchemaForMissingElement() {
        assertNull(tested.findEdgeSchema(EdgeHandle(uuid4().toString())))
        assertNull(tested.findNodeSchema(NodeHandle(uuid4().toString())))
    }

    @Test
    fun getSchema() {
        val a = tested.createNode(Tree { it[name] = "Oak" })
        val s = tested.findNodeSchema(a.handle)
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

    @Test
    fun fieldValidatorPass() {
        val node = tested.createNode(PersonV1 { it[name] = "John Doe" })
        assertEquals("John Doe", node { name })
    }

    @Test
    fun fieldValidatorFail() {
        assertThrows<RollbackException> {
            tested.createNode(PersonV1 { it[name] = "  " })
        }
    }

    @Test
    fun duplicateValidators() {
        assertThrows<IllegalStateException> {
            object : Schema("test", 1) {
                @Suppress("unused")
                val name = textField("n")
                    .onValidate { it.isNotEmpty() }
                    .onValidate { it.startsWith("A") }
            }
        }
    }

    @Test
    fun complexValidationPass() {
        tested.createNode(Tree {
            it[age] = 100
            it[secret] = byteArrayOf(0x64)
        })
    }

    @Test
    fun complexValidationFail() {
        assertThrows<RollbackException> {
            tested.createNode(Tree {
                it[age] = 99
                it[secret] = byteArrayOf(0x64)
            })
        }
    }

    @Test
    fun singleFieldDbValidationFail() {
        val node = tested.createNode(Tree {
            it[age] = 99
            it[secret] = byteArrayOf(0x60)
        })

        assertThrows<RollbackException> {
            val localNode = Node(node.handle, node.toMutableFieldMap().edit { it[age] = 100 })
            tested.updateNodeField(localNode, Tree.secret, byteArrayOf(0x64))
            // Setting the `secret` to [0x64] requires the `age` to be equal to 100.
            // While the local value is equal to that value, the value stored
            // in the database is `99`, so the update should fail.
        }

        val freshNode = tested.query(NodeMatch(Tree, Where.handle(node.handle))).first()
        assertEquals(99, freshNode { age })
        assertTrue(byteArrayOf(0x60).contentEquals(freshNode { secret }))
    }

    @Test
    fun singleFieldLocalValidationFail() {
        val edge = tested.createEdge(Tree {
            it[age] = 100
            it[secret] = byteArrayOf(0x60)
        })

        assertThrows<RollbackException> {
            val localEdge = Edge(edge.handle, edge.toMutableFieldMap().edit { it[age] = 99 })
            tested.updateEdgeField(localEdge, Tree.secret, byteArrayOf(0x64))
            // Setting the `secret` to [0x64] requires the `age` to be equal to 100.
            // Even though the value in the database is equal to 100,
            // the update fails, since the local value is unexpected.
        }

        val freshEdge = tested.query(EdgeMatch(Tree, Where.handle(edge.handle))).first()
        assertEquals(100, freshEdge { age })
        assertTrue(byteArrayOf(0x60).contentEquals(freshEdge { secret }))
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
