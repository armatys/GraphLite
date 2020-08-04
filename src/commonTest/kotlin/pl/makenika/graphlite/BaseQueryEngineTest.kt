package pl.makenika.graphlite

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import pl.makenika.graphlite.sql.SqliteDriver
import kotlin.test.*

abstract class BaseQueryEngineTest {
    private lateinit var tested: GraphLiteDatabase

    protected abstract fun blocking(fn: suspend () -> Unit)
    protected abstract fun makeDriver(): SqliteDriver

    @BeforeTest
    fun setUp() = blocking {
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
    fun schemaQuery() = blocking {
        val p1 = tested.createNode(PersonV1 { it[name] = "John Doe" })
        val p2 = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val m = NodeMatch(PersonV1)
        val people = tested.query(m).toList()
        assertEquals(2, people.size)
        assertEquals(p1, people[0])
        assertEquals(p2, people[1])
    }

    @Test
    fun byNameQuery() = blocking {
        tested.createNode(PersonV1 { it[name] = "John Doe" })
        val p1 = tested.createNode("p2", PersonV1 { it[name] = "Jane Smith" })
        val m = NodeMatch(PersonV1, Where.handle("p2"))
        val people = tested.query(m).toList()
        assertEquals(1, people.size)
        assertEquals(p1, people[0])
    }

    @Test
    fun byEqAndHandleQuery() = blocking {
        val t1 = tested.createNode("t1", Tree { it[name] = "Oak"; it[age] = 101 })!!
        val m = NodeMatch(Tree, Where.and(Where.eq(Tree.age, 101), Where.handle("t1")))
        val list = tested.query(m).toList()
        assertEquals(1, list.size)
        assertEquals(t1, list[0])
    }

    @Test
    fun byIdOrNameQuery() = blocking {
        val p1 = tested.createNode("p1", PersonV1 { it[name] = "Jane Smith" })
        val p2 = tested.createNode("p2", PersonV1 { it[name] = "John Doe" })
        assertNotNull(p2)

        val m = NodeMatch(PersonV1, Where.or(Where.handle("p1"), Where.handle("p2")))
        val people = tested.query(m).toList()

        assertEquals(2, people.size)
        assertEquals(p1, people[0])
        assertEquals(p2, people[1])
    }

    @Test
    fun byGeoValueQuery() = blocking {
        val bounds = GeoBounds(1.0, 1.02, -5.5, -5.4)
        val tree = tested.createNode(Tree { it[location] = bounds })
        val m = NodeMatch(Tree, Where.eq(Tree.location, bounds))
        val trees = tested.query(m).toList()
        assertEquals(1, trees.size)
        assertEquals(tree, trees[0])
    }

    @Test
    fun byStringValueQuery() = blocking {
        tested.createNode(PersonV1 { it[name] = "John Doe" })
        val p1 = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val p2 = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val m = NodeMatch(PersonV1, Where.eq(PersonV1.name, "Jane Smith"))
        val people = tested.query(m).toSet()
        assertEquals(setOf(p1, p2), people)
    }

    @Test
    fun byFtsQuery() = blocking {
        tested.createNode(PersonV1 { it[name] = "John Doe" })
        val p1 = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val p2 = tested.createNode(PersonV1 { it[name] = "Charlie Smith" })
        val m = NodeMatch(PersonV1, Where.fts(PersonV1.name, "Smith"))
        val people = tested.query(m).toSet()
        assertEquals(setOf(p1, p2), people)
    }

    @Test
    fun byIntBetweenQuery() = blocking {
        tested.createNode(Tree { it[age] = 42 })
        val t2 = tested.createNode(Tree { it[age] = 128 })
        val m = NodeMatch(Tree, Where.between(Tree.age, 100, 130))
        val trees = tested.query(m).toList()
        assertEquals(1, trees.size)
        assertEquals(t2, trees.first())
    }

    @Test
    fun byIntGreaterThanQuery() = blocking {
        val a = tested.createNode(Tree { it[age] = 42 })
        val b = tested.createNode(Tree { it[age] = 128 })
        tested.createNode(Tree { it[age] = 15 })
        val m = NodeMatch(Tree, Where.gt(Tree.age, 40))
        val trees = tested.query(m).toSet()
        assertEquals(setOf(a, b), trees)
    }

    @Test
    fun byRealLessThanQuery() = blocking {
        val a = tested.createNode(Tree { it[diameter] = 30.0 })
        val b = tested.createNode(Tree { it[diameter] = 6.0 })
        tested.createNode(Tree { it[diameter] = 128.0 })
        val m = NodeMatch(Tree, Where.lt(Tree.diameter, 40.0))
        val trees = tested.query(m).toSet()
        assertEquals(setOf(a, b), trees)
    }

    @Test
    fun byStringWithinQuery() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        tested.createNode(PersonV1 { it[name] = "Charlie" })
        val m = NodeMatch(PersonV1, Where.within(PersonV1.name, listOf("Alice", "Bob")))
        val trees = tested.query(m).toSet()
        assertEquals(setOf(a, b), trees)
    }

    @Test
    fun byGeoInsideQuery() = blocking {
        val a = tested.createNode(Tree { it[location] = GeoBounds(5.0, 5.1, 10.0, 10.05) })
        tested.createNode(Tree { it[location] = GeoBounds(6.0, 6.1, 10.0, 10.05) })
        tested.createNode(Tree { it[location] = GeoBounds(15.0, 15.1, -20.0, -19.95) })
        val m = NodeMatch(Tree, Where.inside(Tree.location, GeoBounds(4.0, 6.05, 9.0, 11.0)))
        val trees = tested.query(m).toSet()
        assertEquals(setOf(a), trees)
    }

    @Test
    fun byGeoOverlapsQuery() = blocking {
        val a = tested.createNode(Tree { it[location] = GeoBounds(5.0, 6.0, 10.0, 11.0) })
        val b = tested.createNode(Tree { it[location] = GeoBounds(6.0, 7.0, 10.0, 11.0) })
        tested.createNode(Tree { it[location] = GeoBounds(15.0, 16.0, -20.0, -19.0) })
        val m = NodeMatch(Tree, Where.overlaps(Tree.location, GeoBounds(4.0, 6.05, 9.0, 11.0)))
        val trees = tested.query(m).toSet()
        assertEquals(setOf(a, b), trees)
    }

    @Test
    fun followOutEdges() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val b = tested.createNode(PersonV1 { it[name] = "John Doe" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie Chaplin" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.handle, a.handle, b.handle, directed = true)
        tested.connect(loveEdge.handle, b.handle, a.handle, directed = true)
        tested.connect(likeEdge2.handle, b.handle, c.handle, directed = true)

        val match = NodeMatch(PersonV1, Where.handle(b.handle)).outgoing(EdgeMatch(Likes))
        val edges = tested.query(match).toSet()
        assertEquals(setOf(likeEdge2), edges)
    }

    @Test
    fun traceInEdges() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val b = tested.createNode(PersonV1 { it[name] = "John Doe" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie Chaplin" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.handle, a.handle, b.handle, directed = true)
        tested.connect(loveEdge.handle, b.handle, a.handle, directed = true)
        tested.connect(likeEdge2.handle, b.handle, c.handle, directed = true)

        val match = NodeMatch(PersonV1, Where.handle(b.handle)).incoming(EdgeMatch(Likes))
        val edges = tested.query(match).toSet()
        assertEquals(setOf(likeEdge), edges)
    }

    @Test
    fun connectedEdges() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val b = tested.createNode(PersonV1 { it[name] = "John Doe" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie Chaplin" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.handle, a.handle, b.handle, directed = true)
        tested.connect(loveEdge.handle, b.handle, a.handle, directed = true)
        tested.connect(likeEdge2.handle, b.handle, c.handle, directed = true)

        val match = NodeMatch(PersonV1, Where.handle(b.handle)).via(EdgeMatch(Likes))
        val edges = tested.query(match).toSet()
        assertEquals(setOf(likeEdge, likeEdge2), edges)
    }

    @Test
    fun endpointNodes() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.handle, a.handle, b.handle, directed = true)
        tested.connect(loveEdge.handle, b.handle, c.handle, directed = true)

        val match = EdgeMatch(Likes).endpoints(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(a, b), people)
    }

    @Test
    fun sourceNodes() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.handle, a.handle, b.handle, directed = true)
        tested.connect(likeEdge2.handle, a.handle, c.handle, directed = true)
        tested.connect(loveEdge.handle, b.handle, c.handle, directed = true)

        val match = EdgeMatch(Likes).sources(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(a), people)
    }

    @Test
    fun targetNodes() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.handle, a.handle, b.handle, directed = true)
        tested.connect(likeEdge2.handle, c.handle, b.handle, directed = true)
        tested.connect(loveEdge.handle, b.handle, c.handle, directed = true)

        val match = EdgeMatch(Likes).targets(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(b), people)
    }

    @Test
    fun nodeNeighbours() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.handle, a.handle, b.handle, directed = true)
        tested.connect(likeEdge2.handle, b.handle, c.handle, directed = true)
        tested.connect(loveEdge.handle, c.handle, b.handle, directed = true)

        val match = NodeMatch(PersonV1, Where.handle(b.handle)).adjacent(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(a, c), people)
    }

    @Test
    fun nodeSourceNeighbours() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.handle, a.handle, b.handle, directed = true)
        tested.connect(loveEdge.handle, b.handle, c.handle, directed = true)
        tested.connect(likeEdge2.handle, c.handle, a.handle, directed = true)

        val match = NodeMatch(PersonV1, Where.handle(b.handle)).sources(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(a), people)
    }

    @Test
    fun nodeTargetNeighbours() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.handle, a.handle, b.handle, directed = true)
        tested.connect(loveEdge.handle, b.handle, c.handle, directed = true)
        tested.connect(likeEdge2.handle, c.handle, a.handle, directed = true)

        val match = NodeMatch(PersonV1, Where.handle(b.handle)).targets(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(c), people)
    }

    @Test
    fun mutualLove() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        tested.createNode(PersonV1 { it[name] = "Charlie" })
        val abLove = tested.createEdge(Loves())
        val bcLove = tested.createEdge(Loves())
        tested.connect(abLove.handle, a.handle, b.handle, directed = true)
        tested.connect(bcLove.handle, b.handle, a.handle, directed = true)

        val match = NodeMatch(PersonV1, Where.handle(a.handle))
            .outgoing(EdgeMatch(Loves))
            .targets(NodeMatch(PersonV1))
            .outgoing(EdgeMatch(Loves))
            .targets(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(a), people)
    }

    @Test
    fun unrequitedLove() = blocking {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val abLove = tested.createEdge(Loves())
        val bcLove = tested.createEdge(Loves())
        tested.connect(abLove.handle, a.handle, b.handle, directed = true)
        tested.connect(bcLove.handle, b.handle, c.handle, directed = true)

        val match = NodeMatch(PersonV1, Where.handle(a.handle))
            .outgoing(EdgeMatch(Loves))
            .targets(NodeMatch(PersonV1))
            .outgoing(EdgeMatch(Loves))
            .targets(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(c), people)
    }

    @Test
    fun stringOrderAsc() = blocking {
        tested.createNode(PersonV1 { it[name] = "Charlie" })
        tested.createNode(PersonV1 { it[name] = "Alice" })
        tested.createNode(PersonV1 { it[name] = "Bob" })
        val people = tested.query(NodeMatch(PersonV1, order = Order.asc(PersonV1.name))).toList()
        assertEquals(listOf("Alice", "Bob", "Charlie"), people.map { it { name } })
    }

    @Test
    fun stringOrderDesc() = blocking {
        tested.createNode(PersonV1 { it[name] = "Charlie" })
        tested.createNode(PersonV1 { it[name] = "Alice" })
        tested.createNode(PersonV1 { it[name] = "Bob" })
        val people = tested.query(NodeMatch(PersonV1, order = Order.desc(PersonV1.name))).toList()
        assertEquals(listOf("Charlie", "Bob", "Alice"), people.map { it { name } })
    }

    @Test
    fun intOrderAsc() = blocking {
        tested.createNode(Tree { it[age] = 30 })
        tested.createNode(Tree { it[age] = 25 })
        tested.createNode(Tree { it[age] = 47 })
        val trees = tested.query(NodeMatch(Tree, order = Order.asc(Tree.age))).toList()
        assertEquals(listOf(25L, 30L, 47L), trees.map { it { age } })
    }

    @Test
    fun intOrderDesc() = blocking {
        tested.createNode(Tree { it[age] = 30 })
        tested.createNode(Tree { it[age] = 25 })
        tested.createNode(Tree { it[age] = 47 })
        val trees = tested.query(NodeMatch(Tree, order = Order.desc(Tree.age))).toList()
        assertEquals(listOf<Long>(47, 30, 25), trees.map { it { age } })
    }

    @Test
    fun realOrderAsc() = blocking {
        tested.createNode(Tree { it[diameter] = 1.0 })
        tested.createNode(Tree { it[diameter] = 0.3 })
        tested.createNode(Tree { it[diameter] = 2.0 })
        val trees = tested.query(NodeMatch(Tree, order = Order.asc(Tree.diameter))).toList()
        assertEquals(listOf(0.3, 1.0, 2.0), trees.map { it { diameter } })
    }

    @Test
    fun realOrderDesc() = blocking {
        tested.createEdge(Tree { it[diameter] = 1.0 })
        tested.createEdge(Tree { it[diameter] = 0.3 })
        tested.createEdge(Tree { it[diameter] = 2.0 })
        val trees = tested.query(EdgeMatch(Tree, order = Order.desc(Tree.diameter))).toList()
        assertEquals(listOf(2.0, 1.0, 0.3), trees.map { it { diameter } })
    }

    @Test
    fun filteredQueryWithOrder() = blocking {
        tested.createNode(Tree {
            it[name] = "Oak"
            it[age] = 5
        })
        tested.createNode(Tree {
            it[name] = "Oak"
            it[age] = 13
        })
        tested.createNode(Tree {
            it[name] = "Oak"
            it[age] = 2
        })
        tested.createNode(Tree {
            it[name] = "Willow"
            it[age] = 6
        })
        val trees =
            tested.query(NodeMatch(Tree, Where.eq(Tree.name, "Oak"), Order.asc(Tree.age))).toList()
        assertEquals(listOf<Long>(2, 5, 13), trees.map { it { age } })
    }

    @Test
    fun deleteWhileQuerying() = blocking {
        for (c in 'a'..'y') {
            val node = tested.createNode(PersonV1 { it[name] = c.toString() })
        }
        val nodeZ = tested.createNode(PersonV1 { it[name] = "z" })

        coroutineScope {
            launch {
                tested.query(NodeMatch(PersonV1, order = Order.asc(PersonV1.name))).collect {
                    yield()
                }
            }

            launch {
                tested.deleteNode(nodeZ.handle)
            }
        }
    }
}
