package pl.makenika.graphlite

import pl.makenika.graphlite.sql.SqliteDriver
import kotlin.test.*

abstract class BaseQueryEngineTest {
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
    fun schemaQuery() {
        val p1 = tested.createNode(PersonV1 { it[name] = "John Doe" })
        val p2 = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val m = NodeMatch(PersonV1)
        val people = tested.query(m).toList()
        assertEquals(2, people.size)
        assertEquals(p1, people[0])
        assertEquals(p2, people[1])
    }

    @Test
    fun byIdQuery() {
        tested.createNode(PersonV1 { it[name] = "John Doe" })
        val p1 = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val m = NodeMatch(PersonV1, Where.id(p1.id))
        val people = tested.query(m).toList()
        assertEquals(1, people.size)
        assertEquals(p1, people[0])
    }

    @Test
    fun byNameQuery() {
        tested.createNode(PersonV1 { it[name] = "John Doe" })
        val p1 = tested.createNode("p2", PersonV1 { it[name] = "Jane Smith" })
        val m = NodeMatch(PersonV1, Where.name("p2"))
        val people = tested.query(m).toList()
        assertEquals(1, people.size)
        assertEquals(p1, people[0])
    }

    @Test
    fun byIdAndNameQuery() {
        val p1 = tested.createNode("p1", PersonV1 { it[name] = "Jane Smith" })!!
        val m = NodeMatch(PersonV1, Where.and(Where.id(p1.id), Where.name("p1")))
        val people = tested.query(m).toList()
        assertEquals(1, people.size)
        assertEquals(p1, people[0])
    }

    @Test
    fun byIdOrNameQuery() {
        val p1 = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val p2 = tested.createNode("p2", PersonV1 { it[name] = "John Doe" })
        assertNotNull(p2)

        val m = NodeMatch(PersonV1, Where.or(Where.id(p1.id), Where.name("p2")))
        val people = tested.query(m).toList()

        assertEquals(2, people.size)
        assertEquals(p1, people[0])
        assertEquals(p2, people[1])
    }

    @Test
    fun byGeoValueQuery() {
        val bounds = GeoBounds(1.0, 1.02, -5.5, -5.4)
        val tree = tested.createNode(Tree { it[location] = bounds })
        val m = NodeMatch(Tree, Where.eq(Tree.location, bounds))
        val trees = tested.query(m).toList()
        assertEquals(1, trees.size)
        assertEquals(tree, trees[0])
    }

    @Test
    fun byStringValueQuery() {
        tested.createNode(PersonV1 { it[name] = "John Doe" })
        val p1 = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val p2 = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val m = NodeMatch(PersonV1, Where.eq(PersonV1.name, "Jane Smith"))
        val people = tested.query(m).toSet()
        assertEquals(setOf(p1, p2), people)
    }

    @Test
    fun byFtsQuery() {
        tested.createNode(PersonV1 { it[name] = "John Doe" })
        val p1 = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val p2 = tested.createNode(PersonV1 { it[name] = "Charlie Smith" })
        val m = NodeMatch(PersonV1, Where.fts(PersonV1.name, "Smith"))
        val people = tested.query(m).toSet()
        assertEquals(setOf(p1, p2), people)
    }

    @Test
    fun byIntBetweenQuery() {
        tested.createNode(Tree { it[age] = 42 })
        val t2 = tested.createNode(Tree { it[age] = 128 })
        val m = NodeMatch(Tree, Where.between(Tree.age, 100, 130))
        val trees = tested.query(m).toList()
        assertEquals(1, trees.size)
        assertEquals(t2, trees.first())
    }

    @Test
    fun byIntGreaterThanQuery() {
        val a = tested.createNode(Tree { it[age] = 42 })
        val b = tested.createNode(Tree { it[age] = 128 })
        tested.createNode(Tree { it[age] = 15 })
        val m = NodeMatch(Tree, Where.gt(Tree.age, 40))
        val trees = tested.query(m).toSet()
        assertEquals(setOf(a, b), trees)
    }

    @Test
    fun byRealLessThanQuery() {
        val a = tested.createNode(Tree { it[diameter] = 30.0 })
        val b = tested.createNode(Tree { it[diameter] = 6.0 })
        tested.createNode(Tree { it[diameter] = 128.0 })
        val m = NodeMatch(Tree, Where.lt(Tree.diameter, 40.0))
        val trees = tested.query(m).toSet()
        assertEquals(setOf(a, b), trees)
    }

    @Test
    fun byStringWithinQuery() {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        tested.createNode(PersonV1 { it[name] = "Charlie" })
        val m = NodeMatch(PersonV1, Where.within(PersonV1.name, listOf("Alice", "Bob")))
        val trees = tested.query(m).toSet()
        assertEquals(setOf(a, b), trees)
    }

    @Test
    fun byGeoInsideQuery() {
        val a = tested.createNode(Tree { it[location] = GeoBounds(5.0, 5.1, 10.0, 10.05) })
        tested.createNode(Tree { it[location] = GeoBounds(6.0, 6.1, 10.0, 10.05) })
        tested.createNode(Tree { it[location] = GeoBounds(15.0, 15.1, -20.0, -19.95) })
        val m = NodeMatch(Tree, Where.inside(Tree.location, GeoBounds(4.0, 6.05, 9.0, 11.0)))
        val trees = tested.query(m).toSet()
        assertEquals(setOf(a), trees)
    }

    @Test
    fun byGeoOverlapsQuery() {
        val a = tested.createNode(Tree { it[location] = GeoBounds(5.0, 6.0, 10.0, 11.0) })
        val b = tested.createNode(Tree { it[location] = GeoBounds(6.0, 7.0, 10.0, 11.0) })
        tested.createNode(Tree { it[location] = GeoBounds(15.0, 16.0, -20.0, -19.0) })
        val m = NodeMatch(Tree, Where.overlaps(Tree.location, GeoBounds(4.0, 6.05, 9.0, 11.0)))
        val trees = tested.query(m).toSet()
        assertEquals(setOf(a, b), trees)
    }

    @Test
    fun followOutEdges() {
        val a = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val b = tested.createNode(PersonV1 { it[name] = "John Doe" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie Chaplin" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.name, a.name, b.name, directed = true)
        tested.connect(loveEdge.name, b.name, a.name, directed = true)
        tested.connect(likeEdge2.name, b.name, c.name, directed = true)

        val match = NodeMatch(PersonV1, Where.id(b.id)).outgoing(EdgeMatch(Likes))
        val edges = tested.query(match).toSet()
        assertEquals(setOf(likeEdge2), edges)
    }

    @Test
    fun traceInEdges() {
        val a = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val b = tested.createNode(PersonV1 { it[name] = "John Doe" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie Chaplin" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.name, a.name, b.name, directed = true)
        tested.connect(loveEdge.name, b.name, a.name, directed = true)
        tested.connect(likeEdge2.name, b.name, c.name, directed = true)

        val match = NodeMatch(PersonV1, Where.id(b.id)).incoming(EdgeMatch(Likes))
        val edges = tested.query(match).toSet()
        assertEquals(setOf(likeEdge), edges)
    }

    @Test
    fun connectedEdges() {
        val a = tested.createNode(PersonV1 { it[name] = "Jane Smith" })
        val b = tested.createNode(PersonV1 { it[name] = "John Doe" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie Chaplin" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.name, a.name, b.name, directed = true)
        tested.connect(loveEdge.name, b.name, a.name, directed = true)
        tested.connect(likeEdge2.name, b.name, c.name, directed = true)

        val match = NodeMatch(PersonV1, Where.id(b.id)).via(EdgeMatch(Likes))
        val edges = tested.query(match).toSet()
        assertEquals(setOf(likeEdge, likeEdge2), edges)
    }

    @Test
    fun endpointNodes() {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.name, a.name, b.name, directed = true)
        tested.connect(loveEdge.name, b.name, c.name, directed = true)

        val match = EdgeMatch(Likes).endpoints(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(a, b), people)
    }

    @Test
    fun sourceNodes() {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.name, a.name, b.name, directed = true)
        tested.connect(likeEdge2.name, a.name, c.name, directed = true)
        tested.connect(loveEdge.name, b.name, c.name, directed = true)

        val match = EdgeMatch(Likes).sources(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(a), people)
    }

    @Test
    fun targetNodes() {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.name, a.name, b.name, directed = true)
        tested.connect(likeEdge2.name, c.name, b.name, directed = true)
        tested.connect(loveEdge.name, b.name, c.name, directed = true)

        val match = EdgeMatch(Likes).targets(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(b), people)
    }

    @Test
    fun nodeNeighbours() {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.name, a.name, b.name, directed = true)
        tested.connect(likeEdge2.name, b.name, c.name, directed = true)
        tested.connect(loveEdge.name, c.name, b.name, directed = true)

        val match = NodeMatch(PersonV1, Where.id(b.id)).adjacent(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(a, c), people)
    }

    @Test
    fun nodeSourceNeighbours() {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.name, a.name, b.name, directed = true)
        tested.connect(loveEdge.name, b.name, c.name, directed = true)
        tested.connect(likeEdge2.name, c.name, a.name, directed = true)

        val match = NodeMatch(PersonV1, Where.id(b.id)).sources(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(a), people)
    }

    @Test
    fun nodeTargetNeighbours() {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val likeEdge = tested.createEdge(Likes())
        val likeEdge2 = tested.createEdge(Likes())
        val loveEdge = tested.createEdge(Loves())
        tested.connect(likeEdge.name, a.name, b.name, directed = true)
        tested.connect(loveEdge.name, b.name, c.name, directed = true)
        tested.connect(likeEdge2.name, c.name, a.name, directed = true)

        val match = NodeMatch(PersonV1, Where.id(b.id)).targets(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(c), people)
    }

    @Test
    fun mutualLove() {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        tested.createNode(PersonV1 { it[name] = "Charlie" })
        val abLove = tested.createEdge(Loves())
        val bcLove = tested.createEdge(Loves())
        tested.connect(abLove.name, a.name, b.name, directed = true)
        tested.connect(bcLove.name, b.name, a.name, directed = true)

        val match = NodeMatch(PersonV1, Where.id(a.id))
            .outgoing(EdgeMatch(Loves))
            .targets(NodeMatch(PersonV1))
            .outgoing(EdgeMatch(Loves))
            .targets(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(a), people)
    }

    @Test
    fun unrequitedLove() {
        val a = tested.createNode(PersonV1 { it[name] = "Alice" })
        val b = tested.createNode(PersonV1 { it[name] = "Bob" })
        val c = tested.createNode(PersonV1 { it[name] = "Charlie" })
        val abLove = tested.createEdge(Loves())
        val bcLove = tested.createEdge(Loves())
        tested.connect(abLove.name, a.name, b.name, directed = true)
        tested.connect(bcLove.name, b.name, c.name, directed = true)

        val match = NodeMatch(PersonV1, Where.id(a.id))
            .outgoing(EdgeMatch(Loves))
            .targets(NodeMatch(PersonV1))
            .outgoing(EdgeMatch(Loves))
            .targets(NodeMatch(PersonV1))
        val people = tested.query(match).toSet()
        assertEquals(setOf(c), people)
    }

    @Test
    fun stringOrderAsc() {
        tested.createNode(PersonV1 { it[name] = "Charlie" })
        tested.createNode(PersonV1 { it[name] = "Alice" })
        tested.createNode(PersonV1 { it[name] = "Bob" })
        val people = tested.query(NodeMatch(PersonV1, order = Order.asc(PersonV1.name))).toList()
        assertEquals(listOf("Alice", "Bob", "Charlie"), people.map { it { name } })
    }

    @Test
    fun stringOrderDesc() {
        tested.createNode(PersonV1 { it[name] = "Charlie" })
        tested.createNode(PersonV1 { it[name] = "Alice" })
        tested.createNode(PersonV1 { it[name] = "Bob" })
        val people = tested.query(NodeMatch(PersonV1, order = Order.desc(PersonV1.name))).toList()
        assertEquals(listOf("Charlie", "Bob", "Alice"), people.map { it { name } })
    }

    @Test
    fun intOrderAsc() {
        tested.createNode(Tree { it[age] = 30 })
        tested.createNode(Tree { it[age] = 25 })
        tested.createNode(Tree { it[age] = 47 })
        val trees = tested.query(NodeMatch(Tree, order = Order.asc(Tree.age))).toList()
        assertEquals(listOf(25L, 30L, 47L), trees.map { it { age } })
    }

    @Test
    fun intOrderDesc() {
        tested.createNode(Tree { it[age] = 30 })
        tested.createNode(Tree { it[age] = 25 })
        tested.createNode(Tree { it[age] = 47 })
        val trees = tested.query(NodeMatch(Tree, order = Order.desc(Tree.age))).toList()
        assertEquals(listOf<Long>(47, 30, 25), trees.map { it { age } })
    }

    @Test
    fun realOrderAsc() {
        tested.createNode(Tree { it[diameter] = 1.0 })
        tested.createNode(Tree { it[diameter] = 0.3 })
        tested.createNode(Tree { it[diameter] = 2.0 })
        val trees = tested.query(NodeMatch(Tree, order = Order.asc(Tree.diameter))).toList()
        assertEquals(listOf(0.3, 1.0, 2.0), trees.map { it { diameter } })
    }

    @Test
    fun realOrderDesc() {
        tested.createEdge(Tree { it[diameter] = 1.0 })
        tested.createEdge(Tree { it[diameter] = 0.3 })
        tested.createEdge(Tree { it[diameter] = 2.0 })
        val trees = tested.query(EdgeMatch(Tree, order = Order.desc(Tree.diameter))).toList()
        assertEquals(listOf(2.0, 1.0, 0.3), trees.map { it { diameter } })
    }

    @Test
    fun filteredQueryWithOrder() {
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

    // TODO test using "EXPLAIN QUERY PLAN"
}
