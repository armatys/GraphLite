package pl.makenika.graphlite

import pl.makenika.graphlite.impl.EdgesBySchema
import pl.makenika.graphlite.impl.NodesBySchema
import pl.makenika.graphlite.impl.OrderImpl
import pl.makenika.graphlite.impl.WhereImpl

interface ElementMatch<S : Schema>

interface EdgeMatch<S : Schema> : ElementMatch<S> {
    /** Travels to matching incident nodes. */
    infix fun <R : Schema> endpoints(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching incident source nodes. */
    infix fun <R : Schema> sources(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching incident target nodes. */
    infix fun <R : Schema> targets(match: NodeMatch<R>): NodeMatch<R>
}

@Suppress("FunctionName")
fun <S : Schema> EdgeMatch(schema: S, where: Where<S>? = null, order: Order<S>? = null): EdgeMatch<S> {
    return EdgesBySchema(schema, where, order)
}

interface NodeMatch<S : Schema> : ElementMatch<S> {
    /** Travels via matching incoming edges. */
    infix fun <R : Schema> incoming(match: EdgeMatch<R>): EdgeMatch<R>

    /** Travels via matching outgoing edges. */
    infix fun <R : Schema> outgoing(match: EdgeMatch<R>): EdgeMatch<R>

    /** Travels via matching edges. */
    infix fun <R : Schema> via(match: EdgeMatch<R>): EdgeMatch<R>

    /** Travels to matching neighbour nodes. */
    infix fun <R : Schema> adjacent(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching source nodes. */
    infix fun <R : Schema> sources(match: NodeMatch<R>): NodeMatch<R>

    /** Travels to matching target nodes. */
    infix fun <R : Schema> targets(match: NodeMatch<R>): NodeMatch<R>
}

@Suppress("FunctionName")
fun <S : Schema> NodeMatch(schema: S, where: Where<S>? = null, order: Order<S>? = null): NodeMatch<S> {
    return NodesBySchema(schema, where, order)
}

interface Where<S : Schema> {
    companion object {
        fun <S : Schema> and(a: Where<S>, b: Where<S>): Where<S> = WhereImpl.And(a, b)

        fun <S : Schema> or(a: Where<S>, b: Where<S>): Where<S> = WhereImpl.Or(a, b)

        fun <S : Schema> id(id: ElementId): Where<S> = WhereImpl.Id(id)

        fun <S : Schema> name(name: String): Where<S> = WhereImpl.Name(name)

        fun <S : Schema, V : Any, T : V?> between(field: IndexableScalarField<S, T>, start: V, end: V): Where<S> =
            WhereImpl.Between(field, start, end)

        fun <S : Schema, T> eq(field: IndexableField<S, T>, value: T): Where<S> = WhereImpl.Equal(field, value)

        fun <S : Schema, V : Any, T : V?> gt(field: IndexableScalarField<S, T>, value: V): Where<S> =
            WhereImpl.GreaterThan(field, value)

        fun <S : Schema, V : Any, T : V?> lt(field: IndexableScalarField<S, T>, value: V): Where<S> =
            WhereImpl.LessThan(field, value)

        fun <S : Schema, T> within(field: IndexableScalarField<S, T>, values: List<T>): Where<S> =
            WhereImpl.Within(field, values)

        fun <S : Schema> fts(field: IndexableScalarField<S, String>, value: String): Where<S> =
            WhereImpl.FullText(field, value)

        fun <S : Schema, T : GeoCoordinates?> inside(field: IndexableField<S, T>, value: GeoCoordinates): Where<S> =
            WhereImpl.Inside(field, value)

        fun <S : Schema, T : GeoCoordinates?> overlaps(field: IndexableField<S, T>, value: GeoCoordinates): Where<S> =
            WhereImpl.Overlaps(field, value)
    }
}

interface Order<S : Schema> {
    companion object {
        fun <S : Schema> asc(field: IndexableScalarField<S, *>): Order<S> = OrderImpl.Asc(field)
        fun <S : Schema> desc(field: IndexableScalarField<S, *>): Order<S> = OrderImpl.Desc(field)
    }
}
