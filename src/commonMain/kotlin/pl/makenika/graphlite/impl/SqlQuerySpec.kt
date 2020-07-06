package pl.makenika.graphlite.impl

import pl.makenika.graphlite.Schema

data class SqlQuerySpec<S : Schema>(
    val schema: S,
    val join: String?,
    val where: String,
    val whereBindings: List<String>,
    val orderBy: String?
)
