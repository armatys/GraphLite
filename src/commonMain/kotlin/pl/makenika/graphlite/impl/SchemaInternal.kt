package pl.makenika.graphlite.impl

import pl.makenika.graphlite.Schema
import pl.makenika.graphlite.SchemaHandle

internal class SchemaInternal(
    val id: String,
    val clockValue: Long,
    val parentId: String?,
    schemaHandle: SchemaHandle,
    schemaVersion: Long
) : Schema(schemaHandle, schemaVersion)
