package pl.makenika.graphlite

public object Animal : Schema("animal", 1) {
    val name = textField("n")
}

internal object Likes : Schema("likes", 1)

internal object LikesV2 : Schema("likes", 2) {
    val level = longField("l")
}

internal object Loves : Schema("loves", 1)

internal object PersonV1 : Schema("person", 1) {
    val name = textField("n").onValidate { it.isNotBlank() }
}

internal object PersonV2 : Schema("person", 2) {
    val firstName = textField("fn")
    val lastName = textField("ln")
}

internal object Tree : Schema("tree", 1) {
    val age = optional().longField("a")
    val diameter = optional().doubleField("d")
    val location = optional().geoField("l")
    val name = optional().textField("n")
    val secret = optional().blobField("s").onValidate {
        if (it.contentEquals(byteArrayOf(0x64))) this[age] == 100L else true
    }
}
