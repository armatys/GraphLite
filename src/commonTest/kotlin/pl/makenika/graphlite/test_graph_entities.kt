package pl.makenika.graphlite

object Animal : Schema("animal", 1) {
    val name = textField("n")
}

object Likes : Schema("likes", 1)

object LikesV2 : Schema("likes", 2) {
    val level = longField("l")
}

object Loves : Schema("loves", 1)

object PersonV1 : Schema("person", 1) {
    val name = textField("n")
}

object PersonV2 : Schema("person", 2) {
    val firstName = textField("fn")
    val lastName = textField("ln")
}

object Tree : Schema("tree", 1) {
    val age = optional().longField("a")
    val diameter = optional().doubleField("d")
    val location = optional().geoField("l")
    val name = optional().textField("n")
    val secret = optional().blobField("s")
}
