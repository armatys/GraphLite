package pl.makenika.graphlite

data class GeoCoordinates(
    val minLat: Double,
    val maxLat: Double,
    val minLon: Double,
    val maxLon: Double
) {
    init {
        require(minLat <= maxLat)
        require(minLon <= maxLon)
    }
}
