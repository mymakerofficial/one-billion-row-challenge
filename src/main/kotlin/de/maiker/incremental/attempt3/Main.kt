package de.maiker.incremental.attempt3

import java.io.File
import java.util.*

data class StationData(
    var min: Double = .0,
    var max: Double = .0,
    var sum: Double = .0,
    var count: Int = 0
) {
    override fun toString(): String {
        val mean = sum / count
        return String.format(locale = Locale.ROOT, "%.1f/%.1f/%.1f", min, mean, max)
    }
}

fun main() {
    val stations = mutableMapOf<String, StationData>()

    File("./measurements.txt")
        .forEachLine {
            val (stationName, valueStr) = it.split(";")
            val value = valueStr.toDouble()

            val stationData = stations.getOrPut(stationName) { StationData() }

            if (value < stationData.min) {
                stationData.min = value
            }
            if (value > stationData.max) {
                stationData.max = value
            }
            stationData.sum += value
            stationData.count++
        }

    val results = stations.toSortedMap()

    println(results)
}
