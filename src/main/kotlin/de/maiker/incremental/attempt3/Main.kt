package de.maiker.incremental.attempt3

import java.io.File
import java.util.*

fun formatData(min: Double, mean: Double, max: Double): String {
    // use Locale.ROOT so that the decimal separator is always a dot
    return String.format(locale = Locale.ROOT, "%.1f/%.1f/%.1f", min, mean, max)
}

data class StationData(
    var min: Double = .0,
    var max: Double = .0,
    var sum: Double = .0,
    var count: Int = 0
) {
    val mean get() = sum / count

    override fun toString(): String = formatData(min, mean, max)
}

fun main() {
    val startTime = System.nanoTime()

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

    val endTime = System.nanoTime()
    println("Took ${(endTime - startTime) / 1_000_000} ms")
}
