package de.maiker.incremental.attempt4

import java.io.File
import java.util.*

data class StationData(
    var min: Int = 0,
    var max: Int = 0,
    var sum: Int = 0,
    var count: Int = 0
) {
    override fun toString(): String {
        val mean = sum / count
        return String.format(locale = Locale.ROOT, "%.1f/%.1f/%.1f", min.toDouble()/10, mean.toDouble()/10, max.toDouble()/10)
    }
}

fun parseNumber(input: String): Int {
    var number = 0
    var sign = 1
    for (char in input) {
        if (char == '-') {
            sign = -1
            continue
        }
        if (char == '.') {
            continue
        }
        number = number * 10 + (char.code - 48 /* '0' */)
    }
    return number * sign
}

fun main() {
    val stations = mutableMapOf<String, StationData>()

    File("./measurements-small.txt")
        .forEachLine {
            val (stationName, valueStr) = it.split(";")
            val value = parseNumber(valueStr)

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
