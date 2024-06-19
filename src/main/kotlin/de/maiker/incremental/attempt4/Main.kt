package de.maiker.incremental.attempt4

import java.io.File
import java.util.*

data class StationData(
    var min: Int = Int.MAX_VALUE,
    var max: Int = Int.MIN_VALUE,
    var sum: Int = 0,
    var count: Int = 0
) {
    override fun toString(): String {
        val mean = sum / count
        return String.format(locale = Locale.ROOT, "%.1f/%.1f/%.1f", min.toDouble()/10, mean.toDouble()/10, max.toDouble()/10)
    }
}

fun parseNumber(input: String): Int {
    var isNegative = false
    var number = 0
    var index = 0

    if (input[0] == '-') {
        isNegative = true
        index = 1
    }

    var char: Char
    while (index < input.length) {
        char = input[index]
        index++

        if (char == '.') {
            continue
        }

        number = number * 10 + (char.code - 48 /* '0' */)
    }

    return if (isNegative) -number else number
}

fun main() {
    val startTime = System.nanoTime()

    val stations = mutableMapOf<String, StationData>()

    File("./measurements.txt")
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

    val endTime = System.nanoTime()
    println("Took ${(endTime - startTime) / 1_000_000} ms")
}
