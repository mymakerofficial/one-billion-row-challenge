package de.maiker.incremental.attempt2

import de.maiker.util.printTime
import java.io.File
import java.util.*

fun formatData(min: Double, mean: Double, max: Double): String {
    // use Locale.ROOT so that the decimal separator is always a dot
    return String.format(locale = Locale.ROOT, "%.1f/%.1f/%.1f", min, mean, max)
}

fun main() {
    val startTime = System.nanoTime()

    val stations = mutableMapOf<String, MutableList<Double>>()

    File("./measurements.txt")
        .forEachLine {
            val (stationName, valueStr) = it.split(";")
            val value = valueStr.toDouble()

            stations.getOrPut(stationName) { mutableListOf() }
                .add(value)
        }

    val results = stations
        .mapValues { (_, values) ->
            val min = values.min()
            val max = values.max()
            val mean = values.average()

            formatData(min, mean, max)
        }
        .toSortedMap()

    println(results)

    printTime(startTime)
}
