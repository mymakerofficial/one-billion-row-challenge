package de.maiker.incremental.attempt2

import java.io.File
import java.util.*

fun main() {
    val stations = mutableMapOf<String, MutableList<Double>>()

    File("./measurements.txt")
        .forEachLine {
            val (stationName, valueStr) = it.split(";")
            val value = valueStr.toDouble()

            val listOfValues = stations.getOrPut(stationName) { mutableListOf() }
            listOfValues.add(value)
        }

    val results = stations
        .mapValues { (_, values) ->
            val min = values.min()
            val max = values.max()
            val mean = values.average()

            String.format(locale = Locale.ROOT, "%.1f/%.1f/%.1f", min, mean, max)
        }
        .toSortedMap()

    println(results)
}
