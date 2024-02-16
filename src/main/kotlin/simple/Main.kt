package de.maiker.simple

import java.io.File

// calculate the min / mean / max of each station
// sort alphabetically by station name
// round to 1 decimal place

data class Measurement(val station: String, val value: Double) {
    constructor(parts: List<String>) : this(parts[0], parts[1].toDouble())
}

data class StationResult(var median: Double, var min: Double, var max: Double) {
    fun add(value: Double) {
        if (value < min) {
            min = value
        }
        if (value > max) {
            max = value
        }

        median = median + value / 2
    }

    override fun toString(): String {
        return round(min).toString() + "/" + round(median).toString() + "/" + round(max).toString()
    }

    private fun round(value: Double): Double {
        return Math.round(value * 10.0) / 10.0
    }
}

fun main() {
    val FILE = "./measurements.txt"

    val measurements = File(FILE)
        .readLines()
        .map { Measurement(it.split(";")) }
        .groupBy({ it.station }, { it.value })
        .mapValues { (_, values) ->
            StationResult(
                values.min(),
                values.average(),
                values.max()
            )
        }

    println(measurements)

}