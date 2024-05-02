package de.maiker.old.bad

import java.io.File
import java.util.*

// calculate the min / mean / max of each station
// sort alphabetically by station name
// round to 1 decimal place

data class Measurement(val station: String, val value: Double) {
    constructor(parts: List<String>) : this(parts[0], parts[1].toDouble())
}

data class StationResult(var count: Int, var sum: Double, var min: Double, var max: Double) {
    public fun add(value: Double) {
        count++
        sum += value

        if (value < min) {
            min = value
        }
        if (value > max) {
            max = value
        }
    }

    override fun toString(): String {
        return round(min).toString() + "/" + round(sum / count).toString() + "/" + round(max).toString()
    }

    private fun round(value: Double): Double {
        return Math.round(value * 10.0) / 10.0
    }
}

fun main() {
    val FILE = "./measurements.txt"

    val startTime = System.nanoTime()

    val reader = File(FILE).bufferedReader()

    val measurements = TreeMap<String, StationResult>()

    while (true) {
        val line = reader.readLine() ?: break
        val measurement = Measurement(line.split(";"))

        val record = measurements[measurement.station]

        if (record == null) {
            measurements[measurement.station] = StationResult(0, measurement.value, measurement.value, measurement.value)
        } else {
            record.add(measurement.value)
        }
    }

    val endTime = System.nanoTime()

    println(measurements)

    println("Time: " + (endTime - startTime) / 1000000 + "ms")
}