package de.maiker.incremental.attempt1

import java.io.File
import java.util.*

fun main() {
    val result = File("./measurements.txt")
        .readLines()
        .map { it.split(";") }
        .groupBy( { it.first() }, { it.last().toDouble() } )
        .mapValues { (_, values) ->
            val min = values.min()
            val max = values.max()
            val mean = values.average()

            // use Locale.ROOT so that the decimal separator is always a dot
            String.format(locale = Locale.ROOT, "%.1f/%.1f/%.1f", min, mean, max)
        }
        .toSortedMap()

    println(result)
}
