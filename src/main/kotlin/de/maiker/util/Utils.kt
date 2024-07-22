package de.maiker.util

fun printTime(startTime: Long) {
    val endTime = System.nanoTime()
    val ms = (endTime - startTime) / 1_000_000
    // mm:ss.SSS
    val formatted = String.format("%d:%02d.%03d", ms / 60000, (ms % 60000) / 1000, ms % 1000)
    println("Took ${ms}ms (${formatted})")
}