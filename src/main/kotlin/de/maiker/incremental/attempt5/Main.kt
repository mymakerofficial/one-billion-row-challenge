package de.maiker.incremental.attempt5

import java.io.RandomAccessFile
import java.lang.foreign.Arena
import java.lang.foreign.ValueLayout
import java.nio.channels.FileChannel
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

fun main() {
    val startTime = System.nanoTime()

    val stations = mutableMapOf<String, StationData>()

    val channel = RandomAccessFile("./measurements.txt", "r").getChannel()
    val memorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofShared())

    val size = channel.size()
    var position = 0L

    val nameBuffer = ByteArray(100)
    var nameLength: Int
    fun readName(): String {
        nameLength = 0
        while (position < size) {
            val byte = memorySegment.getAtIndex(ValueLayout.JAVA_BYTE, position)
            position++

            if (byte == 59.toByte() /* ';' */) {
                break
            }

            nameBuffer[nameLength++] = byte
        }
        return String(nameBuffer, 0, nameLength)
    }

    var isNegative: Boolean
    var number: Int
    fun readNumber(): Int {
        isNegative = false
        number = 0

        if (memorySegment.getAtIndex(ValueLayout.JAVA_BYTE, position) == 45.toByte() /* '-' */) {
            isNegative = true
            position++
        }

        while (position < size) {
            val byte = memorySegment.getAtIndex(ValueLayout.JAVA_BYTE, position)
            position++

            if (byte == 10.toByte() /* \n */) {
                break
            }

            if (byte == 46.toByte() /* '.' */) {
                continue
            }

            number = number * 10 + (byte - 48 /* '0' */)
        }

        return if (isNegative) -number else number
    }

    while (position < size) {
        val stationName = readName()
        val value = readNumber()

        val stationData = stations.getOrPut(stationName) { StationData() }

        if (value < stationData.min) { stationData.min = value }
        if (value > stationData.max) { stationData.max = value }
        stationData.sum += value
        stationData.count++
    }

    println(stations.toSortedMap())

    val endTime = System.nanoTime()
    println("Took ${(endTime - startTime) / 1_000_000} ms")
}

