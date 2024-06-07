package de.maiker.incremental.attempt4

import java.io.RandomAccessFile
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout
import java.nio.channels.FileChannel
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

fun main() {
    val startTime = System.nanoTime()

    val stations = mutableMapOf<String, StationData>()

    val channel = RandomAccessFile("./measurements.txt", "r").getChannel()
    val memorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofShared())

    val size = channel.size()

    var position = 0L

    fun readName(memorySegment: MemorySegment): String {
        val data = ByteArray(100)
        var length = 0
        while (position < size) {
            val byte = memorySegment.getAtIndex(ValueLayout.JAVA_BYTE, position)
            position++

            if (byte == 59.toByte()) {
                break
            }

            data[length++] = byte
        }
        return String(data, 0, length)
    }

    fun readNumber(memorySegment: MemorySegment): Int {
        var number = 0
        var sign = 1
        while (position < size) {
            val byte = memorySegment.getAtIndex(ValueLayout.JAVA_BYTE, position)
            position++

            if (byte == 10.toByte()) {
                break
            }

            if (byte == 45.toByte()) {
                sign = -1
                continue
            }

            if (byte == 46.toByte()) {
                continue
            }

            number = number * 10 + (byte - 48)
        }
        return number * sign
    }

    while (position < size) {
        val stationName = readName(memorySegment)
        val value = readNumber(memorySegment)

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

