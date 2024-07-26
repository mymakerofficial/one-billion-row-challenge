package de.maiker.incremental.attempt6

import de.maiker.util.printTime
import sun.misc.Unsafe
import java.io.File
import java.io.RandomAccessFile
import java.lang.foreign.Arena
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.util.*


data class StationData(
    var name: String,
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
    val name = "measurements"
    val input = "./${name}.txt"
    val output = "./${name}.out"

    val startTime = System.nanoTime()

    val unsafe = Unsafe::class.java.getDeclaredField("theUnsafe").let {
        it.isAccessible = true
        it.get(Unsafe::class) as Unsafe
    }

    val channel = RandomAccessFile(input, "r").getChannel()
    val memorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofShared())

    val startAddress = memorySegment.address()
    val endAddress = startAddress + memorySegment.byteSize()
    var address = startAddress

    var byte: Byte

    val stations = mutableMapOf<Int, StationData>()

    val SEMICOLON = 59.toByte() /* ';' */
    val DASH = 45.toByte() /* '-' */
    val NEWLINE = 10.toByte() /* \n */
    val DOT = 46.toByte() /* '.' */

    var nameLength = 0L
    var hash: Int
    fun readName(): Int {
        nameLength = 0
        hash = 5381
        while (true) { // no need to check, we will always hit the ';'
            byte = unsafe.getByte(address)
            address++

            if (byte == SEMICOLON /* ';' */) {
                break
            }

            hash = (((hash shl 5) + hash) + byte)
            nameLength++
        }
        return hash
    }

    var isNegative: Boolean
    var number: Int
    fun readNumber(): Int {
        isNegative = false
        number = 0

        byte = unsafe.getByte(address)
        if (byte == DASH /* '-' */) {
            isNegative = true
            address++
        }

        while (true) { // no need to check, we will always hit the new line
            byte = unsafe.getByte(address)
            address++

            if (byte == NEWLINE /* \n */) {
                break
            }

            if (byte == DOT /* '.' */) {
                continue
            }

            number = number * 10 + (byte - 48 /* '0' */)
        }

        return if (isNegative) -number else number
    }

    val ARRAY_BYTE_BASE_OFFSET = Unsafe.ARRAY_BYTE_BASE_OFFSET.toLong()
    val nameBuffer = ByteArray(100)
    var nameStartAddress: Long
    while (address < endAddress) {
        nameStartAddress = address
        val hash = readName()
        val value = readNumber()

        val stationData = stations.getOrPut(hash) {
            unsafe.copyMemory(null, nameStartAddress, nameBuffer, ARRAY_BYTE_BASE_OFFSET, nameLength)

            StationData(
                name = String(nameBuffer, 0, nameLength.toInt(), StandardCharsets.UTF_8)
            )
        }

        if (value < stationData.min) { stationData.min = value }
        if (value > stationData.max) { stationData.max = value }
        stationData.sum += value
        stationData.count++
    }

    println(stations.mapKeys {
        it.value.name
    }.toSortedMap())

    printTime(startTime)

    val actual = stations.mapKeys {
        it.value.name
    }.toSortedMap().toString()
    File(output).bufferedReader().use { reader ->
        val expected = reader.readText()
        println("Expected:\t $expected")
        println("Actual:\t\t $actual")

        if (expected == actual) {
            println("SUCCESS")
        } else {
            println("FAILURE")
        }
    }
}

