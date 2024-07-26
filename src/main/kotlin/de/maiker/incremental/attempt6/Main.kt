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

val name = "measurements"
val input = "./${name}.txt"
val output = "./${name}.out"

val SEMICOLON = 59.toByte() /* ';' */
val DASH = 45.toByte() /* '-' */
val NEWLINE = 10.toByte() /* \n */
val DOT = 46.toByte() /* '.' */
val ARRAY_BYTE_BASE_OFFSET = Unsafe.ARRAY_BYTE_BASE_OFFSET.toLong()

val unsafe = Unsafe::class.java.getDeclaredField("theUnsafe").let {
    it.isAccessible = true
    it.get(Unsafe::class) as Unsafe
}

class Reader(
    private val startAddress: Long,
    private val endAddress: Long,
) {
    private var address = startAddress
    private var byte: Byte = 0

    fun hasRemaining(): Boolean = address < endAddress

    private var nameStartAddress = 0L
    private var nameLength = 0L
    private var hash = 5381
    fun readName(): Int {
        nameStartAddress = address
        nameLength = 0
        hash = 5381
        // no need to check, we will always hit the ';'
        while (true) {
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

    private val nameBuffer = ByteArray(100)
    fun getName(): String {
        unsafe.copyMemory(null, nameStartAddress, nameBuffer, ARRAY_BYTE_BASE_OFFSET, nameLength)
        return String(nameBuffer, 0, nameLength.toInt(), StandardCharsets.UTF_8)
    }

    private var isNegative = false
    private var number = 0
    fun readNumber(): Int {
        isNegative = false
        number = 0

        byte = unsafe.getByte(address)
        if (byte == DASH /* '-' */) {
            isNegative = true
            address++
        }

        // no need to check, we will always hit the new line
        while (true) {
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
}

fun main() {
    val startTime = System.nanoTime()

    val channel = RandomAccessFile(input, "r").getChannel()
    val memorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofShared())

    val startAddress = memorySegment.address()
    val endAddress = startAddress + memorySegment.byteSize()

    val reader = Reader(startAddress, endAddress)
    val stations = mutableMapOf<Int, StationData>()

    while (reader.hasRemaining()) {
        val hash = reader.readName()
        val value = reader.readNumber()

        val stationData = stations.getOrPut(hash) {
            StationData(name = reader.getName())
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

