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
    var nameAddress: Long = 0,
    var nameLength: Int = 0,
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

    var nameLength = 0
    var hash: Int
    fun readName(): Int {
        nameLength = 0
        hash = 5381
        while (address < endAddress) {
            byte = unsafe.getByte(address)
            address++

            if (byte == 59.toByte() /* ';' */) {
                break
            }

            // TODO is not sortable
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
        if (byte == 45.toByte() /* '-' */) {
            isNegative = true
            address++
        }

        while (address < endAddress) {
            byte = unsafe.getByte(address)
            address++

            if (byte == 13.toByte() /* \r */) {
                continue
            }

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

    var nameStartAddress: Long
    while (address < endAddress) {
        nameStartAddress = address
        val hash = readName()
        val value = readNumber()

        val stationData = stations.getOrPut(hash) { StationData(
            nameStartAddress,
            nameLength
        ) }

        if (value < stationData.min) { stationData.min = value }
        if (value > stationData.max) { stationData.max = value }
        stationData.sum += value
        stationData.count++
    }

    println(stations.mapKeys {
        val name = ByteArray(it.value.nameLength)
        unsafe.copyMemory(null, it.value.nameAddress, name, Unsafe.ARRAY_BYTE_BASE_OFFSET.toLong(), it.value.nameLength.toLong())
        String(name, StandardCharsets.UTF_8)
    }.entries.sortedBy { it.key }.joinToString(
        separator = ", ",
        prefix = "{",
        postfix = "}"
    ) { (name, data) ->
        "${name}=${data}"
    })

    printTime(startTime)

    val actual = stations.mapKeys {
        val name = ByteArray(it.value.nameLength)
        unsafe.copyMemory(null, it.value.nameAddress, name, Unsafe.ARRAY_BYTE_BASE_OFFSET.toLong(), it.value.nameLength.toLong())
        String(name, StandardCharsets.UTF_8)
    }.entries.sortedBy { it.key }.joinToString(
        separator = ", ",
        prefix = "{",
        postfix = "}"
    ) { (name, data) ->
        "${name}=${data}"
    }
    File(output).bufferedReader().use { reader ->
        val expected = reader.readText()
        println("Expected:\t $expected")
        println("Actual:\t\t $actual")
    }
}

