package de.maiker.incremental.attempt8

import de.maiker.util.printTime
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import sun.misc.Unsafe
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

class Reader(
    startAddress: Long,
    private val endAddress: Long,
    isFileStart: Boolean = false
) {
    companion object {
        val SEMICOLON = 59.toByte() /* ';' */
        val DASH = 45.toByte() /* '-' */
        val NEWLINE = 10.toByte() /* \n */
        val DOT = 46.toByte() /* '.' */
        val ARRAY_BYTE_BASE_OFFSET = Unsafe.ARRAY_BYTE_BASE_OFFSET.toLong()

        val unsafe = Unsafe::class.java.getDeclaredField("theUnsafe").let {
            it.isAccessible = true
            it.get(Unsafe::class) as Unsafe
        }
    }

    private var address = startAddress
    private var byte: Byte = 0

    init {
        if (!isFileStart) {
            // walk to the next line
            while (this.hasRemaining()) {
                byte = unsafe.getByte(address)
                address++

                if (byte == NEWLINE) {
                    break
                }
            }
        }
    }

    fun hasRemaining(): Boolean = address < endAddress

    private var nameStartAddress = 0L
    private var nameLength = 0L
    private var hash = 5381
    fun parseName(): Int {
        nameStartAddress = address
        nameLength = 0
        hash = 5381
        // we dont check if we have left our chunk because we want to finish reading
        while (true) {
            byte = unsafe.getByte(address)
            address++

            if (byte == SEMICOLON) {
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
        if (byte == DASH) {
            isNegative = true
            address++
        }

        // we dont check if we have left our chunk because we want to finish reading
        while (true) {
            byte = unsafe.getByte(address)
            address++

            if (byte == NEWLINE) {
                break
            }

            if (byte == DOT) {
                continue
            }

            number = number * 10 + (byte - 48 /* '0' */)
        }

        return if (isNegative) -number else number
    }
}

suspend fun main() = coroutineScope {
    val startTime = System.nanoTime()

    val channel = RandomAccessFile("measurements.txt", "r").getChannel()
    val memorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofShared())

    val startAddress = memorySegment.address()
    val endAddress = startAddress + memorySegment.byteSize()

    val chunkCount = Runtime.getRuntime().availableProcessors()
    val chunkSize = channel.size() / chunkCount

    (0..chunkCount)
        .map { index -> async {
            val chunkStart = startAddress + index * chunkSize
            val chunkEnd = minOf(chunkStart + chunkSize, endAddress)
            processChunk(chunkStart, chunkEnd, isFileStart = index == 0)
        } }
        .awaitAll()
        .fold(mutableMapOf(), ::merge)
        .mapKeys { it.value.name }
        .toSortedMap()
        .also { println(it) }

    printTime(startTime)
}

fun processChunk(startAddress: Long, endAddress: Long, isFileStart: Boolean): MutableMap<Int, StationData> {
    val reader = Reader(startAddress, endAddress, isFileStart)
    val stations = mutableMapOf<Int, StationData>()

    while (reader.hasRemaining()) {
        val hash = reader.parseName()
        val value = reader.readNumber()

        val stationData = stations.getOrPut(hash) {
            StationData(name = reader.getName())
        }

        if (value < stationData.min) { stationData.min = value }
        if (value > stationData.max) { stationData.max = value }
        stationData.sum += value
        stationData.count++
    }

    return stations
}

fun merge(acc: MutableMap<Int, StationData>, chunkStations: MutableMap<Int, StationData>): MutableMap<Int, StationData> {
    chunkStations.forEach { (hash, stationData) ->
        val accStationData = acc.getOrPut(hash) { stationData }

        if (stationData.min < accStationData.min) { accStationData.min = stationData.min }
        if (stationData.max > accStationData.max) { accStationData.max = stationData.max }
        accStationData.sum += stationData.sum
        accStationData.count += stationData.count
    }

    return acc
}