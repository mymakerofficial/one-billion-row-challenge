package de.maiker.concurrent

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

// store numbers as int to save memory
data class StationData(
    var count: Int,
    var sum: Int,
    var min: Int,
    var max: Int
) {
    constructor(value: Int) : this(1, value, value, value)

    fun update(value: Int) {
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
        return "%.1f/%.1f/%.1f".format(min / 10.0, sum / 10.0 / count, max / 10.0)
    }
}

class Result {
    private val stationMap = HashMap<Int, String>(10000, 1f)
    private val result = HashMap<Int, StationData>(10000, 1f)

    fun add(hash: Int, station: String, number: Int) {
        val values = result[hash] // this is very slow

        if (values != null) {
            values.update(number)
            return
        }

        stationMap[hash] = station
        result[hash] = StationData(number)
    }

    fun merge(other: Result) {
        other.stationMap.forEach { (hash, station) ->
            stationMap[hash] = station
        }
        other.result.forEach { (station, data) ->
            result.merge(station, data) { a, b ->
                a.count += b.count
                a.sum += b.sum
                a.min = if (a.min < b.min) a.min else b.min
                a.max = if (a.max > b.max) a.max else b.max
                a
            }
        }
    }

    override fun toString(): String {
        return result
            .mapKeys { stationMap[it.key]!! }
            .entries
            .sortedBy { it.key }
            .joinToString(
                separator = ", ",
                prefix = "{",
                postfix = "}"
            ) { (station, data) ->
                "$station=${data}"
            }
    }

    fun size(): Int {
        return result.size
    }
}

// find the position of the next line break
fun findNextEndOfLine(channel: FileChannel, initialPosition: Long): Long {
    var position = initialPosition

    // create a buffer to hold the current byte
    val buffer = ByteBuffer.allocate(1)

    while (position < channel.size()) {
        // read the next byte
        channel.read(buffer, position)

        position++

        // check if the byte is a line break
        if (buffer.get(0).toInt() == 10 /* \n */) {
            return position
        }

        buffer.clear()
    }

    // in case we didn't find a line break we return the end of the file
    return channel.size()
}

fun getBuffers(channel: FileChannel, chunks: Int): Array<MappedByteBuffer> {
    val chunkSize = channel.size() / chunks

    var position = 0L

    return Array(chunks) {
        // we can't split the chunks at random positions so we need to find the next line break
        val endPosition = findNextEndOfLine(channel, position + chunkSize)
        val size = endPosition - position

        // map the chunk to memory
        val buffer = channel.map(
            FileChannel.MapMode.READ_ONLY,
            position,
            size
        )

        position += size

        buffer
    }
}

fun parseNumberFromByteBuffer(buffer: MappedByteBuffer): Int {
    var number = 0
    var sign = 1

    while (buffer.hasRemaining()) {
        val charInt = buffer.get().toInt()

        // fuck you windows
        if (charInt == 13 /* \r */) {
            continue
        }

        if (charInt == 10 /* \n */) {
            break
        }

        if (charInt == 45 /* - */) {
            sign = -1
            continue
        }

        if (charInt == 46 /* . */) {
            continue
        }

        number = number * 10 + (charInt - 48 /* '0' */)
    }

    return number * sign
}

suspend fun calculateMeasurements(buffers: Array<MappedByteBuffer>): Result {
    val result = coroutineScope {
        buffers.map { buffer ->
            async {
                val partialResult = Result()

                val bytes = ByteArray(128)
                var hash = 5381
                var position = 0
                while (buffer.hasRemaining()) {
                    val char = buffer.get()
                    if (char.toInt() == 59 /* ; */) {
                        val station = String(
                            bytes,
                            offset = 0,
                            length = position,
                        )

                        val value = parseNumberFromByteBuffer(buffer)

                        partialResult.add(hash, station, value)

                        hash = 5381
                        position = 0
                    } else {
                        hash = (hash * 33) xor char.toInt()
                        bytes[position++] = char
                    }
                }

                partialResult
            }
        }.awaitAll().fold(Result()) { acc, result ->
            acc.merge(result)
            acc
        }
    }

    return result
}

suspend fun main() {
    val filePath = Paths.get("./measurements.txt")
    val chunks = Runtime.getRuntime().availableProcessors()

    val startTime = System.nanoTime()

    val channel = FileChannel.open(filePath, StandardOpenOption.READ)

    val buffers = getBuffers(channel, chunks)

    val result = calculateMeasurements(buffers)

    print(result)

    val endTime = System.nanoTime()

    println("\n\nProcessed ${channel.size() / 1_000_000_000.0}GB in ${(endTime - startTime) / 1_000_000_000.0}s using $chunks threads.")
}