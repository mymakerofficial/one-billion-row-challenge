package de.maiker.bench.memorySegment

import java.io.RandomAccessFile
import java.lang.foreign.Arena
import java.lang.foreign.ValueLayout
import java.nio.channels.FileChannel


fun main() {
    val startTime = System.nanoTime()

    val channel = RandomAccessFile("./measurements.txt", "r").getChannel()
    val memorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofShared())

    for (index in 1 until channel.size()) {
        val value = memorySegment.getAtIndex(ValueLayout.JAVA_BYTE, index)
    }

    val endTime = System.nanoTime()
    println("Took ${(endTime - startTime) / 1_000_000} ms")
}

