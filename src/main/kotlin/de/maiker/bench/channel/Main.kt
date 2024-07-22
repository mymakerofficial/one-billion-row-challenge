package de.maiker.bench.channel

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

fun main() {
    val startTime = System.nanoTime()

    val channel = RandomAccessFile("./measurements.txt", "r").getChannel()
    val mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())

    while (mappedByteBuffer.hasRemaining()) {
        val value = mappedByteBuffer.get()
    }

    val endTime = System.nanoTime()
    println("Took ${(endTime - startTime) / 1_000_000} ms")
}

