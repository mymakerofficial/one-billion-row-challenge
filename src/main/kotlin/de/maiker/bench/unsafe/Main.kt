package de.maiker.bench.unsafe

import sun.misc.Unsafe
import java.io.RandomAccessFile
import java.lang.foreign.Arena
import java.nio.channels.FileChannel

fun main() {
    val startTime = System.nanoTime()

    val unsafe = Unsafe::class.java.getDeclaredField("theUnsafe").let {
        it.isAccessible = true
        it.get(null) as Unsafe
    }

    val channel = RandomAccessFile("./measurements-small.txt", "r").getChannel()
    val memorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofShared())

    val start = memorySegment.address()
    val end = start + memorySegment.byteSize()

    for (address in start until end) {
        val value = unsafe.getByte(address)
    }

    val endTime = System.nanoTime()
    println("Took ${(endTime - startTime) / 1_000_000} ms")
}

