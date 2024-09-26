package de.maiker.bench.bits

import sun.misc.Unsafe
import java.io.RandomAccessFile
import java.lang.foreign.Arena
import java.nio.channels.FileChannel

@OptIn(ExperimentalStdlibApi::class)
fun main() {
    val unsafe = Unsafe::class.java.getDeclaredField("theUnsafe").let {
        it.isAccessible = true
        it.get(null) as Unsafe
    }

    val channel = RandomAccessFile("./test.txt", "r").getChannel()
    val memorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofShared())

    val start = memorySegment.address()
    val end = start + memorySegment.byteSize()

    for (address in start until end) {
        val long = unsafe.getLong(address) // least significant byte first
        val continuation = long and 0b1000_0000 // is the continuation bit
        // either read 1 byte or 2 bytes
        val char = if (continuation == 0L) {
            long and 0b0111_1111
        } else {
            val byte1 = long and 0b0011_1111
            val byte2 = long shr 8 and 0b0011_1111
            (byte1 and 0x1F) shl 6 or (byte2 and 0x3F) // 0x1F = 0001 1111, 0x3F = 0011 1111
        }

        println("${address.toHexString()} -> ${long.toHexString()} : ${continuation.toHexString()} : ${char.toHexString()} : ${char.toInt().toChar()}")
    }
}

