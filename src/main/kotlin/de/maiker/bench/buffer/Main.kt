package de.maiker.bench.buffer

import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel



fun main() {
    val startTime = System.nanoTime()

    val channel: FileChannel = FileInputStream(File("./measurements.txt")).getChannel()
    val fileSize = channel.size()

    // Use a fixed buffer size
    val bufferSize = 1024 * 1024 * 1024 // 1GB buffer
    val buffer: ByteBuffer = ByteBuffer.allocateDirect(bufferSize)

    var totalBytesRead: Long = 0
    var bytesRead: Int
    while (totalBytesRead < fileSize) {
        buffer.clear()
        bytesRead = channel.read(buffer)
        if (bytesRead == -1) break

        buffer.flip()

        // Process bytes here if needed
        while (buffer.hasRemaining()) {
            val byte: Byte = buffer.get()
            // Do something with the byte if necessary
        }

        totalBytesRead += bytesRead.toLong()
    }

    val endTime = System.nanoTime()
    println("Took ${(endTime - startTime) / 1_000_000} ms")
}