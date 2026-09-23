package me.magnum.melonds.impl.emulator.debug

import java.io.OutputStream

internal object RendererDebugRawWriter {

    fun write(values: IntArray, output: OutputStream) {
        val bytes = ByteArray(8192)
        var wordIndex = 0
        while (wordIndex < values.size) {
            val wordCount = minOf(bytes.size / Int.SIZE_BYTES, values.size - wordIndex)
            for (index in 0 until wordCount) {
                val value = values[wordIndex + index]
                val offset = index * Int.SIZE_BYTES
                bytes[offset] = value.toByte()
                bytes[offset + 1] = (value ushr 8).toByte()
                bytes[offset + 2] = (value ushr 16).toByte()
                bytes[offset + 3] = (value ushr 24).toByte()
            }
            output.write(bytes, 0, wordCount * Int.SIZE_BYTES)
            wordIndex += wordCount
        }
    }
}
