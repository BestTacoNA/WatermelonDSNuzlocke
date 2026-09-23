package me.magnum.melonds.impl

import java.io.File
import java.io.InputStream
import java.io.OutputStream

internal fun copySaveWithSnapshot(
    cacheDirectory: File,
    maxBytes: Long,
    openSource: () -> InputStream,
    openTarget: () -> OutputStream,
) {
    val snapshot = File.createTempFile("save-import-", ".tmp", cacheDirectory)
    try {
        openSource().use { input ->
            snapshot.outputStream().use { output ->
                val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                var size = 0L
                while (true) {
                    val count = input.read(buffer)
                    if (count < 0) break
                    size += count
                    require(size <= maxBytes) { "Selected save file is too large" }
                    output.write(buffer, 0, count)
                }
                require(size > 0) { "Selected save file is empty" }
            }
        }
        snapshot.inputStream().use { input ->
            openTarget().use { output -> input.copyTo(output) }
        }
    } finally {
        snapshot.delete()
    }
}
