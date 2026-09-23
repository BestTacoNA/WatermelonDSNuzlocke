package me.magnum.melonds.impl.emulator.debug

internal object RendererFaithfulDiagnosticPayload {
    const val HEADER_WORDS = 64
    const val REGS_WORDS = 24592
    const val CAUSAL_WORDS_ABI5 = 1021992

    class Metadata internal constructor(private val header: IntArray, private val causalHeader: IntArray) {
        val frameId: Long get() = read64(header, 4)

        fun toJson(crc32: String): String {
            val fields = linkedMapOf<String, String>()
            fields["schema"] = "\"WFD1\""
            fields["rawFormat"] = "\"u32le\""
            fields["crc32"] = "\"$crc32\""
            val scalarFields = mapOf(
                1 to "version", 2 to "headerWords", 3 to "totalWords",
                8 to "outputWidth", 9 to "outputHeight", 10 to "regsOffsetWords",
                11 to "regsWords", 12 to "causalOffsetWords", 13 to "causalWords",
                14 to "causalAbi", 15 to "snapshotPublished", 24 to "sourcePolygonCount",
                25 to "sourceCaptureCnt", 26 to "sourceScreenSwap", 27 to "sourceIdentityValid",
                28 to "snapshotWidth", 29 to "snapshotHeight", 30 to "snapshotScreenSwap",
                31 to "snapshotZeroPolygons", 32 to "projectionSourceWidth",
                33 to "projectionSourceHeight", 34 to "projectionDestinationWidth",
                35 to "projectionDestinationHeight", 36 to "projectionOperation",
                37 to "nativeProjectionOperation", 38 to "outputScale", 39 to "internalScale",
                40 to "inputRingSlot", 41 to "previousRegsWords", 42 to "metaOffsetInRegsWords",
                43 to "metaWords", 44 to "currentRegsOffsetInRegsWords", 45 to "currentRegsWords",
                46 to "routeLineCount", 47 to "visiblePixelCount", 48 to "nativeWidth", 49 to "nativeHeight",
            )
            for ((index, key) in scalarFields) fields[key] = unsigned(header[index]).toString()
            for ((index, key) in mapOf(4 to "frameId", 6 to "publicationGeneration",
                16 to "snapshotFrameId", 18 to "snapshotPublicationGeneration",
                20 to "sourceRenderProductEpoch", 22 to "sourceSequence")) {
                fields[key] = java.lang.Long.toUnsignedString(read64(header, index))
            }
            fields["causalValidFlags"] = unsigned(causalHeader[0]).toString()
            fields["causalGeneration"] = java.lang.Long.toUnsignedString(read64(causalHeader, 2))
            fields["captureEpoch"] = java.lang.Long.toUnsignedString(read64(causalHeader, 5))
            fields["captureProductCount"] = unsigned(causalHeader[4]).toString()
            fields["routeOffsetInCausalWords"] = unsigned(causalHeader[9]).toString()
            fields["lineageOffsetInCausalWords"] = unsigned(causalHeader[10]).toString()
            fields["productOffsetInCausalWords"] = unsigned(causalHeader[11]).toString()
            return fields.entries.joinToString(prefix = "{\n", postfix = "\n}\n", separator = ",\n") {
                "  \"${it.key}\": ${it.value}"
            }
        }
    }

    fun parse(words: IntArray?, expectedFrameId: Long): Metadata? {
        if (words == null || words.size < HEADER_WORDS || expectedFrameId <= 0L) return null
        if (words[0] != 0x31444657 || words[1] != 1 || words[2] != HEADER_WORDS) return null
        if (read64(words, 4) != expectedFrameId || read64(words, 6) == 0L) return null
        if (words[3] != words.size || words[10] != HEADER_WORDS || words[11] != REGS_WORDS ||
            words[12] != HEADER_WORDS + REGS_WORDS || words[13] != CAUSAL_WORDS_ABI5 ||
            words[14] != 5 || words.size != HEADER_WORDS + REGS_WORDS + CAUSAL_WORDS_ABI5
        ) return null
        val scale = words[38]
        if (scale !in 1..8 || words[8] != 256 * scale || words[9] != 386 * scale) return null
        if (words[40] !in 0..2 || words[41] != 12288 || words[42] != 12288 || words[43] != 16 ||
            words[44] != 12304 || words[45] != 12288 || words[46] != 384 || words[47] != 98304 ||
            words[48] != 256 || words[49] != 192 || (50 until HEADER_WORDS).any { words[it] != 0 }
        ) return null
        val causal = words[12]
        if (words[causal + 1] != 5 || words[causal + 12] != CAUSAL_WORDS_ABI5 ||
            words[causal + 7] != 384 || words[causal + 8] != 98304
        ) return null
        for (index in 9..11) {
            if (words[causal + index] !in 16 until CAUSAL_WORDS_ABI5) return null
        }
        return Metadata(words.copyOfRange(0, HEADER_WORDS), words.copyOfRange(causal, causal + 16))
    }

    private fun unsigned(value: Int): Long = value.toLong() and 0xFFFFFFFFL
    private fun read64(words: IntArray, index: Int): Long =
        unsigned(words[index]) or (unsigned(words[index + 1]) shl 32)
}
