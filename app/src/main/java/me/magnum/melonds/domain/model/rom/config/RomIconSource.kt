package me.magnum.melonds.domain.model.rom.config

enum class RomIconSource {
    DEFAULT,
    NATIVE,
    RETRO_ACHIEVEMENTS;

    fun usesRetroAchievements(globalEnabled: Boolean): Boolean = when (this) {
        DEFAULT -> globalEnabled
        NATIVE -> false
        RETRO_ACHIEVEMENTS -> true
    }
}
