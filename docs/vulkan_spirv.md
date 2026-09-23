# Vulkan SPIR-V Regeneration
## Automatic Flow (recommended)

Vulkan shader headers are regenerated automatically during native/app builds:

- CMake target: `regenerate_vulkan_spirv_headers`
- Gradle task: `:app:regenerateVulkanSpirv`
- `preBuild` depends on `:app:regenerateVulkanSpirv`

This means a normal app build updates out-of-date `*ShaderData.h` automatically.

## Manual Commands

To force regeneration of Vulkan shader headers from `.comp`, `.vert`, and `.frag` sources:

```bash
./gradlew :app:regenerateVulkanSpirv
```

To verify that committed headers are in sync (without writing files), run:

```bash
./gradlew :app:checkVulkanSpirv
```

Requirements:

- `glslc` (preferred), or `glslangValidator`
- `xxd`

Compiler detection order in `scripts/regenerate_vulkan_spirv.sh`:

1. `glslc` in `PATH`
2. `$ANDROID_NDK_HOME/shader-tools/*/glslc` (or `$ANDROID_NDK_ROOT/...`)
3. `glslangValidator` in `PATH`
