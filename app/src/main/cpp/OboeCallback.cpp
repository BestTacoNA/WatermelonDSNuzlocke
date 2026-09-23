#include "OboeCallback.h"
#include "MelonDS.h"
#include "types.h"
#include <algorithm>
#include <chrono>
#include <utility>

using namespace melonDS;

OboeCallback::OboeCallback(
    int volume,
    void (*onErrorCallback)(oboe::AudioStream*, std::uint64_t),
    std::uint64_t streamGeneration,
    std::shared_ptr<MelonDSAndroid::AudioOutputCapture> capture
) : _volume(volume),
    onErrorCallback(onErrorCallback),
    streamGeneration(streamGeneration),
    audioOutputCapture(std::move(capture)) {
}

oboe::DataCallbackResult
OboeCallback::onAudioReady(oboe::AudioStream *stream, void *audioData, int32_t numFrames) {
    const bool captureRequested =
        audioOutputCapture && audioOutputCapture->tryBeginCallback(streamGeneration);
    const auto callbackStartNs = captureRequested
        ? std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now().time_since_epoch()).count()
        : 0;
    auto currentInstance = activeInstance.lock();
    MelonDSAndroid::AudioOutputAdaptiveSnapshot before {};
    MelonDSAndroid::AudioOutputAdaptiveSnapshot after {};
    MelonDSAndroid::AudioOutputControllerSnapshot controller {};
    melonDS::AudioOutputDrainObservation drain {};
    int readFrames = 0;

    if (!currentInstance)
    {
        std::fill_n(static_cast<s16*>(audioData), numFrames * 2, 0);
        drain.valid = true;
        drain.requestedFrames = static_cast<std::uint32_t>(numFrames);
        drain.returnedFrames = static_cast<std::uint32_t>(numFrames);
        drain.inactiveZeroFrames = static_cast<std::uint32_t>(numFrames);
    }
    else
    {
        if (captureRequested)
            before = currentInstance->getAudioOutputAdaptiveSnapshot();
        readFrames = currentInstance->readAudioOutputAdaptivo(
            (s16*) audioData, numFrames, captureRequested ? &drain : nullptr);
        if (captureRequested)
        {
            after = currentInstance->getAudioOutputAdaptiveSnapshot();
            controller = currentInstance->getAudioOutputControllerSnapshot();
        }
    }

    if (MelonDSAndroid::isFastForwardActive() && MelonDSAndroid::isMuteOnFastForward())
    {

        std::fill_n(static_cast<s16*>(audioData), numFrames * 2, 0);
    }
    else if (_volume < 256)
    {
        s16* samples = (s16*) audioData;
        for (int i = 0; i < numFrames * 2; i++)
            samples[i] = ((s32) samples[i] * _volume) >> 8;
    }

    if (captureRequested) [[unlikely]]
    {
        const auto pcmReadyNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();

        const oboe::ResultWithValue<int32_t> xruns = stream->getXRunCount();
        const std::int32_t xrunCount = xruns ? xruns.value() : -1;
        audioOutputCapture->appendAndEndCallback(
            static_cast<const std::int16_t*>(audioData),
            numFrames,
            readFrames,
            streamGeneration,
            _volume,
            currentInstance != nullptr,
            callbackStartNs,
            pcmReadyNs,
            before,
            after,
            controller,
            drain,
            xrunCount);
    }

    return oboe::DataCallbackResult::Continue;
}

void OboeCallback::onErrorAfterClose(oboe::AudioStream* stream, oboe::Result result)
{
    if (result == oboe::Result::ErrorDisconnected && onErrorCallback != nullptr) {
        onErrorCallback(stream, streamGeneration);
    }
}
