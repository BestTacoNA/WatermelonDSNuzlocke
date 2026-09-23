#ifndef MELONDS_OBOECALLBACK_H
#define MELONDS_OBOECALLBACK_H

#include "AudioOutputCapture.h"
#include "MelonInstance.h"

#include <oboe/Oboe.h>
#include <cstdint>
#include <memory>

class OboeCallback : public oboe::AudioStreamCallback {
private:
    int _volume;
    void (*onErrorCallback)(oboe::AudioStream*, std::uint64_t);
    std::uint64_t streamGeneration;
    std::shared_ptr<MelonDSAndroid::AudioOutputCapture> audioOutputCapture;

public:
    std::weak_ptr<MelonDSAndroid::MelonInstance> activeInstance;

    OboeCallback(
        int volume,
        void (*onErrorCallback)(oboe::AudioStream*, std::uint64_t),
        std::uint64_t streamGeneration,
        std::shared_ptr<MelonDSAndroid::AudioOutputCapture> audioOutputCapture
    );
    oboe::DataCallbackResult onAudioReady(oboe::AudioStream *stream, void *audioData, int32_t numFrames) override;
    void onErrorAfterClose(oboe::AudioStream* stream, oboe::Result result) override;
};


#endif //MELONDS_OBOECALLBACK_H
