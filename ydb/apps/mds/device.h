#pragma once

#include <ydb/library/pdisk_io/aio.h>

#include <util/generic/string.h>

#include <memory>
#include <utility>

namespace NMdsApp {

class TIOPoller;

class TDevice {
public:
    TDevice() = default;

    TDevice(TString path, TString serial, std::unique_ptr<NKikimr::NPDisk::IAsyncIoContext> ioCtx)
        : Path_(std::move(path))
        , Serial_(std::move(serial))
        , IoCtx_(std::move(ioCtx))
    {
    }

    TDevice(TDevice&&) noexcept = default;
    TDevice& operator=(TDevice&&) noexcept = default;

    TDevice(const TDevice&) = delete;
    TDevice& operator=(const TDevice&) = delete;

    const TString& GetPath() const {
        return Path_;
    }

    const TString& GetSerial() const {
        return Serial_;
    }

    bool HasIoContext() const {
        return static_cast<bool>(IoCtx_);
    }

private:
    TString Path_;
    TString Serial_;
    std::unique_ptr<NKikimr::NPDisk::IAsyncIoContext> IoCtx_;

    friend class TIOPoller;
};

} // namespace NMdsApp
