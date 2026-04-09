#pragma once

#include "device.h"

#include <ydb/library/pdisk_io/buffers.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_countedqueuemanyone.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/thread.h>

#include <atomic>
#include <functional>
#include <memory>
#include <utility>

namespace NMdsApp {

class IOperation {
public:
    virtual ~IOperation() = default;

    // The caller keeps the operation object alive until either OnCompletion()
    // or OnSubmitError() is called.
    virtual TStringBuf GetDevicePath() const = 0;

    virtual NKikimr::NPDisk::TReqId GetReqId() const {
        return {};
    }

    virtual NWilson::TTraceId* GetTraceIdPtr() {
        return nullptr;
    }

    virtual void Prepare(
        NKikimr::NPDisk::IAsyncIoContext& ioContext,
        NKikimr::NPDisk::IAsyncIoOperation* ioOperation) = 0;

    virtual void OnCompletion(NKikimr::NPDisk::TAsyncIoOperationResult* result) = 0;
    virtual void OnSubmitError(NKikimr::NPDisk::EIoResult result) = 0;
};

class TIOPoller {
private:
    using TIoCallback = std::function<void(NKikimr::NPDisk::EIoResult)>;

    static constexpr ui32 SubmitQueueSize = 4 << 10;
    static constexpr ui64 SetupMaxEvents = 32;
    static constexpr ui64 PollMaxEvents = 1;
    static constexpr ui64 IdleWaitTimeoutMs = 1;

    struct TSubmittedOperation {
        TDevice* Device = nullptr;
        IOperation* Operation = nullptr;
        NKikimr::NPDisk::IAsyncIoOperation* AsyncOperation = nullptr;
    };

    class TManagedOperationBase : public IOperation {
    public:
        TManagedOperationBase(TString path, TIoCallback callback)
            : Path_(std::move(path))
            , Callback_(std::move(callback))
        {
        }

        TStringBuf GetDevicePath() const override {
            return Path_;
        }

        void OnSubmitError(NKikimr::NPDisk::EIoResult result) override {
            if (result != NKikimr::NPDisk::EIoResult::Ok) {
                ReportError("submit", result);
            }
            Finish(result);
        }

        virtual TString ToString() const = 0;

    protected:
        void OnIoCompleted(NKikimr::NPDisk::EIoResult result, TStringBuf operationName) {
            if (result != NKikimr::NPDisk::EIoResult::Ok) {
                ReportError(operationName, result);
            }
            Finish(result);
        }

    private:
        void ReportError(TStringBuf stage, NKikimr::NPDisk::EIoResult result) const {
            Cerr << "async io " << stage << " failed for " << Path_.Quote()
                << ", result# " << static_cast<i64>(result) << " ToString# " << ToString() << Endl;
        }

        void Finish(NKikimr::NPDisk::EIoResult result) {
            try {
                if (Callback_) {
                    Callback_(result);
                }
            } catch (...) {
                Cerr << "async io callback failed for " << Path_.Quote()
                    << ": " << CurrentExceptionMessage() << Endl;
            }
            delete this;
        }

    private:
        TString Path_;
        TIoCallback Callback_;
    };

    class TPreadOperation final : public TManagedOperationBase {
    public:
        TPreadOperation(TString path, TString* data, size_t count, size_t offset, TIoCallback callback)
            : TManagedOperationBase(std::move(path), std::move(callback))
            , Data_(data)
            , Count_(count)
            , Offset_(offset)
        {
            if (!Data_) {
                ythrow yexception() << "pread destination buffer is null";
            }

            ResizeUninitialized(*Data_, Count_);
        }

        void Prepare(
            NKikimr::NPDisk::IAsyncIoContext& ioContext,
            NKikimr::NPDisk::IAsyncIoOperation* ioOperation) override
        {
            ioContext.PreparePRead(ioOperation, Data_->Detach(), Count_, Offset_);
        }

        void OnCompletion(NKikimr::NPDisk::TAsyncIoOperationResult* result) override {
            OnIoCompleted(result->Result, "pread");
        }

        TString ToString() const override {
            const TString path(GetDevicePath());
            return TStringBuilder()
                << "TPreadOperation"
                << " Path# " << path.Quote()
                << " Count# " << Count_
                << " Offset# " << Offset_;
        }

    private:
        TString* Data_;
        size_t Count_;
        size_t Offset_;
    };

    class TPwriteOperation final : public TManagedOperationBase {
    public:
        TPwriteOperation(TString path, NKikimr::NPDisk::TAlignedData data, size_t count, size_t offset, TIoCallback callback)
            : TManagedOperationBase(std::move(path), std::move(callback))
            , Data_(std::move(data))
            , Count_(count)
            , Offset_(offset)
        {
        }

        void Prepare(
            NKikimr::NPDisk::IAsyncIoContext& ioContext,
            NKikimr::NPDisk::IAsyncIoOperation* ioOperation) override
        {
            ioContext.PreparePWrite(ioOperation, Data_.Get(), Count_, Offset_);
        }

        void OnCompletion(NKikimr::NPDisk::TAsyncIoOperationResult* result) override {
            OnIoCompleted(result->Result, "pwrite");
        }

        TString ToString() const override {
            const TString path(GetDevicePath());
            return TStringBuilder()
                << "TPwriteOperation"
                << " Path# " << path.Quote()
                << " Count# " << Count_
                << " Offset# " << Offset_
                << " BufferSize# " << Data_.Size();
        }

    private:
        NKikimr::NPDisk::TAlignedData Data_;
        size_t Count_;
        size_t Offset_;
    };

    class TSharedCallback final : public NKikimr::NPDisk::ICallback {
    public:
        explicit TSharedCallback(TIOPoller& owner)
            : Owner_(owner)
        {
        }

        void Exec(NKikimr::NPDisk::TAsyncIoOperationResult* result) override {
            Owner_.HandleCompletion(*result);
        }

    private:
        TIOPoller& Owner_;
    };

    class TSubmitThread final : public ISimpleThread {
    public:
        explicit TSubmitThread(TIOPoller& owner)
            : Owner_(owner)
        {
        }

        void* ThreadProc() override {
            TThread::SetCurrentThreadName("MdsIoSubmit");
            Owner_.RunSubmitLoop();
            return nullptr;
        }

    private:
        TIOPoller& Owner_;
    };

    class TGetEventsThread final : public ISimpleThread {
    public:
        explicit TGetEventsThread(TIOPoller& owner)
            : Owner_(owner)
        {
        }

        void* ThreadProc() override {
            TThread::SetCurrentThreadName("MdsIoGet");
            Owner_.RunGetEventsLoop();
            return nullptr;
        }

    private:
        TIOPoller& Owner_;
    };

public:
    explicit TIOPoller(TVector<TDevice> devices)
        : Devices_(std::move(devices))
        , SharedCallback_(std::make_unique<TSharedCallback>(*this))
        , SubmitThread_(*this)
        , GetEventsThread_(*this)
    {
        if (Devices_.empty()) {
            ythrow yexception() << "TIOPoller requires at least one device";
        }

        BuildDeviceIndex();
        SetupDevices();

        SubmitThread_.Start();
        GetEventsThread_.Start();
        Started_ = true;
    }

    ~TIOPoller() {
        Stop();
        DestroyDevices();
    }

    TIOPoller(const TIOPoller&) = delete;
    TIOPoller& operator=(const TIOPoller&) = delete;

    TIOPoller(TIOPoller&&) = delete;
    TIOPoller& operator=(TIOPoller&&) = delete;

    bool Submit(IOperation* op) {
        if (!op || Stopping_.load(std::memory_order_acquire) || Failed_.load(std::memory_order_acquire)) {
            return false;
        }

        TDevice* device = FindDevice(op->GetDevicePath());
        if (!device || !device->IoCtx_) {
            return false;
        }

        auto submitted = std::make_unique<TSubmittedOperation>();
        submitted->Device = device;
        submitted->Operation = op;
        submitted->AsyncOperation = device->IoCtx_->CreateAsyncIoOperation(
            submitted.get(),
            op->GetReqId(),
            op->GetTraceIdPtr());

        if (!submitted->AsyncOperation) {
            return false;
        }

        try {
            op->Prepare(*device->IoCtx_, submitted->AsyncOperation);
        } catch (...) {
            device->IoCtx_->DestroyAsyncIoOperation(submitted->AsyncOperation);
            throw;
        }

        InFlight_.fetch_add(1, std::memory_order_acq_rel);
        SubmitQueue_.Push(submitted.release());
        return true;
    }

private:
    void BuildDeviceIndex() {
        DevicesByPath_.reserve(Devices_.size());

        for (TDevice& device : Devices_) {
            if (!device.IoCtx_) {
                ythrow yexception() << "device " << device.Path_.Quote() << " does not have IAsyncIoContext";
            }

            const TStringBuf path = device.Path_;
            const bool inserted = DevicesByPath_.emplace(path, &device).second;
            if (!inserted) {
                ythrow yexception() << "duplicate device path " << device.Path_.Quote();
            }
        }
    }

    void SetupDevices() {
        size_t configured = 0;
        try {
            for (; configured < Devices_.size(); ++configured) {
                TDevice& device = Devices_[configured];
                const auto result = device.IoCtx_->Setup(SetupMaxEvents, false);
                if (result != NKikimr::NPDisk::EIoResult::Ok) {
                    ythrow yexception() << "failed to setup async io context for " << device.Path_.Quote()
                        << ", result# " << static_cast<i64>(result);
                }
            }
        } catch (...) {
            for (size_t i = 0; i < configured; ++i) {
                Devices_[i].IoCtx_->Destroy();
            }
            throw;
        }
    }

    void Stop() {
        if (!Started_) {
            return;
        }

        Stopping_.store(true, std::memory_order_release);
        SubmitQueue_.WakeUp();

        SubmitThread_.Join();
        GetEventsThread_.Join();
        Started_ = false;
    }

    void DestroyDevices() {
        for (TDevice& device : Devices_) {
            if (!device.IoCtx_) {
                continue;
            }

            const auto result = device.IoCtx_->Destroy();
            if (result != NKikimr::NPDisk::EIoResult::Ok) {
                Cerr << "failed to destroy async io context for " << device.Path_
                    << ", result# " << static_cast<i64>(result) << Endl;
            }
        }
    }

    TDevice* FindDevice(TStringBuf path) {
        const auto it = DevicesByPath_.find(path);
        return it == DevicesByPath_.end() ? nullptr : it->second;
    }

    bool ShouldStopGetEventsThread() const {
        if (!Stopping_.load(std::memory_order_acquire)) {
            return false;
        }

        if (Failed_.load(std::memory_order_acquire)) {
            return true;
        }

        return InFlight_.load(std::memory_order_acquire) == 0;
    }

    void RunSubmitLoop() {
        while (true) {
            const TAtomicBase queued = SubmitQueue_.GetWaitingSize();
            if (queued > 0) {
                for (TAtomicBase i = 0; i < queued; ++i) {
                    TSubmittedOperation* submitted = SubmitQueue_.Pop();
                    if (Failed_.load(std::memory_order_acquire)) {
                        FailSubmittedOperation(submitted, NKikimr::NPDisk::EIoResult::IOError);
                    } else {
                        SubmitOperation(submitted);
                    }
                }
                continue;
            }

            if (Stopping_.load(std::memory_order_acquire)) {
                return;
            }

            SubmitQueue_.ProducedWaitI();
        }
    }

    void SubmitOperation(TSubmittedOperation* submitted) {
        auto* const ioCtx = submitted->Device->IoCtx_.get();

        auto result = NKikimr::NPDisk::EIoResult::TryAgain;
        while (result == NKikimr::NPDisk::EIoResult::TryAgain
                && !Stopping_.load(std::memory_order_acquire)) {
            result = ioCtx->Submit(submitted->AsyncOperation, SharedCallback_.get());
        }

        if (result == NKikimr::NPDisk::EIoResult::Ok) {
            return;
        }

        if (result != NKikimr::NPDisk::EIoResult::TryAgain) {
            RecordError(TStringBuilder()
                << "failed to submit async io operation for " << submitted->Device->Path_.Quote()
                << ", result# " << static_cast<i64>(result));
        }

        FailSubmittedOperation(submitted, result);
    }

    void FailSubmittedOperation(TSubmittedOperation* submitted, NKikimr::NPDisk::EIoResult result) {
        try {
            submitted->Operation->OnSubmitError(result);
        } catch (...) {
            RecordError(TStringBuilder() << "submit error callback failed: " << CurrentExceptionMessage());
        }

        submitted->Device->IoCtx_->DestroyAsyncIoOperation(submitted->AsyncOperation);
        delete submitted;
        InFlight_.fetch_sub(1, std::memory_order_acq_rel);
    }

    void RunGetEventsLoop() {
        size_t nextDevice = 0;
        NKikimr::NPDisk::TAsyncIoOperationResult event;

        while (!ShouldStopGetEventsThread()) {
            bool madeProgress = false;

            for (size_t scanned = 0; scanned < Devices_.size(); ++scanned) {
                TDevice& device = Devices_[nextDevice];
                nextDevice = (nextDevice + 1) % Devices_.size();

                // Current OSS backend is libaio, and it requires maxEvents > 0 even for polling.
                const i64 ret = device.IoCtx_->GetEvents(0, PollMaxEvents, &event, TDuration::Zero());

                if (ret == -static_cast<i64>(NKikimr::NPDisk::EIoResult::InterruptedSystemCall)) {
                    continue;
                }

                if (ret < 0) {
                    RecordError(TStringBuilder()
                        << "failed to get async io events for " << device.Path_.Quote()
                        << ", result# " << -ret);
                    break;
                }

                if (ret > 0) {
                    madeProgress = true;
                }
            }

            if (!madeProgress && !ShouldStopGetEventsThread()) {
                Sleep(TDuration::MilliSeconds(IdleWaitTimeoutMs));
            }
        }
    }

    void HandleCompletion(NKikimr::NPDisk::TAsyncIoOperationResult& result) {
        auto* const submitted = static_cast<TSubmittedOperation*>(result.Operation->GetCookie());

        try {
            submitted->Operation->OnCompletion(&result);
        } catch (...) {
            RecordError(TStringBuilder() << "completion callback failed: " << CurrentExceptionMessage());
        }

        submitted->Device->IoCtx_->DestroyAsyncIoOperation(result.Operation);
        delete submitted;
        InFlight_.fetch_sub(1, std::memory_order_acq_rel);
    }

    void RecordError(TString message) {
        bool expected = false;
        if (Failed_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            Cerr << message << Endl;
        }

        Stopping_.store(true, std::memory_order_release);
        SubmitQueue_.WakeUp();
    }

public: 

    void Pread(const TString& path, TString* data, size_t count, size_t offset, TIoCallback callback) {
        auto op = std::make_unique<TPreadOperation>(path, data, count, offset, std::move(callback));
        SubmitOwnedOperation(std::move(op), "pread");
    }

    void Pwrite(const TString& path, NKikimr::NPDisk::TAlignedData data, size_t count, size_t offset, TIoCallback callback) {
        if (count > data.Size()) {
            ythrow yexception() << "pwrite count# " << count << " exceeds buffer size# " << data.Size();
        }

        auto op = std::make_unique<TPwriteOperation>(path, std::move(data), count, offset, std::move(callback));
        SubmitOwnedOperation(std::move(op), "pwrite");
    }

private:
    void SubmitOwnedOperation(std::unique_ptr<IOperation> op, TStringBuf operationName) {
        const TString path(op->GetDevicePath());
        if (!Submit(op.get())) {
            ythrow yexception() << "failed to enqueue " << operationName
                << " for device " << path.Quote();
        }

        op.release();
    }

    TVector<TDevice> Devices_;
    THashMap<TStringBuf, TDevice*> DevicesByPath_;
    NKikimr::NPDisk::TCountedQueueManyOne<TSubmittedOperation, SubmitQueueSize> SubmitQueue_;
    std::unique_ptr<TSharedCallback> SharedCallback_;
    TSubmitThread SubmitThread_;
    TGetEventsThread GetEventsThread_;
    std::atomic<bool> Stopping_{false};
    std::atomic<bool> Failed_{false};
    std::atomic<ui64> InFlight_{0};
    bool Started_ = false;
};

} // namespace NMdsApp
