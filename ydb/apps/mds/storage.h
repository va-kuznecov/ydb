#pragma once

#include "io_poller.h"

#include <rocksdb/db.h>

#include <library/cpp/json/fast_sax/unescape.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/writer/json_value.h>

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/align.h>

#include <deque>
#include <cstring>
#include <memory>
#include <exception>
#include <future>
#include <mutex>
#include <utility>

namespace NMdsApp {

namespace NStorageDetail {

class THDDWarehouse {
public:
    THDDWarehouse(TIOPoller& ioPoller, TString path)
        : IoPoller_(ioPoller)
        , Path_(std::move(path))
    {
    }

    THDDWarehouse(const THDDWarehouse&) = delete;
    THDDWarehouse& operator=(const THDDWarehouse&) = delete;
    THDDWarehouse(THDDWarehouse&&) = delete;
    THDDWarehouse& operator=(THDDWarehouse&&) = delete;

    ~THDDWarehouse() = default;

    ui64 Reserve(size_t size) {
        const ui64 offset = NextOffset_;
        NextOffset_ += size;
        NextOffset_ = AlignUp<ui64>(NextOffset_, 4096);
        return offset;
    }

    const TString& GetPath() const {
        return Path_;
    }

    std::future<TString> ReadAsync(ui64 offset, size_t size) {
        auto promise = std::make_shared<std::promise<TString>>();
        auto future = promise->get_future();

        if (size == 0) {
            promise->set_value(TString());
            return future;
        }

        auto data = std::make_shared<TString>();

        try {
            IoPoller_.Pread(
                Path_,
                data.get(),
                size,
                offset,
                [promise, data, path = Path_](NKikimr::NPDisk::EIoResult result) mutable {
                    try {
                        if (result != NKikimr::NPDisk::EIoResult::Ok) {
                            ythrow yexception() << "failed to read from " << path.Quote()
                                << ", result# " << static_cast<i64>(result);
                        }

                        promise->set_value(std::move(*data));
                    } catch (...) {
                        promise->set_exception(std::current_exception());
                    }
                });
        } catch (...) {
            promise->set_exception(std::current_exception());
        }

        return future;
    }

    std::future<void> WriteAsync(ui64 offset, size_t size, NKikimr::NPDisk::TAlignedData data) {
        auto promise = std::make_shared<std::promise<void>>();
        auto future = promise->get_future();

        if (size > data.Size()) {
            try {
                ythrow yexception() << "write size# " << size << " exceeds buffer size# " << data.Size();
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
            return future;
        }

        if (size == 0) {
            promise->set_value();
            return future;
        }

        try {
            IoPoller_.Pwrite(
                Path_,
                std::move(data),
                size,
                offset,
                [promise, path = Path_](NKikimr::NPDisk::EIoResult result) mutable {
                    try {
                        if (result != NKikimr::NPDisk::EIoResult::Ok) {
                            ythrow yexception() << "failed to write to " << path.Quote()
                                << ", result# " << static_cast<i64>(result);
                        }

                        promise->set_value();
                    } catch (...) {
                        promise->set_exception(std::current_exception());
                    }
                });
        } catch (...) {
            promise->set_exception(std::current_exception());
        }

        return future;
    }

private:
    TIOPoller& IoPoller_;
    TString Path_;
    ui64 NextOffset_ = 0;
};


class TRocksDbHandle {
public:
    explicit TRocksDbHandle(TString dbPath)
        : DbPath_(std::move(dbPath))
    {
        Options_.create_if_missing = true;
        WriteOptions_.sync = true;
    }

    void Open() {
        TFsPath(DbPath_).MkDirs();

        rocksdb::DB* db = nullptr;
        const rocksdb::Status status = rocksdb::DB::Open(
            Options_,
            std::string(DbPath_.data(), DbPath_.size()),
            &db);
        if (!status.ok()) {
            ythrow yexception() << "failed to open RocksDB at " << DbPath_.Quote() << ": " << status.ToString();
        }

        Db_.reset(db);
    }

    void Put(const TString& key, const TString& value) {
        const rocksdb::Status status = Db_->Put(
            WriteOptions_,
            rocksdb::Slice(key.data(), key.size()),
            rocksdb::Slice(value.data(), value.size()));
        if (!status.ok()) {
            ythrow yexception() << "failed to write RocksDB key " << key.Quote() << " at " << DbPath_.Quote()
                << ": " << status.ToString();
        }
    }

    TString Get(const TString& key) {
        std::string value;
        const rocksdb::Status status = Db_->Get(
            rocksdb::ReadOptions(),
            rocksdb::Slice(key.data(), key.size()),
            &value);
        if (status.IsNotFound()) {
            ythrow yexception() << "failed to read RocksDB key " << key.Quote() << " at " << DbPath_.Quote()
                << ": key not found";
        }
        if (!status.ok()) {
            ythrow yexception() << "failed to read RocksDB key " << key.Quote() << " at " << DbPath_.Quote()
                << ": " << status.ToString();
        }

        return TString(value.data(), value.size());
    }

private:
    TString DbPath_;
    rocksdb::Options Options_;
    rocksdb::WriteOptions WriteOptions_;
    std::unique_ptr<rocksdb::DB> Db_;
};

inline TString BuildDbPath(const TString& rootPath, TStringBuf storageKind) {
    const TString dbDirName = TStringBuilder() << "mds-rocksdb-" << storageKind;
    return (TFsPath(rootPath) / dbDirName).GetPath();
}

struct TBlobMetadata {
    TString Path;
    ui64 Offset = 0;
    ui64 Size = 0;

    NJson::TJsonValue ToJson() const {
        NJson::TJsonValue value(NJson::JSON_MAP);
        value.InsertValue("path", Path);
        value.InsertValue("offset", Offset);
        value.InsertValue("size", Size);
        return value;
    }

    void FromJson(const NJson::TJsonValue& value) {
        Path = value["path"].GetStringSafe();
        Offset = value["offset"].GetUIntegerSafe();
        Size = value["size"].GetUIntegerSafe();
    }

    void FromString(const TString& str) {
        NJson::TJsonValue val;
        NJson::ReadJsonTree(str, &val, true);
        FromJson(val);
    }
};

inline TString BuildBlobMetadataJson(const TString& path, ui64 offset, ui64 size) {
    TBlobMetadata metadata;
    metadata.Path = path;
    metadata.Offset = offset;
    metadata.Size = size;

    const NJson::TJsonValue json = metadata.ToJson();

    NJsonWriter::TBuf writer(NJsonWriter::HEM_RELAXED);
    writer.WriteJsonValue(&json);
    return writer.Str();
}

} // namespace NStorageDetail

class TStorageRegistry {
public:
    TStorageRegistry(TVector<TString> nvmePaths, TVector<TString> hddPaths) {
        if (!nvmePaths.empty()) {
            OpenGroup(nvmePaths.front(), "nvme");
        }

        if (!hddPaths.empty()) {
            Poller_ = std::make_unique<TIOPoller>(CreatePollerDevices(hddPaths));
        }

        for (const TString& hddPath : hddPaths) {
            HDDs_.emplace_back(*Poller_, hddPath);
        }
    }

    void Put(const TString& key, const TString& value) {
        if (!Db_) {
            ythrow yexception() << "storage is not initialized";
        }

        assert(!HDDs_.empty());

        const ui64 size = value.size();
        ui64 offset = 0;
        const TString* hddPath = nullptr;
        NStorageDetail::THDDWarehouse* warehouse = nullptr;

        std::future<void> writeFuture;
        {
            std::lock_guard guard(Mutex_);

            const size_t hddIndex = NextHddIndex_;
            NextHddIndex_ = (NextHddIndex_ + 1) % HDDs_.size();

            warehouse = &HDDs_[hddIndex];
            offset = warehouse->Reserve(size);
            hddPath = &warehouse->GetPath();

            writeFuture = warehouse->WriteAsync(
                offset,
                size,
                [&] {
                    NKikimr::NPDisk::TAlignedData data(size);
                    if (size) {
                        std::memcpy(data.Get(), value.data(), size);
                    }
                    return data;
                }());

        }
        writeFuture.get();
        TString metadata = NStorageDetail::BuildBlobMetadataJson(*hddPath, offset, size);
        Db_->Put(key, metadata);
    }

    TString Get(const TString& key) {
        if (!Db_) {
            ythrow yexception() << "storage is not initialized";
        }

        TString value = Db_->Get(key);
        if (HDDs_.empty()) {
            return {};
        }

        NStorageDetail::TBlobMetadata metadata;
        metadata.FromString(value);

        auto* warehouse = FindWarehouseByPath(metadata.Path);
        if (!warehouse) {
            ythrow yexception() << "failed to find configured HDD warehouse for path " << metadata.Path.Quote();
        }

        return warehouse->ReadAsync(metadata.Offset, metadata.Size).get();
    }

private:
    static TVector<TDevice> CreatePollerDevices(const TVector<TString>& hddPaths) {
        TVector<TDevice> devices;
        devices.reserve(hddPaths.size());

        ui32 pDiskId = 1;
        for (const TString& hddPath : hddPaths) {
            devices.emplace_back(
                hddPath,
                TString(),
                NKikimr::NPDisk::CreateAsyncIoContextReal(
                    hddPath,
                    pDiskId++,
                    NKikimr::NPDisk::TDeviceMode::None));
        }

        return devices;
    }

    void OpenGroup(const TString& nvme, TStringBuf storageKind) {
        TString dbPath = NStorageDetail::BuildDbPath(nvme, storageKind);
        Db_ = std::make_unique<NStorageDetail::TRocksDbHandle>(dbPath);
        Db_->Open();
    }

    NStorageDetail::THDDWarehouse* FindWarehouseByPath(const TString& path) {
        for (auto& warehouse : HDDs_) {
            if (warehouse.GetPath() == path) {
                return &warehouse;
            }
        }

        return nullptr;
    }

private:
    std::unique_ptr<NStorageDetail::TRocksDbHandle> Db_;
    std::mutex Mutex_;
    size_t NextHddIndex_ = 0;
    std::unique_ptr<TIOPoller> Poller_;
    std::deque<NStorageDetail::THDDWarehouse> HDDs_;
};

} // namespace NMdsApp
