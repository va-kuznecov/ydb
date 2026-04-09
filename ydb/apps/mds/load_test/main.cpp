#include <ydb/apps/mds/proto/mds.grpc.pb.h>
#include <ydb/core/util/lz4_data_generator.h>

#include <library/cpp/getopt/last_getopt.h>

#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/status.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/random/random.h>
#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/types.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace NMdsLoadTest {

namespace {

using namespace std::chrono_literals;

volatile std::sig_atomic_t StopRequestedBySignal = 0;
static constexpr size_t MaxGeneratedValueSize = 64 * 1024 * 1024;

void HandleStopSignal(int) {
    StopRequestedBySignal = 1;
}

struct TOptions {
    size_t MinSize = 0;
    size_t MaxSize = 0;
    ui32 ReadInFlight = 0;
    ui32 WriteInFlight = 0;
    TString ServerUrl;

    static TOptions Parse(int argc, char** argv) {
        TOptions options;

        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.SetTitle("MDS load generator");

        opts.AddLongOption("min_size", "Minimal size of written values in bytes.")
            .RequiredArgument("BYTES")
            .StoreResult(&options.MinSize)
            .Required();

        opts.AddLongOption("max_size", "Maximal size of written values in bytes.")
            .RequiredArgument("BYTES")
            .StoreResult(&options.MaxSize)
            .Required();

        opts.AddLongOption("read_in_flight", "Number of concurrent read requests.")
            .RequiredArgument("COUNT")
            .StoreResult(&options.ReadInFlight);
            //.Required();

        opts.AddLongOption("write_in_flight", "Number of concurrent write requests.")
            .RequiredArgument("COUNT")
            .StoreResult(&options.WriteInFlight)
            .Required();

        opts.AddLongOption("server_url", "MDS server URL: host:port, grpc://host:port or grpcs://host:port.")
            .RequiredArgument("URL")
            .StoreResult(&options.ServerUrl)
            .Required();

        NLastGetopt::TOptsParseResult(&opts, argc, argv);

        if (options.MinSize > options.MaxSize) {
            ythrow yexception() << "--min_size must be less than or equal to --max_size";
        }

        if (options.MinSize > MaxGeneratedValueSize || options.MaxSize > MaxGeneratedValueSize) {
            ythrow yexception() << "--min_size and --max_size must be less than or equal to 8 MiB";
        }

        if (!options.ReadInFlight && !options.WriteInFlight) {
            ythrow yexception() << "at least one of --read_in_flight or --write_in_flight must be positive";
        }

        if (options.ReadInFlight && !options.WriteInFlight) {
            ythrow yexception() << "--write_in_flight must be positive when --read_in_flight is used";
        }

        if (options.ServerUrl.empty()) {
            ythrow yexception() << "--server_url must not be empty";
        }

        return options;
    }
};

struct TEndpointConfig {
    TString Target;
    std::shared_ptr<grpc::ChannelCredentials> Credentials;
};

TEndpointConfig ParseServerUrl(const TString& serverUrl) {
    TStringBuf url(serverUrl);
    bool secure = false;

    if (url.SkipPrefix("grpc://")) {
        secure = false;
    } else if (url.SkipPrefix("grpcs://")) {
        secure = true;
    } else if (url.Contains("://")) {
        ythrow yexception() << "unsupported server URL scheme in " << serverUrl.Quote()
            << "; expected grpc://, grpcs:// or host:port";
    }

    const size_t slashPos = url.find('/');
    if (slashPos != TStringBuf::npos) {
        if (slashPos + 1 != url.size()) {
            ythrow yexception() << "server URL path is not supported: " << serverUrl.Quote();
        }
        url = url.substr(0, slashPos);
    }

    if (url.empty()) {
        ythrow yexception() << "server URL target is empty";
    }

    return {
        TString(url),
        secure ? grpc::SslCredentials(grpc::SslCredentialsOptions()) : grpc::InsecureChannelCredentials(),
    };
}

TStringBuf GenerateValue(size_t size) {
    static const TString value = NKikimr::FastGenDataForLZ4(MaxGeneratedValueSize);

    return TStringBuf(value.data(), size);
}

class TWrittenKeys {
public:
    void Add(TString key) {
        std::lock_guard lock(Mutex_);
        const size_t index = KeyOrder_.size();
        const bool inserted = KeyToIndex_.emplace(key, index).second;
        if (!inserted) {
            return;
        }

        KeyOrder_.push_back(std::move(key));
        Cv_.notify_all();
    }

    bool WaitAndPick(TString* key, const std::atomic<bool>& stopRequested) {
        std::unique_lock lock(Mutex_);
        Cv_.wait(lock, [this, &stopRequested] {
            return stopRequested.load(std::memory_order_relaxed) || !KeyOrder_.empty();
        });

        if (stopRequested.load(std::memory_order_relaxed) || KeyOrder_.empty()) {
            return false;
        }

        *key = KeyOrder_[RandomNumber<size_t>(KeyOrder_.size())];
        return true;
    }

    void NotifyStop() {
        Cv_.notify_all();
    }

    size_t Size() const {
        std::lock_guard lock(Mutex_);
        return KeyOrder_.size();
    }

private:
    mutable std::mutex Mutex_;
    std::condition_variable Cv_;
    THashMap<TString, size_t> KeyToIndex_;
    TVector<TString> KeyOrder_;
};

class TStats {
public:
    void OnWriteStarted() {
        WriteStarted_.fetch_add(1, std::memory_order_relaxed);
    }

    void OnWriteOk(size_t bytes) {
        WriteOk_.fetch_add(1, std::memory_order_relaxed);
        BytesWritten_.fetch_add(bytes, std::memory_order_relaxed);
    }

    void OnWriteError() {
        WriteErrors_.fetch_add(1, std::memory_order_relaxed);
    }

    void OnReadStarted() {
        ReadStarted_.fetch_add(1, std::memory_order_relaxed);
    }

    void OnReadOk(size_t bytes) {
        ReadOk_.fetch_add(1, std::memory_order_relaxed);
        BytesRead_.fetch_add(bytes, std::memory_order_relaxed);
    }

    void OnReadNotFound() {
        ReadNotFound_.fetch_add(1, std::memory_order_relaxed);
    }

    void OnReadError() {
        ReadErrors_.fetch_add(1, std::memory_order_relaxed);
    }

    struct TSnapshot {
        ui64 WriteStarted = 0;
        ui64 WriteOk = 0;
        ui64 WriteErrors = 0;
        ui64 ReadStarted = 0;
        ui64 ReadOk = 0;
        ui64 ReadNotFound = 0;
        ui64 ReadErrors = 0;
        ui64 BytesWritten = 0;
        ui64 BytesRead = 0;
    };

    TSnapshot Snapshot() const {
        TSnapshot snapshot;
        snapshot.WriteStarted = WriteStarted_.load(std::memory_order_relaxed);
        snapshot.WriteOk = WriteOk_.load(std::memory_order_relaxed);
        snapshot.WriteErrors = WriteErrors_.load(std::memory_order_relaxed);
        snapshot.ReadStarted = ReadStarted_.load(std::memory_order_relaxed);
        snapshot.ReadOk = ReadOk_.load(std::memory_order_relaxed);
        snapshot.ReadNotFound = ReadNotFound_.load(std::memory_order_relaxed);
        snapshot.ReadErrors = ReadErrors_.load(std::memory_order_relaxed);
        snapshot.BytesWritten = BytesWritten_.load(std::memory_order_relaxed);
        snapshot.BytesRead = BytesRead_.load(std::memory_order_relaxed);
        return snapshot;
    }

    void PrintProgress(const TSnapshot& previous, const TSnapshot& current, size_t knownKeys) const {
        Cout
            << "writes: ok=" << current.WriteOk
            << " rate=" << (current.WriteOk - previous.WriteOk) << "/s"
            << " errors=" << current.WriteErrors
            << " speed=" << HumanReadableSize(current.BytesWritten - previous.BytesWritten, SF_QUANTITY) << "/s"
            << " | reads: ok=" << current.ReadOk
            << " rate=" << (current.ReadOk - previous.ReadOk) << "/s"
            << " not_found=" << current.ReadNotFound
            << " errors=" << current.ReadErrors
            << " speed=" << HumanReadableSize(current.BytesRead - previous.BytesRead, SF_QUANTITY) << "/s"
            << " | known_keys=" << knownKeys
            << Endl;
    }

private:
    std::atomic<ui64> WriteStarted_ = 0;
    std::atomic<ui64> WriteOk_ = 0;
    std::atomic<ui64> WriteErrors_ = 0;
    std::atomic<ui64> ReadStarted_ = 0;
    std::atomic<ui64> ReadOk_ = 0;
    std::atomic<ui64> ReadNotFound_ = 0;
    std::atomic<ui64> ReadErrors_ = 0;
    std::atomic<ui64> BytesWritten_ = 0;
    std::atomic<ui64> BytesRead_ = 0;
};

class TLoadGenerator {
public:
    explicit TLoadGenerator(TOptions options)
        : Options_(std::move(options))
        , Endpoint_(ParseServerUrl(Options_.ServerUrl))
        , Channel_(grpc::CreateChannel(Endpoint_.Target, Endpoint_.Credentials))
    {
    }

    void Run() {
        if (!Channel_->WaitForConnected(std::chrono::system_clock::now() + 5s)) {
            ythrow yexception() << "failed to connect to " << Options_.ServerUrl.Quote() << " within 5 seconds";
        }

        Cout << "Connected to " << Endpoint_.Target << ", press Ctrl+C to stop" << Endl;

        StartWorkers();

        while (!StopRequestedBySignal) {
            std::this_thread::sleep_for(100ms);
        }

        Stop();
        Cout << "--------------------------------------" << Endl;
        Stats_.PrintProgress(TStats::TSnapshot{}, Stats_.Snapshot(), WrittenKeys_.Size());
    }

private:
    void StartWorkers() {
        Threads_.reserve(static_cast<size_t>(Options_.ReadInFlight) + Options_.WriteInFlight + 1);

        for (ui32 i = 0; i < Options_.WriteInFlight; ++i) {
            Threads_.emplace_back([this] {
                WriterLoop();
            });
        }

        for (ui32 i = 0; i < Options_.ReadInFlight; ++i) {
            Threads_.emplace_back([this] {
                ReaderLoop();
            });
        }

        Threads_.emplace_back([this] {
            ProgressLoop();
        });
    }

    void Stop() {
        bool expected = false;
        if (!StopRequested_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
            return;
        }

        WrittenKeys_.NotifyStop();

        for (auto& thread : Threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    void ProgressLoop() {
        TStats::TSnapshot previous = Stats_.Snapshot();

        while (!StopRequested_.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(1s);

            const TStats::TSnapshot current = Stats_.Snapshot();
            Stats_.PrintProgress(previous, current, WrittenKeys_.Size());
            previous = current;
        }
    }

    void WriterLoop() {
        auto stub = NMds::MdsService::NewStub(Channel_);

        while (!StopRequested_.load(std::memory_order_relaxed)) {
            const ui64 id = NextKeyId_.fetch_add(1, std::memory_order_relaxed);
            const TString key = TStringBuilder() << "mds-load-test-" << id;
            const size_t valueSize = PickValueSize();
            const TStringBuf value = GenerateValue(valueSize);

            Stats_.OnWriteStarted();

            grpc::ClientContext context;
            NMds::PutRequest request;
            NMds::PutResponse response;

            request.set_key(key.data(), key.size());
            request.set_value(value.data(), value.size());

            const grpc::Status status = stub->Put(&context, request, &response);
            if (status.ok() && response.status() == "OK") {
                WrittenKeys_.Add(key);
                Stats_.OnWriteOk(valueSize);
            } else {
                Stats_.OnWriteError();
                MaybeLogFailure("Put", key, status, response.status());
            }
        }
    }

    void ReaderLoop() {
        auto stub = NMds::MdsService::NewStub(Channel_);

        while (!StopRequested_.load(std::memory_order_relaxed)) {
            TString key;
            if (!WrittenKeys_.WaitAndPick(&key, StopRequested_)) {
                return;
            }

            Stats_.OnReadStarted();

            grpc::ClientContext context;
            NMds::GetRequest request;
            NMds::GetResponse response;
            request.set_key(key.data(), key.size());

            const grpc::Status status = stub->Get(&context, request, &response);
            if (status.ok()) {
                Stats_.OnReadOk(response.value().size());
            } else if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
                Stats_.OnReadNotFound();
                MaybeLogFailure("Get", key, status, TString());
            } else {
                Stats_.OnReadError();
                MaybeLogFailure("Get", key, status, TString());
            }
        }
    }

    void MaybeLogFailure(TStringBuf operation, TStringBuf key, const grpc::Status& status, TStringBuf responseStatus) {
        const ui64 logged = LoggedFailures_.fetch_add(1, std::memory_order_relaxed);
        if (logged >= 10) {
            if (logged == 10) {
                Cerr << "Too many RPC failures, suppressing further logs" << Endl;
            }
            return;
        }

        Cerr
            << operation << " failed for key " << TString(key).Quote()
            << ": grpc_status=" << static_cast<int>(status.error_code())
            << " grpc_message=" << status.error_message();

        if (!responseStatus.empty()) {
            Cerr << " response_status=" << TString(responseStatus).Quote();
        }

        Cerr << Endl;
    }

    size_t PickValueSize() const {
        if (Options_.MinSize == Options_.MaxSize) {
            return Options_.MinSize;
        }

        return Options_.MinSize + RandomNumber<size_t>(Options_.MaxSize - Options_.MinSize + 1);
    }

private:
    const TOptions Options_;
    const TEndpointConfig Endpoint_;
    std::shared_ptr<grpc::Channel> Channel_;
    TWrittenKeys WrittenKeys_;
    TStats Stats_;
    std::atomic<bool> StopRequested_ = false;
    std::atomic<ui64> NextKeyId_ = 0;
    std::atomic<ui64> LoggedFailures_ = 0;
    TVector<std::thread> Threads_;
};

int Run(int argc, char** argv) {
    std::signal(SIGINT, HandleStopSignal);
    std::signal(SIGTERM, HandleStopSignal);

    TLoadGenerator generator(TOptions::Parse(argc, argv));
    generator.Run();
    return 0;
}

} // namespace

} // namespace NMdsLoadTest

int main(int argc, char** argv) {
    try {
        return NMdsLoadTest::Run(argc, argv);
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
        return 1;
    }
}
