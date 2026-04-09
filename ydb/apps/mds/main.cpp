#include <ydb/apps/mds/proto/mds.grpc.pb.h>
#include "storage.h"

#include <library/cpp/getopt/last_getopt.h>

#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/thread.h>

#include <memory>

namespace NMdsApp {

namespace {

struct TOptions {
    TVector<TString> NvmePaths;
    TVector<TString> HddPaths;
    ui16 Port = 0;

    static TOptions Parse(int argc, char** argv) {
        TOptions options;

        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.SetTitle("Basic MDS app");

        opts.AddLongOption("nvme", "NVMe storage root path. Repeat the option to pass multiple paths.")
            .RequiredArgument("PATH")
            .AppendTo(&options.NvmePaths);

        opts.AddLongOption("hdd", "HDD storage root path. Repeat the option to pass multiple paths.")
            .RequiredArgument("PATH")
            .AppendTo(&options.HddPaths);

        opts.AddLongOption("port", "Port for the gRPC endpoint.")
            .RequiredArgument("PORT")
            .StoreResult(&options.Port)
            .Required();

        NLastGetopt::TOptsParseResult(&opts, argc, argv);

        if (!options.Port) {
            ythrow yexception() << "--port must be in range 1..65535";
        }

        if (options.NvmePaths.empty() && options.HddPaths.empty()) {
            ythrow yexception() << "at least one --nvme or --hdd path must be provided";
        }

        return options;
    }
};

class TMdsService final : public NMds::MdsService::Service {
public:
    explicit TMdsService(TStorageRegistry& storage)
        : Storage_(storage)
    {
    }

    grpc::Status Ping(grpc::ServerContext* /*context*/, const NMds::PingRequest* request,
            NMds::PingResponse* response) override {
        try {
            const auto requestMessage = request->message();
            const auto responseMessage = TString(TStringBuilder() << "pong: " << requestMessage);

            response->set_message(responseMessage);
            response->set_ping_count(PingCount_++);

            return grpc::Status::OK;
        } catch (const std::exception& ex) {
            return grpc::Status(grpc::StatusCode::INTERNAL, ex.what());
        }
    }

    grpc::Status Put(grpc::ServerContext* /*context*/, const NMds::PutRequest* request,
            NMds::PutResponse* response) override {
        try {
            Storage_.Put(request->key(), request->value());
            response->set_status("OK");

            return grpc::Status::OK;
        } catch (const std::exception& ex) {
            return grpc::Status(grpc::StatusCode::INTERNAL, ex.what());
        }
    }

    grpc::Status Get(grpc::ServerContext* /*context*/, const NMds::GetRequest* request,
            NMds::GetResponse* response) override {
        try {
            if (TString value = Storage_.Get(request->key())) {
                response->set_value(value);
                return grpc::Status::OK;
            } else {
                return grpc::Status(grpc::StatusCode::NOT_FOUND, "the key was not found");
            }
        } catch (const std::exception& ex) {
            return grpc::Status(grpc::StatusCode::INTERNAL, ex.what());
        }
    }

private:
    TStorageRegistry& Storage_;

    std::atomic<size_t> PingCount_{0};
};

int Run(int argc, char** argv) {
    const TOptions options = TOptions::Parse(argc, argv);
    TStorageRegistry storage(options.NvmePaths, options.HddPaths);
    TMdsService service(storage);

    const TString endpoint = TStringBuilder() << "0.0.0.0:" << options.Port;

    grpc::ServerBuilder builder;
    builder.AddListeningPort(endpoint, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        ythrow yexception() << "failed to start gRPC server on " << endpoint;
    }

    Cout << "MDS gRPC server is listening on " << endpoint << Endl;
    server->Wait();
    return 0;
}

} // namespace

} // namespace NMdsApp

int main(int argc, char** argv) {
    try {
        return NMdsApp::Run(argc, argv);
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
        return 1;
    }
}
