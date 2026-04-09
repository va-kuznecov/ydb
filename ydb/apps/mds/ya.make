PROGRAM(mds-server)

SRCS(
    main.cpp
    storage.cpp
)

PEERDIR(
    contrib/libs/grpc
    contrib/libs/rocksdb
    library/cpp/getopt
    library/cpp/json
    library/cpp/json/fast_sax
    library/cpp/json/writer
    ydb/apps/mds/proto
    ydb/core/blobstorage/pdisk
    ydb/library/pdisk_io
    ydb/library/services
)

END()

RECURSE(
    load_test
)
