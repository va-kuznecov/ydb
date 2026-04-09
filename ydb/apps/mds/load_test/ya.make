PROGRAM(mds-load-test)

SRCS(
    main.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/getopt
    ydb/apps/mds/proto
)

END()
