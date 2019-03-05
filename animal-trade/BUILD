exports_files(glob(["data/**"]), visibility = ["//visibility:public"])

java_library(
    name = "trade-migration",
    srcs = glob(["src/*.java"]),
    deps = [
        "@graknlabs_client_java//:client-java",
        "@graknlabs_grakn_core//concept:concept",
        "@graknlabs_graql//java:graql",
        "@apache_commons_csv//jar"
    ],

    data = [
        "//data:trade-data"
    ]
)

java_binary(
    name = "trade-migration-binary",
    runtime_deps = [ ":trade-migration" ],
    classpath_resources = [":logback"],
    main_class = "grakn.examples.animaltrade.App",
    visibility = ["//visibility:public"]
)

filegroup(
    name = "logback",
    srcs = ["src/logback.xml"],
    visibility = ["//visibility:public"]
)