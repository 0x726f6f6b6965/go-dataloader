load("@gazelle//:def.bzl", "gazelle")
load("@rules_go//go:def.bzl", "go_library", "go_test")

gazelle(name = "gazelle")

go_library(
    name = "go-dataloader",
    srcs = [
        "cache.go",
        "dataloader.go",
        "def.go",
        "option.go",
        "trace.go",
    ],
    importpath = "github.com/0x726f6f6b6965/go-dataloader",
    visibility = ["//visibility:public"],
)

go_test(
    name = "go-dataloader_test",
    srcs = ["dataloader_test.go"],
    deps = [
        ":go-dataloader",
        "//mockopt",
        "@com_github_stretchr_testify//suite",
    ],
)
