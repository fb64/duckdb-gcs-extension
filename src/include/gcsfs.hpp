#pragma once
#include "duckdb/common/file_system.hpp"
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/grpc_plugin.h>

namespace duckdb{
class GCSFileHandle : public FileHandle {
public:
	GCSFileHandle(FileSystem &file_system, const string &path, const FileOpenFlags &flags,string bucket_name, string file_path,uint64_t file_length)
	    : FileHandle(file_system, path, flags) {
	}
};


class GCSFileSystem : public FileSystem{
public:
    explicit GCSFileSystem(google::cloud::storage::Client client):gcs_client(client){
    }

    bool CanSeek() override {
        return true;
    }
    bool OnDiskFile(FileHandle &handle) override {
        return false;
    }
    bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override {
        return false;
    }

    string GetName() const override {
        return "GCSFileSystem";
    }
    string PathSeparator(const string &path) override {
        return "/";
    }

private:
    google::cloud::storage::Client gcs_client;

};
}