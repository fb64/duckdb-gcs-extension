#pragma once
#include "duckdb/common/file_system.hpp"
#include <google/cloud/storage/client.h>

namespace duckdb {
class GCSFileHandle : public FileHandle {
public:
	GCSFileHandle(FileSystem &file_system, const string &path, const FileOpenFlags &flags,
	              google::cloud::storage::ObjectMetadata metadata)
	    : FileHandle(file_system, path, flags), file_offset(0), _metadata(metadata) {
	}

	void Close() override {
	}

	google::cloud::storage::ObjectMetadata metadata() const {
		return _metadata;
	}

	string bucket() const {
		return _metadata.bucket();
	}
	string file_path() const {
		return _metadata.name();
	}

	uint64_t size() const {
		return _metadata.size();
	}

	time_t last_modified() const {
		return std::chrono::system_clock::to_time_t(_metadata.updated());
	}

	idx_t file_offset;

private:
	google::cloud::storage::ObjectMetadata _metadata;
};

class GCSFileSystem : public FileSystem {
public:
	explicit GCSFileSystem(google::cloud::storage::Client client) : gcs_client(client) {
	}

	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t GetFileSize(FileHandle &handle) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	static void GCSUrlParse(string path, std::string &bucket_name, std::string &file_path);

	bool CanHandleFile(const string &fpath) override;

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

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

private:
	google::cloud::storage::Client gcs_client;
};
inline bool GCSFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("gsfs://", 0) == 0;
}
} // namespace duckdb