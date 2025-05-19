#pragma once
#include "duckdb/common/file_system.hpp"

#include <iostream>
#include <google/cloud/storage/client.h>

namespace duckdb {
class GCSFileHandle : public FileHandle {
public:
	GCSFileHandle(FileSystem &file_system, const string &path, const FileOpenFlags &flags, const string &bucket,
	              const string &file_path, uint64_t size)
	    : FileHandle(file_system, path, flags), file_offset(0), _bucket(bucket), _file_path(file_path), _size(size) {
	}

	void Close() override {
		if (_write_stream) {
			_write_stream->Close();
		}
	}

	google::cloud::storage::ObjectMetadata metadata() const {
		return _metadata;
	}

	string bucket() const {
		return _bucket;
	}
	string file_path() const {
		return _file_path;
	}

	uint64_t size() const {
		return _size;
	}

	time_t last_modified() const {
		return std::chrono::system_clock::to_time_t(_metadata.updated());
	}

	idx_t file_offset;

	bool IsReadyToWrite() const {
		if (_write_stream) {
			return true;
		} else {
			return false;
		}
	}

	void InitWriteStream(google::cloud::storage::Client &gcs_client) {
		_write_stream = gcs_client.WriteObject(bucket(), file_path(), google::cloud::storage::AutoFinalizeDisabled());
	}

	void WriteInto(char *buffer, int64_t nr_bytes) {
		_write_stream->write(buffer, nr_bytes);
		file_offset += nr_bytes;
	}

private:
	google::cloud::storage::ObjectMetadata _metadata;
	string _bucket;
	string _file_path;
	uint64_t _size;
	std::optional<google::cloud::storage::ObjectWriteStream> _write_stream = std::nullopt;
};

class GCSFileSystem : public FileSystem {
public:
	static const string PREFIX;

	explicit GCSFileSystem(google::cloud::storage::Client client) : gcs_client(client) {
	}

	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t GetFileSize(FileHandle &handle) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;
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

	static void GCSUrlParse(string path, std::string &bucket_name, std::string &file_path);

private:
	google::cloud::storage::Client gcs_client;
};
} // namespace duckdb