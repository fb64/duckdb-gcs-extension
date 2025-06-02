#pragma once
#include "duckdb/common/file_system.hpp"
#include <google/cloud/storage/client.h>

namespace duckdb {
class GCSFileHandle : public FileHandle {
public:
	GCSFileHandle(FileSystem &file_system, const string &path, const FileOpenFlags &flags, const string &bucket,
	              const string &file_path, uint64_t size, time_t last_modified)
	    : FileHandle(file_system, path, flags), file_offset(0), _bucket(bucket), _file_path(file_path), _size(size),
	      _last_modified(last_modified) {
	}

	void Close() override {
		if (_write_stream != nullptr) {
			_write_stream->Close();
			_write_stream = nullptr;
		}
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
		return _last_modified;
		// return std::chrono::system_clock::to_time_t(_metadata.updated());
	}

	idx_t file_offset;

	bool IsReadyToWrite() const {
		if (_write_stream) {
			return true;
		} else {
			return false;
		}
	}

	void InitWriteStream(google::cloud::storage::ObjectWriteStream& stream);
	void WriteInto(char *buffer, int64_t nr_bytes);

private:
	string _bucket;
	string _file_path;
	uint64_t _size;
	time_t _last_modified;
	unique_ptr<google::cloud::storage::ObjectWriteStream> _write_stream = nullptr;
};

class GCSFileSystem : public FileSystem {
public:
	static const string PREFIX;

	explicit GCSFileSystem() {
		gcs_client = make_uniq<google::cloud::storage::Client>(std::move(google::cloud::storage::Client()));
	}

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

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
	unique_ptr<google::cloud::storage::Client> gcs_client;
};
} // namespace duckdb