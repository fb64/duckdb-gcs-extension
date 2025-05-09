#include "gcsfs.hpp"
#include "duckdb/common/string_util.hpp"

#include <iostream>

namespace duckdb {

unique_ptr<FileHandle> GCSFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                               optional_ptr<FileOpener> opener) {
	string bucket_name, file_path;
	GCSFileSystem::GCSUrlParse(path, bucket_name, file_path);

	auto metadata = gcs_client.GetObjectMetadata(bucket_name, file_path).value();
	auto gsfh = make_uniq<GCSFileHandle>(*this, path, flags, metadata);
	return gsfh;
}

void GCSFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	if (nr_bytes > 0) {
		auto &gsfh = handle.Cast<GCSFileHandle>();
		auto read_range = google::cloud::storage::ReadRange(location, location + nr_bytes);
		auto reader = gcs_client.ReadObject(gsfh.bucket(), gsfh.file_path(), read_range);
		auto read_buffer = char_ptr_cast(buffer);
		if (!reader) {
			std::ostringstream reader_status;
			reader_status << reader.status();
			throw IOException("Could not read from file: %s", {{"errno", std::to_string(errno)}}, reader_status.str(),
			                  strerror(errno));
		}
		reader.read(read_buffer, nr_bytes);
	}
}

int64_t GCSFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &gsfh = handle.Cast<GCSFileHandle>();
	idx_t max_read = gsfh.size() - gsfh.file_offset;
	nr_bytes = MinValue<idx_t>(max_read, nr_bytes);
	Read(handle, buffer, nr_bytes, gsfh.file_offset);
	gsfh.file_offset += nr_bytes;
	return nr_bytes;
}

int64_t GCSFileSystem::GetFileSize(FileHandle &handle) {
	const auto &gsfh = handle.Cast<GCSFileHandle>();
	return gsfh.size();
}
time_t GCSFileSystem::GetLastModifiedTime(FileHandle &handle) {
	const auto &gsfh = handle.Cast<GCSFileHandle>();
	return gsfh.last_modified();
}

void GCSFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &gsfh = handle.Cast<GCSFileHandle>();
	gsfh.file_offset = location;
}

vector<string> GCSFileSystem::Glob(const string &path, FileOpener *opener) {
	string bucket, pattern_path;
	GCSFileSystem::GCSUrlParse(path, bucket, pattern_path);
	vector<string> files_list;
	auto list_results = gcs_client.ListObjects(bucket, google::cloud::storage::MatchGlob(pattern_path));
	for (auto &&result : list_results) {
		string file_path = "gsfs://" + result->bucket() + "/" + result->name();
		files_list.push_back(file_path);
	}
	return files_list;
}

void GCSFileSystem::GCSUrlParse(string path, std::string &bucket_name, std::string &file_path) {
	const std::string prefix = "gsfs://";
	if (path.substr(0, prefix.size()) != prefix) {
		throw IOException("URL needs to start with  gsfs://");
	}

	std::string remaining_url = path.substr(prefix.size());

	size_t pos = remaining_url.find('/');
	if (pos == std::string::npos || pos == remaining_url.size() - 1) {
		throw IOException("File path is empty");
	}

	bucket_name = remaining_url.substr(0, pos);
	file_path = remaining_url.substr(pos + 1);
}

} // namespace duckdb
