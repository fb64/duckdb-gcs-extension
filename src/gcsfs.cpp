#include "gcsfs.hpp"
#include "include/gcsfs.hpp"

#include "duckdb/common/string_util.hpp"

#include <execinfo.h>
#include <iostream>
#include <unistd.h>

namespace duckdb {

const string GCSFileSystem::PREFIX = std::string("gsfs://");

unique_ptr<FileHandle> GCSFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                               optional_ptr<FileOpener> opener) {
	unique_ptr<GCSFileHandle> gsfh;
	string bucket_name, file_path;
	GCSUrlParse(path, bucket_name, file_path);
	if (flags.OpenForWriting()) {
		gsfh = make_uniq<GCSFileHandle>(*this, path, flags, bucket_name, file_path, 0);
	} else if (flags.OpenForReading()) {
		auto metadata = gcs_client.GetObjectMetadata(bucket_name, file_path).value();
		gsfh = make_uniq<GCSFileHandle>(*this, path, flags, metadata.bucket(), metadata.name(), metadata.size());
	}
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

void GCSFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &gsfh = handle.Cast<GCSFileHandle>();
	auto write_buffer = char_ptr_cast(buffer);
	if (!gsfh.IsReadyToWrite()) {
		gsfh.InitWriteStream(gcs_client);
	}
	gsfh.WriteInto(write_buffer, nr_bytes);
}

int64_t GCSFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &gsfh = handle.Cast<GCSFileHandle>();
	Write(handle, buffer, nr_bytes, gsfh.file_offset);
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
bool GCSFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	string bucket, file_path;
	GCSUrlParse(filename, bucket, file_path);
	auto metadata = gcs_client.GetObjectMetadata(bucket, file_path);
	if (metadata) {
		return true;
	}
	return false;
}
void GCSFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	string bucket, file_path;
	GCSUrlParse(filename, bucket, file_path);
	auto status = gcs_client.DeleteObject(bucket, file_path);
	if (!status.ok()) {
		throw IOException("Unable to delete file: " + filename);
	}
}
void GCSFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	string bucket, directory_path;
	GCSUrlParse(directory, bucket, directory_path);
	auto status = gcs_client.DeleteObject(bucket, directory_path);
	if (!status.ok()) {
		throw IOException("Unable to delete directory: " + directory_path);
	}
}
void GCSFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	string src_bucket, src_file_path, dst_bucket, dst_file_path;
	GCSUrlParse(source, src_bucket, src_file_path);
	GCSUrlParse(target, dst_bucket, dst_file_path);
	if (src_bucket != dst_bucket) {
		throw IOException("Cannot move file on different buckets src: %s != dst: %s ",src_bucket, dst_bucket);
	}
	auto result = gcs_client.MoveObject(src_bucket, src_file_path, dst_file_path);
	if (!result) {
		throw IOException("Error while moving file with status:  ",std::move(result).status().message());
	}
}

vector<string> GCSFileSystem::Glob(const string &path, FileOpener *opener) {
	string bucket, pattern_path;
	GCSUrlParse(path, bucket, pattern_path);
	vector<string> files_list;
	auto list_results = gcs_client.ListObjects(bucket, google::cloud::storage::MatchGlob(pattern_path));
	for (auto &&result : list_results) {
		string file_path = PREFIX + result->bucket() + "/" + result->name();
		files_list.push_back(file_path);
	}
	return files_list;
}

void GCSFileSystem::GCSUrlParse(string path, std::string &bucket_name, std::string &file_path) {
	const std::string prefix = PREFIX;
	if (path.substr(0, prefix.size()) != prefix) {
		throw IOException("URL needs to start with %s", {{"errno", std::to_string(errno)}}, PREFIX);
	}

	std::string remaining_url = path.substr(prefix.size());

	size_t pos = remaining_url.find('/');
	if (pos == std::string::npos || pos == remaining_url.size() - 1) {
		throw IOException("File path is empty");
	}

	bucket_name = remaining_url.substr(0, pos);
	file_path = remaining_url.substr(pos + 1);
}

bool GCSFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind(PREFIX, 0) == 0;
}

} // namespace duckdb
