#include "gcsfs.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<FileHandle> GCSFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                               optional_ptr<FileOpener> opener) {

	const std::string prefix = "gsfs://";
	string bucket_name, file_path;
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

	auto metadata = gcs_client.GetObjectMetadata(bucket_name, file_path).value();
	auto gsfh = make_uniq<GCSFileHandle>(*this, path, flags, metadata);
	return gsfh;
}
} // namespace duckdb
