#define DUCKDB_EXTENSION_MAIN

#include "gcs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_util.hpp"
#include "gcsfs.hpp"
#include <google/cloud/storage/grpc_plugin.h>

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	auto &fs = instance.GetFileSystem();
	fs.RegisterSubSystem(make_uniq<GCSFileSystem>(google::cloud::storage::MakeGrpcClient()));
}

void GcsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string GcsExtension::Name() {
	return "gcs";
}

std::string GcsExtension::Version() const {
#ifdef EXT_VERSION_GCS
	return EXT_VERSION_GCS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void gcs_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::GcsExtension>();
}

DUCKDB_EXTENSION_API const char *gcs_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
