#define DUCKDB_EXTENSION_MAIN

#include "gcs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "gcsfs.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &fs = instance.GetFileSystem();
	fs.RegisterSubSystem(make_uniq<GCSFileSystem>());
}

void GcsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
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

DUCKDB_CPP_EXTENSION_ENTRY(gcs, loader) {
	duckdb::LoadInternal(loader);
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
