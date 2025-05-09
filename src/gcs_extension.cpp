#define DUCKDB_EXTENSION_MAIN

#include "gcs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void GcsScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Gcs "+name.GetString()+" 🐥");
        });
}

inline void GcsOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Gcs " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto gcs_scalar_function = ScalarFunction("gcs", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GcsScalarFun);
    ExtensionUtil::RegisterFunction(instance, gcs_scalar_function);

    // Register another scalar function
    auto gcs_openssl_version_scalar_function = ScalarFunction("gcs_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, GcsOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, gcs_openssl_version_scalar_function);
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
