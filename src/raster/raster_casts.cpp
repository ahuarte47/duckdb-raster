#include "raster_types.hpp"
#include "raster_casts.hpp"

// DuckDB
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// RASTER Casts
//======================================================================================================================

struct RasterCasts {
	//------------------------------------------------------------------------------------------------------------------
	// RT_DATACUBE <-> BLOB
	//------------------------------------------------------------------------------------------------------------------

	static bool DataCube2Blob(Vector &source, Vector &result, idx_t count, CastParameters &) {
		// RT_DATACUBE is a BLOB with an alias; the underlying representation is identical.
		result.Reinterpret(source);
		return true;
	}

	static bool Blob2DataCube(Vector &source, Vector &result, idx_t count, CastParameters &) {
		// RT_DATACUBE is a BLOB with an alias; the underlying representation is identical.
		result.Reinterpret(source);
		return true;
	}

	//------------------------------------------------------------------------------------------------------------------
	// RT_BBOX -> STRUCT
	//------------------------------------------------------------------------------------------------------------------

	static bool BBox2Struct(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		return DefaultCasts::NopCast(source, result, count, parameters);
	}

	//------------------------------------------------------------------------------------------------------------------
	// RT_ARRAY -> STRUCT
	//------------------------------------------------------------------------------------------------------------------

	static bool Array2Struct(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		return DefaultCasts::NopCast(source, result, count, parameters);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		// RT_DATACUBE <-> BLOB
		loader.RegisterCastFunction(RasterTypes::DATACUBE(), LogicalType::BLOB, DataCube2Blob, 0);
		loader.RegisterCastFunction(LogicalType::BLOB, RasterTypes::DATACUBE(), Blob2DataCube, 0);

		// RT_BBOX -> STRUCT
		loader.RegisterCastFunction(RasterTypes::BBOX(), LogicalType(LogicalTypeId::STRUCT), BBox2Struct, 0);

		// RT_ARRAY -> STRUCT
		auto element_types = {LogicalType::UTINYINT, LogicalType::TINYINT,  LogicalType::USMALLINT,
		                      LogicalType::SMALLINT, LogicalType::UINTEGER, LogicalType::INTEGER,
		                      LogicalType::UBIGINT,  LogicalType::BIGINT,   LogicalType::FLOAT,
		                      LogicalType::DOUBLE};

		for (const auto &element_type : element_types) {
			loader.RegisterCastFunction(RasterTypes::ARRAY(element_type), LogicalType(LogicalTypeId::STRUCT),
			                            Array2Struct, 0);
		}
	}
};

} // namespace

// ######################################################################################################################
//  Register
// ######################################################################################################################

void RasterCastsFunctions::Register(ExtensionLoader &loader) {
	RasterCasts::Register(loader);
}

} // namespace duckdb
