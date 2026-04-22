#include "raster_types.hpp"
#include "raster_casts.hpp"
#include "data_cube.hpp"

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
	// RT_DATACUBE -> LIST
	//------------------------------------------------------------------------------------------------------------------

	static bool DataCube2List(Vector &source, Vector &result, idx_t count, CastParameters &) {
		DataCube arg_cube(Allocator::DefaultAllocator());
		DataCube raw_cube(Allocator::DefaultAllocator());

		const auto &element_type = ListType::GetChildType(result.GetType());

		for (idx_t i = 0; i < count; i++) {
			Value blob = source.GetValue(i);
			arg_cube.LoadBlob(blob);

			if (arg_cube.GetCubeSize() > 0) {
				const DataHeader header = arg_cube.GetHeader();

				if (header.data_format != DataFormat::RAW) {
					arg_cube.ChangeFormat(DataFormat::RAW, raw_cube);
					Value temp_v = raw_cube.ToArray(element_type, false);
					Value temp_l = StructValue::GetChildren(temp_v)[5];
					result.SetValue(i, std::move(temp_l));
				} else {
					Value temp_v = arg_cube.ToArray(element_type, false);
					Value temp_l = StructValue::GetChildren(temp_v)[5];
					result.SetValue(i, std::move(temp_l));
				}
			} else {
				result.SetValue(i, Value::LIST(element_type, vector<Value>()));
			}
		}

		// If the source was a CONSTANT_VECTOR (all-literal input),
		// the result must also be CONSTANT_VECTOR.
		if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		return true;
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

		auto element_types = {LogicalType::UTINYINT, LogicalType::TINYINT,  LogicalType::USMALLINT,
		                      LogicalType::SMALLINT, LogicalType::UINTEGER, LogicalType::INTEGER,
		                      LogicalType::UBIGINT,  LogicalType::BIGINT,   LogicalType::FLOAT,
		                      LogicalType::DOUBLE};

		// RT_ARRAY -> STRUCT
		for (const auto &element_type : element_types) {
			loader.RegisterCastFunction(RasterTypes::ARRAY(element_type), LogicalType(LogicalTypeId::STRUCT),
			                            Array2Struct, 0);
		}

		// RT_DATACUBE -> LIST
		for (const auto &element_type : element_types) {
			loader.RegisterCastFunction(RasterTypes::DATACUBE(), LogicalType::LIST(element_type), DataCube2List, 0);
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
