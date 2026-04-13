#pragma once

#include "raster_types.hpp"
#include <cstdint>

// DuckDB
#include "duckdb.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
// GDAL
#include "gdal_priv.h"

namespace duckdb {

//! Utility class for raster data handling.
class RasterUtils {
public:
	//! Get the last error message from GDAL as a string.
	static std::string GetLastGdalErrorMsg();

	//! Convert raster pixel coordinate to geographic 2D coordinate.
	static Point2D RasterCoordToWorldCoord(const double matrix[6], const int32_t &col, const int32_t &row);

	//! Get the SRID from a WKT projection definition, returns 0 if it cannot be determined.
	static int GetSrid(const char *proj_def);

	/**
	 * Get a BLOB value from the memory buffer managing a raster tile.
	 * The header contains the metadata of the tile (e.g. compression, data type, dimensions).
	 * The buffer contains the metadata and the raw tile data, so the BLOB content includes
	 * all the information needed to read and interpret the tile data.
	 */
	static Value TileAsBlob(const TileHeader &header, MemoryStream &data_buffer, const idx_t &data_length);

	//! Convert a BLOB value containing data bands of a raster tile into an ARRAY of values.
	static Value BlobAsArray(const Value &blob, const LogicalType &array_type, const bool &filter_nodata = false);

	//! Convert a BLOB value containing data bands of a raster tile into a stream of bytes.
	static bool BlobAsStream(const Value &blob, TileHeader &data_header, MemoryStream &data_buffer);
};

} // namespace duckdb
