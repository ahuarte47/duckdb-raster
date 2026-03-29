#include "raster_utils.hpp"

namespace duckdb {

//======================================================================================================================
// Raster utilities
//======================================================================================================================

std::string RasterUtils::GetLastGdalErrorMsg() {
	return std::string(CPLGetLastErrorMsg());
}

Point2D RasterUtils::RasterCoordToWorldCoord(const double matrix[6], const int32_t &col, const int32_t &row) {
	const double x = matrix[0] + matrix[1] * col + matrix[2] * row;
	const double y = matrix[3] + matrix[4] * col + matrix[5] * row;
	return Point2D(x, y);
}

int RasterUtils::GetSrid(const char *proj_def) {
	int srid = 0; // SRID_UNKNOWN

	if (proj_def) {
		OGRSpatialReference spatial_ref;
		spatial_ref.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);

		if (spatial_ref.importFromWkt(proj_def) == OGRERR_NONE && spatial_ref.AutoIdentifyEPSG() == OGRERR_NONE) {
			const char *code = spatial_ref.GetAuthorityCode(nullptr);
			if (code) {
				srid = atoi(code);
			}
		}
	}
	return srid;
}

Value RasterUtils::TileAsBlob(const TileHeader &header, MemoryStream &data_buffer, const idx_t &data_length) {
	switch (header.compression) {
	case CompressionAlg::NONE:
		// Nothing to do for uncompressed tiles, the buffer already contains the header and the raw tile data.
		return Value::BLOB(data_buffer.GetData(), data_length);
	default:
		throw std::runtime_error("Unsupported compression: " + CompressionAlg::ToString(header.compression));
	}
}

} // namespace duckdb
