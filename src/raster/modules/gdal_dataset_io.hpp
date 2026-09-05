#pragma once

#include <memory>
#include <vector>

// DuckDB
#include "duckdb.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/main/client_context.hpp"

// GDAL
#include "gdal_priv.h"

namespace duckdb {

class DuckDBDatasetFactory {
public:
	//! Resolve a file path into a GDAL-accessible path.
	static std::string GdalFilePath(ClientContext &context, const std::string &file_path);

	//! Open a GDAL dataset from a list of file names and named parameters.
	static GDALDataset *OpenDataset(ClientContext &context, const std::vector<std::string> &file_names,
	                                const named_parameter_map_t &params);

	//! Warp a GDAL dataset from a list of file names and named parameters.
	static GDALDataset *WarpDataset(ClientContext &context, const std::vector<std::string> &file_names,
	                                const named_parameter_map_t &params,
	                                std::vector<GDALDatasetUniquePtr> &child_datasets);

	//! Create a GDAL dataset given a file name and named parameters.
	static GDALDataset *CreateDataset(ClientContext &context, const std::string &file_name,
	                                  const named_parameter_map_t &params);
};

} // namespace duckdb
