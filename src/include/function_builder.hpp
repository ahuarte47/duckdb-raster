#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/catalog/catalog_entry/function_entry.hpp"

namespace duckdb {

//! Register a function or functionSet in the database and add its metadata.
template <typename FUNC>
static void RegisterFunction(ExtensionLoader &loader, FUNC function, CatalogType catalogType, const string &description,
                             const string &example, InsertionOrderPreservingMap<string> &tags) {
	// Register the function
	loader.RegisterFunction(function);
	auto &db = loader.GetDatabaseInstance();

	auto &catalog = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = catalog.GetSchema(transaction, DEFAULT_SCHEMA);
	auto catalog_entry = schema.GetEntry(transaction, catalogType, function.name);
	if (!catalog_entry) {
		// This should not happen, we just registered the function
		throw InternalException("Function with name \"%s\" not found.", function.name.c_str());
	}

	auto &func_entry = catalog_entry->template Cast<FunctionEntry>();

	// Fill a function description and add it to the function entry
	FunctionDescription func_description;
	if (!description.empty()) {
		func_description.description = description;
	}
	if (!example.empty()) {
		func_description.examples.push_back(example);
	}
	for (const auto &tag : tags) {
		func_entry.tags.insert(tag.first, tag.second);
	}

	func_entry.descriptions.push_back(func_description);
}

} // namespace duckdb
