#pragma once

// DuckDB
#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/settings.hpp"

// GDAL
#include "cpl_vsi_virtual.h"
#include "gdal_priv.h"

namespace duckdb {

//======================================================================================================================
// GDAL File System
// NOTE:
//	This file implements a custom GDAL VSI handler that allows GDAL to read files through DuckDB's file system.
//	This allows GDAL to read from any data source that DuckDB supports (e.g. HTTP, S3, Azure, ...).
//
//	This code is a copy of the implementation from duckdb-spatial extension:
//	https://github.com/duckdb/duckdb-spatial/blob/main/src/spatial/modules/gdal/gdal_module.cpp
//
//	Thanks a lot Max (https://github.com/Maxxen) for open sourcing it!
//
//======================================================================================================================

class DuckDBFileHandle final : public VSIVirtualHandle {
public:
	explicit DuckDBFileHandle(unique_ptr<FileHandle> file_handle_p)
	    : file_handle(std::move(file_handle_p)), is_eof(false), can_seek(file_handle->CanSeek()) {
	}

	vsi_l_offset Tell() override {
		return static_cast<vsi_l_offset>(file_handle->SeekPosition());
	}

	int Seek(vsi_l_offset nOffset, int nWhence) override {
		// Reset EOF flag on seek
		is_eof = false;

		// Use the reset function instead to allow compressed file handles to rewind
		// even if they don't support seeking
		if (nWhence == SEEK_SET && nOffset == 0) {
			file_handle->Reset();
			return 0;
		}

		switch (nWhence) {
		case SEEK_SET:
			file_handle->Seek(nOffset);
			return 0;
		case SEEK_CUR:
			file_handle->Seek(file_handle->SeekPosition() + nOffset);
			return 0;
		case SEEK_END:
			file_handle->Seek(file_handle->GetFileSize() + nOffset);
			return 0;
		default:
			return -1;
		}
	}

	size_t Read(void *buffer, size_t size, size_t count) override {
		auto bytes_data = static_cast<char *>(buffer);
		auto bytes_left = size * count;

		try {
			while (bytes_left > 0) {
				const auto bytes_read = file_handle->Read(bytes_data, bytes_left);
				if (bytes_read == 0) {
					is_eof = true;
					break;
				}
				bytes_left -= bytes_read;
				bytes_data += bytes_read;
			}
		} catch (...) {
			if (bytes_left != 0) {
				if (file_handle->SeekPosition() == file_handle->GetFileSize()) {
					// Is at EOF!
					is_eof = true;
				}
			} else {
				// else, error!
				// unfortunately, this version of GDAL cant distinguish between errors and reading less bytes
				// its avaiable in 3.9.2, but we're stuck on 3.8.5 for now.
				throw;
			}
		}

		return count - (bytes_left / size);
	}

	int Eof() override {
		return is_eof ? TRUE : FALSE;
	}

	size_t Write(const void *buffer, size_t size, size_t count) override {
		size_t written_bytes = 0;
		try {
			written_bytes = file_handle->Write(const_cast<void *>(buffer), size * count);
		} catch (...) {
			// ignore
		}
		return written_bytes / size;
	}

	int Flush() override {
		file_handle->Sync();
		return 0;
	}
	int Truncate(vsi_l_offset nNewSize) override {
		file_handle->Truncate(static_cast<int64_t>(nNewSize));
		return 0;
	}
	int Close() override {
		file_handle->Close();
		return 0;
	}

	void ClearErr() override {
		// no-op: EOF tracking is handled by the is_eof flag
	}
	int Error() override {
		return 0;
	}

private:
	unique_ptr<FileHandle> file_handle = nullptr;
	bool is_eof = false;
	bool can_seek = false;
};

class DuckDBFileSystemHandler final : public VSIFilesystemHandler {
public:
	DuckDBFileSystemHandler(string client_prefix, ClientContext &context)
	    : client_prefix(std::move(client_prefix)), context(context) {};

	const char *StripPrefix(const char *pszFilename) const {
		return pszFilename + client_prefix.size();
	}
	string AddPrefix(const string &value) const {
		return client_prefix + value;
	}

	VSIVirtualHandleUniquePtr Open(const char *gdal_file_path, const char *access, bool set_error,
	                               CSLConstList /*papszoptions */) override {
		// Strip the prefix to get the real file path
		const auto real_file_path = StripPrefix(gdal_file_path);

		// Get the DuckDB file system
		auto &fs = FileSystem::GetFileSystem(context);

		// Determine the file open flags
		FileOpenFlags flags;
		const auto len = strlen(access);
		if (access[0] == 'r') {
			flags = FileFlags::FILE_FLAGS_READ;
			if (len > 1 && access[1] == '+') {
				flags |= FileFlags::FILE_FLAGS_WRITE;
			}
			if (len > 2 && access[2] == '+') {
				// might be "rb+"
				flags |= FileFlags::FILE_FLAGS_WRITE;
			}
		} else if (access[0] == 'w') {
			flags = FileFlags::FILE_FLAGS_WRITE;
			if (!fs.IsPipe(real_file_path)) {
				flags |= FileFlags::FILE_FLAGS_FILE_CREATE_NEW;
			}
			if (len > 1 && access[1] == '+') {
				flags |= FileFlags::FILE_FLAGS_READ;
			}
			if (len > 2 && access[2] == '+') {
				// might be "wb+"
				flags |= FileFlags::FILE_FLAGS_READ;
			}
		} else if (access[0] == 'a') {
			flags = FileFlags::FILE_FLAGS_APPEND;
			if (len > 1 && access[1] == '+') {
				flags |= FileFlags::FILE_FLAGS_READ;
			}
			if (len > 2 && access[2] == '+') {
				// might be "ab+"
				flags |= FileFlags::FILE_FLAGS_READ;
			}
		} else {
			throw InternalException("Unknown file access type");
		}

		try {
			auto file = fs.OpenFile(real_file_path, flags | FileCompressionType::AUTO_DETECT);
			return VSIVirtualHandleUniquePtr(new DuckDBFileHandle(std::move(file)));

		} catch (std::exception &ex) {
			// Extract error message from DuckDB
			const ErrorData error_data(ex);

			// Failed to open file via DuckDB File System. If this doesnt have a VSI prefix we can return an error here.
			if (strncmp(real_file_path, "/vsi", 4) != 0) {
				if (set_error) {
					VSIError(VSIE_FileError, "%s", error_data.RawMessage().c_str());
				}
				return nullptr;
			}

			// Fall back to GDAL instead (if external access is enabled)
			if (!Settings::Get<EnableExternalAccessSetting>(context)) {
				if (set_error) {
					VSIError(VSIE_FileError, "%s", error_data.RawMessage().c_str());
				}
				return nullptr;
			}

			const auto handler = VSIFileManager::GetHandler(real_file_path);
			if (!handler) {
				if (set_error) {
					VSIError(VSIE_FileError, "%s", error_data.RawMessage().c_str());
				}
				return nullptr;
			}

			return handler->Open(real_file_path, access);
		}
	}

	int Stat(const char *gdal_file_name, VSIStatBufL *result, int n_flags) override {
		auto real_file_path = StripPrefix(gdal_file_name);
		auto &fs = FileSystem::GetFileSystem(context);

		memset(result, 0, sizeof(VSIStatBufL));

		if (fs.IsPipe(real_file_path)) {
			result->st_mode = S_IFCHR;
			return 0;
		}

		if (!(fs.FileExists(real_file_path) ||
		      (!FileSystem::IsRemoteFile(real_file_path) && fs.DirectoryExists(real_file_path)))) {
			return -1;
		}

#ifdef _WIN32
		if (!FileSystem::IsRemoteFile(real_file_path) && fs.DirectoryExists(real_file_path)) {
			result->st_mode = S_IFDIR;
			return 0;
		}
#endif

		FileOpenFlags flags;
		flags |= FileFlags::FILE_FLAGS_READ;
		flags |= FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS;
		flags |= FileCompressionType::AUTO_DETECT;

		const auto file = fs.OpenFile(real_file_path, flags);
		if (!file) {
			return -1;
		}

		try {
			result->st_size = static_cast<off_t>(fs.GetFileSize(*file));
		} catch (...) {
		}
		try {
			result->st_mtime = Timestamp::ToTimeT(fs.GetLastModifiedTime(*file));
		} catch (...) {
		}
		try {
			const auto type = file->GetType();
			switch (type) {
			case FileType::FILE_TYPE_REGULAR:
				result->st_mode = S_IFREG;
				break;
			case FileType::FILE_TYPE_DIR:
				result->st_mode = S_IFDIR;
				break;
			case FileType::FILE_TYPE_CHARDEV:
				result->st_mode = S_IFCHR;
				break;
			default:
				// HTTPFS returns invalid type for everything basically.
				if (FileSystem::IsRemoteFile(real_file_path)) {
					result->st_mode = S_IFREG;
				} else {
					return -1;
				}
			}
		} catch (...) {
		}
		return 0;
	}

	bool IsLocal(const char *gdal_file_path) const override {
		const auto real_file_path = StripPrefix(gdal_file_path);
		return !FileSystem::IsRemoteFile(real_file_path);
	}

	int Mkdir(const char *pszDirname, long nMode) override {
		auto &fs = FileSystem::GetFileSystem(context);
		const auto dir_name = StripPrefix(pszDirname);

		fs.CreateDirectory(dir_name);
		return 0;
	}

	int Rmdir(const char *pszDirname) override {
		auto &fs = FileSystem::GetFileSystem(context);
		const auto dir_name = StripPrefix(pszDirname);

		fs.RemoveDirectory(dir_name);
		return 0;
	}

	int RmdirRecursive(const char *pszDirname) override {
		auto &fs = FileSystem::GetFileSystem(context);
		const auto dir_name = StripPrefix(pszDirname);

		fs.RemoveDirectory(dir_name);
		return 0;
	}

	char **ReadDirEx(const char *gdal_dir_name, int max_files) override {
		auto &fs = FileSystem::GetFileSystem(context);
		const auto dir_name = StripPrefix(gdal_dir_name);

		CPLStringList files;
		auto files_count = 0;
		fs.ListFiles(dir_name, [&](const string &file_name, bool is_dir) {
			if (files_count >= max_files) {
				return;
			}
			const auto tmp = AddPrefix(file_name);
			files.AddString(tmp.c_str());
			files_count++;
		});
		return files.StealList();
	}

	char **SiblingFiles(const char *gdal_file_path) override {
		auto &fs = FileSystem::GetFileSystem(context);

		const auto real_file_path = StripPrefix(gdal_file_path);

		const auto real_file_stem = StringUtil::GetFileStem(real_file_path);
		const auto base_file_path = fs.JoinPath(StringUtil::GetFilePath(real_file_path), real_file_stem);
		const auto glob_file_path = base_file_path + ".*";

		if (fs.IsRemoteFile(base_file_path)) {
			// Sibling file listing is expensive for remote files, so avoid it here.
			// GDAL will fall back to a ReadDir if needed.
			return nullptr;
		}

		CPLStringList files;
		for (auto &file : fs.Glob(glob_file_path)) {
			files.AddString(AddPrefix(file.path).c_str());
		}
		return files.StealList();
	}

	int HasOptimizedReadMultiRange(const char *pszPath) override {
		return 0;
	}

	int Unlink(const char *prefixed_file_name) override {
		auto &fs = FileSystem::GetFileSystem(context);
		const auto real_file_path = StripPrefix(prefixed_file_name);
		try {
			fs.RemoveFile(real_file_path);
			return 0;
		} catch (...) {
			return -1;
		}
	}

	int Rename(const char *oldpath, const char *newpath, GDALProgressFunc /*pProgressFunc*/,
	           void * /*pProgressData*/) override {
		auto &fs = FileSystem::GetFileSystem(context);
		const auto real_old_path = StripPrefix(oldpath);
		const auto real_new_path = StripPrefix(newpath);

		try {
			fs.MoveFile(real_old_path, real_new_path);
			return 0;
		} catch (...) {
			return -1;
		}
	}

private:
	string client_prefix;
	ClientContext &context;
};

class DuckDBFileSystemPrefix final : public ClientContextState {
public:
	// Use an intentionally leaked heap-allocated mutex to avoid static destruction order issues.
	// A static mutex (either class-level or function-local) can be destroyed before the last
	// DuckDBFileSystemPrefix destructor runs during program teardown, causing
	// "mutex lock failed: Invalid argument". By heap-allocating and never freeing, we guarantee
	// the mutex outlives all users.
	static mutex &GetVSIMutex() {
		static mutex *mtx = new mutex();
		return *mtx;
	}

	explicit DuckDBFileSystemPrefix(ClientContext &context) : context(context) {
		// Create a new random prefix for this client
		client_prefix = StringUtil::Format("/vsiduckdb-%s/", UUID::ToString(UUID::GenerateRandomUUID()));

		// Create a new file handler responding to this prefix
		fs_handler = make_uniq<DuckDBFileSystemHandler>(client_prefix, context);

		// Register the file handler
		lock_guard<mutex> lock(GetVSIMutex());
		VSIFileManager::InstallHandler(client_prefix, fs_handler.get());
	}

	// Delete copy
	DuckDBFileSystemPrefix(const DuckDBFileSystemPrefix &) = delete;
	DuckDBFileSystemPrefix &operator=(const DuckDBFileSystemPrefix &) = delete;

	~DuckDBFileSystemPrefix() override {
		// Uninstall the file handler for this prefix
		lock_guard<mutex> lock(GetVSIMutex());
		VSIFileManager::RemoveHandler(client_prefix);
	}

	// Check if a path looks like a GDAL driver-prefixed URL (e.g., "WFS:https://...", "OAPIF:https://...")
	// These need to be passed directly to GDALOpenEx without our custom VSI prefix.
	static bool IsGDALDriverPrefixedURL(const string &value) {
		auto colon_pos = value.find(':');
		if (colon_pos == string::npos || colon_pos == 0 || colon_pos > 20) {
			return false;
		}
		// Check that the prefix is all uppercase letters (GDAL driver names are uppercase)
		for (idx_t i = 0; i < colon_pos; i++) {
			char c = value[i];
			if (!StringUtil::CharacterIsAlpha(c) || c != StringUtil::CharacterToUpper(c)) {
				return false;
			}
		}
		// Check that after the colon there is a URL scheme (http:// or https://)
		// This excludes database connection strings like "PG:dbname=..." which are not supported.
		auto rest = value.substr(colon_pos + 1);
		return StringUtil::StartsWith(rest, "http://") || StringUtil::StartsWith(rest, "https://");
	}

	string AddPrefix(const string &value) const {
		// If the user explicitly asked for a VSI prefix, we don't add our own
		if (StringUtil::StartsWith(value, "/vsi")) {
			if (!Settings::Get<EnableExternalAccessSetting>(context)) {
				throw PermissionException("Cannot open file '%s' with VSI prefix: External access is disabled", value);
			}
			return value;
		}
		// If the path is a GDAL driver-prefixed URL (e.g., "WFS:https://..."), pass it through directly
		if (IsGDALDriverPrefixedURL(value)) {
			if (!Settings::Get<EnableExternalAccessSetting>(context)) {
				throw PermissionException("Cannot open file '%s' with GDAL driver prefix: External access is disabled",
				                          value);
			}
			return value;
		}
		return client_prefix + value;
	}

	static DuckDBFileSystemPrefix &GetOrCreate(ClientContext &context) {
		return *context.registered_state->GetOrCreate<DuckDBFileSystemPrefix>("gdal-raster-fs", context);
	}

private:
	ClientContext &context;
	string client_prefix;
	unique_ptr<DuckDBFileSystemHandler> fs_handler;
};

} // namespace duckdb
