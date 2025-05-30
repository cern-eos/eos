// ----------------------------------------------------------------------
// File: Backup.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/ASwitzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once

#include <string>
#include <map>
#include <filesystem>
#include <sys/stat.h>
#include <vector>

namespace eos {
namespace console {

/**
 * @brief Supported compression methods for squashfs creation
 */
enum class SquashfsCompression {
    DEFAULT,    ///< System default compression
    ZSTD,       ///< ZSTD compression (best compression/speed ratio)
    GZIP,       ///< GZIP compression (widely supported)
    XZ,         ///< XZ compression (best compression)
    LZO,        ///< LZO compression (fastest)
    LZ4,        ///< LZ4 compression (fast with good compression)
};

/**
 * @brief Configuration structure for backup operations
 * 
 * Contains all configurable parameters that control the behavior
 * of the backup process, including filtering options, operation modes,
 * and output controls.
 */
struct BackupConfig {
    bool dryrun = false;        ///< Only show what would be done without actual copying
    bool noreplace = false;     ///< Don't replace existing files
    bool nodelete = false;      ///< Don't delete files in destination not present in source
    bool verbose = false;       ///< Show detailed progress information
    bool is_silent = false;     ///< Suppress all output
    bool debug = false;         ///< Show debug information including command output
    bool filter_versions = false; ///< Skip version files during copy
    bool filter_atomic = false;  ///< Skip atomic files during copy
    bool filter_hidden = false;  ///< Skip hidden files during copy
    size_t min_sparse_size = 0; ///< Minimum size for treating files as sparse
    std::string mksquash;       ///< Path where to create squashfs archive
    std::string sparsefilelist; ///< Path where to write list of sparse files
    
    // Squashfs compression settings
    std::vector<SquashfsCompression> compression_priority = {  ///< Ordered list of preferred compression methods
        SquashfsCompression::ZSTD,
        SquashfsCompression::LZ4,
        SquashfsCompression::GZIP,
        SquashfsCompression::DEFAULT
    };
    int compression_level = -1;  ///< Compression level (-1 = default, 1-22 for ZSTD, 1-9 for others)
};

/**
 * @brief Structure to hold file system entry information
 * 
 * Contains metadata about a file system entry including timestamps,
 * full stat information, size, and symbolic link target if applicable.
 */
struct FileEntry {
    struct timespec mtime;      ///< Modification time
    struct stat mstat;          ///< Full stat information
    size_t size;               ///< File size
    std::string target;        ///< Symbolic link target (if entry is a symlink)
};

/**
 * @brief Class implementing directory backup functionality
 * 
 * The Backup class provides a comprehensive solution for copying directory
 * trees while preserving all metadata and handling special cases like:
 * - Sparse files for efficient storage
 * - Symbolic links
 * - File permissions and ownership
 * - Timestamps
 * - Optional squashfs archive creation
 * 
 * It supports various filtering options and can operate in dry-run mode
 * for testing purposes. The class is designed to be memory efficient
 * by processing files in a streaming manner.
 */
class Backup {
public:
    /**
     * @brief Construct a new Backup object
     * 
     * @param src Source directory path
     * @param dst Destination directory path
     * @param config Backup configuration parameters
     */
    Backup(const std::string& src, const std::string& dst, const BackupConfig& config);

    /**
     * @brief Execute the backup operation
     * 
     * This is the main entry point that orchestrates the entire backup process:
     * 1. Scans the source directory tree
     * 2. Creates destination directory structure
     * 3. Copies files with appropriate handling of sparse files
     * 4. Creates symbolic links
     * 5. Optionally creates squashfs archive
     * 6. Generates sparse file list if requested
     */
    void run();

    /**
     * @brief Get the list of sparse files identified during backup
     * 
     * @return const std::vector<std::string>& List of relative paths of sparse files
     * @note Only contains valid data after run() has been called
     */
    const std::vector<std::string>& getSparseFiles() const { return sparse_files; }

private:
    std::string src_path;       ///< Source directory path
    std::string dst_path;       ///< Destination directory path
    BackupConfig config;        ///< Backup configuration
    std::vector<std::string> sparse_files;  ///< List of identified sparse files

    /**
     * @brief Structure to hold the complete file system tree
     * 
     * Maintains separate maps for different types of filesystem entries
     * to allow efficient processing based on entry type.
     */
    struct FileTree {
        std::map<std::string, FileEntry> directories;  ///< Directory entries
        std::map<std::string, FileEntry> files;        ///< Regular file entries
        std::map<std::string, FileEntry> links;        ///< Symbolic link entries
    } filetree;

    /**
     * @brief Scan source directory and build file tree
     * 
     * Recursively traverses the source directory, applying configured filters
     * and collecting metadata for all filesystem entries.
     */
    void findFiles();

    /**
     * @brief Create directory structure in destination
     * 
     * Creates all directories with proper permissions, ownership,
     * and timestamps.
     */
    void createDirectories();

    /**
     * @brief Create symbolic links in destination
     * 
     * Creates symbolic links with proper targets and metadata.
     */
    void createSymlinks();

    /**
     * @brief Copy files to destination
     * 
     * Handles regular and sparse file copying, preserving all metadata.
     * Tracks sparse files for later processing if configured.
     */
    void createFiles();

    /**
     * @brief Print operation summary
     * 
     * Outputs statistics about copied files, including counts and sizes
     * for both regular and sparse files.
     */
    void printSummary();

    /**
     * @brief Create squashfs archive
     * 
     * Creates a compressed squashfs archive of the destination directory
     * if configured. Uses system's mksquashfs command.
     */
    void createSquashfs();

    /**
     * @brief Write list of sparse files
     * 
     * Writes the list of identified sparse files to the configured
     * output file.
     */
    void writeSparseFileList();

    /**
     * @brief Calculate total size of all files in the backup
     * @return Total size in bytes
     */
    size_t calculateTotalInputSize() const;

    /**
     * @brief Get size of a file
     * @param path Path to the file
     * @return Size in bytes, 0 if file cannot be accessed
     */
    size_t getFileSize(const std::string& path) const;

    /**
     * @brief Format size in human readable format
     * @param size Size in bytes
     * @return Formatted string (e.g., "1.2 GB", "340.5 MB", etc.)
     */
    std::string formatSize(size_t size) const;

    /**
     * @brief Convert stat information to FileEntry
     * 
     * @param path File path
     * @param buf stat structure
     * @return FileEntry containing metadata
     */
    static FileEntry statToFileEntry(const std::string& path, const struct stat& buf);

    /**
     * @brief Check if a specific compression method is supported
     * @param method The compression method to check
     * @return true if the compression method is supported, false otherwise
     */
    bool isCompressionSupported(SquashfsCompression method);

    /**
     * @brief Get the best available compression method
     * @return pair of (compression method, compression options string)
     */
    std::pair<SquashfsCompression, std::string> getBestCompression();

    /**
     * @brief Convert compression method to command line option
     * @param method The compression method
     * @return Command line option string for mksquashfs
     */
    std::string compressionToString(SquashfsCompression method);
};

}}
