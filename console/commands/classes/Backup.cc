// ----------------------------------------------------------------------
// File: Backup.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "console/commands/classes/Backup.hh"
#include "common/Path.hh"
#include "common/Timing.hh"

#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <thread>
#include <filesystem>
#include <iomanip>

namespace fs = std::filesystem;

namespace eos {
namespace console {

// Add color definitions at the top of the file after includes
namespace {
    // ANSI color codes
    const char* BLUE   = "\033[0;34m";
    const char* GREEN  = "\033[0;32m";
    const char* YELLOW = "\033[0;33m";
    const char* RED    = "\033[0;31m";
    const char* BOLD   = "\033[1m";
    const char* RESET  = "\033[0m";
}

/**
 * @brief Construct a new Backup object with specified paths and configuration
 * 
 * @param src Source directory path
 * @param dst Destination directory path
 * @param cfg Backup configuration
 */
Backup::Backup(const std::string& src, const std::string& dst, const BackupConfig& cfg)
    : src_path(src), dst_path(dst), config(cfg) {}

/**
 * @brief Execute the complete backup operation sequence
 * 
 * This method orchestrates the entire backup process by calling the appropriate
 * methods in sequence. It handles both regular backup operations and optional
 * post-processing steps like squashfs creation.
 */
void Backup::run() {
    if (config.verbose) {
        std::cerr << BLUE << "# Starting backup operation" << RESET << "\n";
        std::cerr << BLUE << "# Source: " << RESET << src_path << "\n";
        std::cerr << BLUE << "# Destination: " << RESET << dst_path << "\n\n";
    }
    
    findFiles();
    createDirectories();
    createSymlinks();
    createFiles();
    printSummary();
    
    if (!config.sparsefilelist.empty()) {
        writeSparseFileList();
    }
    if (!config.mksquash.empty() && !config.dryrun) {
        createSquashfs();
    }
    
    if (config.verbose) {
        std::cerr << BLUE << "# Backup operation completed" << RESET << "\n";
    }
}

/**
 * @brief Convert stat information to a FileEntry structure
 * 
 * @param path File path (used for error reporting)
 * @param buf stat structure containing file metadata
 * @return FileEntry populated with metadata from stat
 */
FileEntry Backup::statToFileEntry(const std::string& path, const struct stat& buf) {
    FileEntry entry;
    entry.mtime = buf.st_mtim;
    entry.mstat = buf;
    entry.size = static_cast<size_t>(buf.st_size);
    return entry;
}

/**
 * @brief Recursively scan source directory and build file tree
 * 
 * This method traverses the source directory structure, applying configured
 * filters and collecting metadata for all filesystem entries. It handles:
 * - Regular files
 * - Directories
 * - Symbolic links
 * - Version files (optional filtering)
 * - Atomic files (optional filtering)
 * - Hidden files (optional filtering)
 */
void Backup::findFiles() {
    if (config.verbose) {
        std::cerr << BLUE << "# Scanning source directory structure..." << RESET << "\n";
    }
    
    eos::common::Path basePath(src_path.c_str());
    struct stat buf;
    size_t file_count = 0, dir_count = 0, link_count = 0;

    for (const auto& entry : fs::recursive_directory_iterator(src_path, fs::directory_options::skip_permission_denied)) {
        std::string p = entry.path().string();
        eos::common::Path iPath(p.c_str());

        // Apply configured filters
        if (config.filter_versions && iPath.isVersionPath()) continue;
        if (config.filter_atomic && iPath.isAtomicFile()) continue;
        if (config.filter_hidden) {
            std::string::size_type pos = iPath.GetFullPath().find("/.");
            if (pos != std::string::npos && !iPath.isVersionPath() && !iPath.isAtomicFile()) continue;
        }

        std::string rel = p;
        if (!::lstat(p.c_str(), &buf)) {
            rel.erase(0, basePath.GetFullPath().length());
            switch (buf.st_mode & S_IFMT) {
                case S_IFDIR:
                    rel += "/";
                    filetree.directories[rel] = statToFileEntry(p, buf);
                    dir_count++;
                    break;
                case S_IFREG:
                    filetree.files[rel] = statToFileEntry(p, buf);
                    file_count++;
                    break;
                case S_IFLNK: {
                    FileEntry e = statToFileEntry(p, buf);
                    char link[4096];
                    ssize_t len = readlink(p.c_str(), link, sizeof(link));
                    if (len >= 0) e.target = std::string(link, len);
                    filetree.links[rel] = e;
                    link_count++;
                    break;
                }
            }
        }
    }

    if (config.verbose) {
        std::cerr << GREEN << "✓ " << RESET 
                  << "Found " << file_count << " files, " 
                  << dir_count << " directories, and "
                  << link_count << " symbolic links\n\n";
    }
}

/**
 * @brief Create directory structure in destination
 * 
 * Creates all directories in the destination with proper:
 * - Permissions
 * - Ownership
 * - Access times
 * - Modification times
 * 
 * In dry-run mode, only prints what would be done.
 */
void Backup::createDirectories() {
    if (config.verbose) {
        std::cerr << BLUE << "# Creating directory structure..." << RESET << "\n";
    }

    size_t created = 0;
    for (const auto& [rel, entry] : filetree.directories) {
        std::string dpath = dst_path + rel;
        if (config.dryrun) {
            if (config.verbose)
                std::cerr << "# Would create directory '" << dpath << "'\n";
            continue;
        }
        fs::create_directories(dpath);
        chmod(dpath.c_str(), entry.mstat.st_mode);
        chown(dpath.c_str(), entry.mstat.st_uid, entry.mstat.st_gid);
        struct timespec tv[2] = { entry.mstat.st_atim, entry.mstat.st_mtim };
        utimensat(AT_FDCWD, dpath.c_str(), tv, 0);
        created++;
    }

    if (config.verbose && !config.dryrun) {
        std::cerr << GREEN << "✓ " << RESET 
                  << "Created " << created << " directories\n\n";
    }
}

/**
 * @brief Create symbolic links in destination
 * 
 * Creates all symbolic links with proper:
 * - Target paths
 * - Ownership
 * 
 * In dry-run mode, only prints what would be done.
 */
void Backup::createSymlinks() {
    if (config.verbose) {
        std::cerr << BLUE << "# Creating symbolic links..." << RESET << "\n";
    }

    size_t created = 0;
    for (const auto& [rel, entry] : filetree.links) {
        std::string link_path = dst_path + rel;
        if (config.dryrun) {
            if (config.verbose)
                std::cerr << "# Would create symlink '" << link_path << "' -> '" << entry.target << "'\n";
            continue;
        }
        symlink(entry.target.c_str(), link_path.c_str());
        lchown(link_path.c_str(), entry.mstat.st_uid, entry.mstat.st_gid);
        created++;
    }

    if (config.verbose && !config.dryrun) {
        std::cerr << GREEN << "✓ " << RESET 
                  << "Created " << created << " symbolic links\n\n";
    }
}

/**
 * @brief Copy files to destination
 * 
 * Handles both regular and sparse file copying. For each file:
 * - Determines if it should be treated as sparse based on size
 * - Creates the file with proper permissions
 * - Copies content for non-sparse files
 * - Sets proper ownership and timestamps
 * - Tracks sparse files for later processing
 * 
 * In dry-run mode, only prints what would be done.
 */
void Backup::createFiles() {
    if (config.verbose) {
        std::cerr << BLUE << "# Copying files..." << RESET << "\n";
    }

    size_t copied_sparse = 0, copied_regular = 0;
    size_t total_files = filetree.files.size();
    size_t current_file = 0;
    size_t total_bytes = 0;

    for (const auto& [rel, entry] : filetree.files) {
        current_file++;
        std::string target = dst_path + rel;
        std::string source = src_path + rel;
        bool is_sparse = entry.size > config.min_sparse_size;

        if (is_sparse) {
            sparse_files.push_back(rel);
            copied_sparse++;
        } else {
            copied_regular++;
        }

        if (config.verbose) {
            float progress = (float)current_file / total_files * 100;
            std::cerr << "\r" << BLUE << "# Progress: " << RESET 
                      << std::fixed << std::setprecision(1) << progress << "% "
                      << "(" << current_file << "/" << total_files << ") "
                      << formatSize(total_bytes);
        }

        if (config.dryrun) {
            if (config.debug)
                std::cerr << "\n# Would copy " << (is_sparse ? "sparse" : "regular") 
                          << " file '" << target << "' [size=" << formatSize(entry.size) << "]\n";
            continue;
        }

        int fd = ::open(target.c_str(), O_RDWR | O_CREAT | O_TRUNC, entry.mstat.st_mode);
        if (fd < 0) continue;

        if (is_sparse) {
            ::ftruncate(fd, entry.size);
            total_bytes += entry.size;
        } else {
            int fdsrc = ::open(source.c_str(), O_RDONLY);
            if (fdsrc >= 0) {
                char buf[4096];
                ssize_t r;
                while ((r = read(fdsrc, buf, sizeof(buf))) > 0) {
                    write(fd, buf, r);
                    total_bytes += r;
                }
                close(fdsrc);
            }
        }

        fchown(fd, entry.mstat.st_uid, entry.mstat.st_gid);
        fchmod(fd, entry.mstat.st_mode);
        struct timespec tv[2] = { entry.mstat.st_atim, entry.mstat.st_mtim };
        futimens(fd, tv);
        close(fd);
    }

    if (config.verbose) {
        std::cerr << "\n" << GREEN << "✓ " << RESET 
                  << "Copied " << copied_regular << " regular files and "
                  << copied_sparse << " sparse files "
                  << "(" << formatSize(total_bytes) << " total)\n\n";
    }
}

/**
 * @brief Print summary of backup operation
 * 
 * If verbose mode is enabled, prints statistics about:
 * - Number and total size of sparse files
 * - Number and total size of regular files
 */
void Backup::printSummary() {
    if (!config.verbose) return;
    size_t total_sparse = 0, total_files_sparse = 0;
    size_t total_real = 0, total_files_real = 0;

    for (const auto& [_, entry] : filetree.files) {
        if (entry.size > config.min_sparse_size) {
            total_sparse += entry.size;
            total_files_sparse++;
        } else {
            total_real += entry.size;
            total_files_real++;
        }
    }

    std::cerr << "\n# Summary:\n";
    std::cerr << "# sparse contents: " << total_sparse << " bytes, " << total_files_sparse << " files\n";
    std::cerr << "# real   contents: " << total_real << " bytes, " << total_files_real << " files\n";
}

std::string Backup::compressionToString(SquashfsCompression method) {
    switch (method) {
        case SquashfsCompression::ZSTD: return "zstd";
        case SquashfsCompression::GZIP: return "gzip";
        case SquashfsCompression::XZ:   return "xz";
        case SquashfsCompression::LZO:  return "lzo";
        case SquashfsCompression::LZ4:  return "lz4";
        default: return "";
    }
}

bool Backup::isCompressionSupported(SquashfsCompression method) {
    if (method == SquashfsCompression::DEFAULT) return true;

    FILE* pipe = popen("mksquashfs -help 2>&1", "r");
    if (!pipe) return false;

    char buffer[128];
    std::string result;
    while (!feof(pipe)) {
        if (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
            result += buffer;
        }
    }
    pclose(pipe);

    std::string compression_name = compressionToString(method);
    return (result.find(compression_name) != std::string::npos);
}

std::pair<SquashfsCompression, std::string> Backup::getBestCompression() {
    // Try each compression method in order of preference
    for (const auto& method : config.compression_priority) {
        if (isCompressionSupported(method)) {
            std::string options = "-comp " + compressionToString(method);
            
            // Add compression level if specified
            if (config.compression_level >= 0) {
                // ZSTD supports levels 1-22, others typically 1-9
                int max_level = (method == SquashfsCompression::ZSTD) ? 22 : 9;
                int level = std::min(config.compression_level, max_level);
                options += " -Xcompression-level " + std::to_string(level);
            }
            
            return {method, options};
        }
    }
    
    // If nothing else works, return default compression
    return {SquashfsCompression::DEFAULT, ""};
}

/**
 * @brief Calculate total size of all files in the backup
 * @return Total size in bytes
 */
size_t Backup::calculateTotalInputSize() const {
    size_t total = 0;
    for (const auto& [_, entry] : filetree.files) {
        total += entry.size;
    }
    return total;
}

/**
 * @brief Get size of a file
 * @param path Path to the file
 * @return Size in bytes, 0 if file cannot be accessed
 */
size_t Backup::getFileSize(const std::string& path) const {
    struct stat st;
    if (stat(path.c_str(), &st) == 0) {
        return st.st_size;
    }
    return 0;
}

/**
 * @brief Format size in human readable format
 * @param size Size in bytes
 * @return Formatted string (e.g., "1.2 GB", "340.5 MB", etc.)
 */
std::string Backup::formatSize(size_t size) const {
    static const char* units[] = {"B", "KB", "MB", "GB", "TB"};
    int unit = 0;
    double size_d = static_cast<double>(size);
    
    while (size_d >= 1024.0 && unit < 4) {
        size_d /= 1024.0;
        unit++;
    }
    
    char buf[32];
    snprintf(buf, sizeof(buf), "%.1f %s", size_d, units[unit]);
    return std::string(buf);
}

/**
 * @brief Create squashfs archive of destination
 * 
 * Creates a compressed squashfs archive using:
 * - ZSTD compression if available, otherwise default compression
 * - Parallel processing based on available CPU cores
 * - No append mode (creates new archive)
 */
void Backup::createSquashfs() {
    // Get input statistics before compression
    size_t total_input_size = calculateTotalInputSize();
    size_t total_files = filetree.files.size();
    size_t total_dirs = filetree.directories.size();
    size_t total_links = filetree.links.size();

    // Get compression method and create archive
    auto [method, compression_opts] = getBestCompression();
    
    std::string options = compression_opts;
    if (!options.empty()) options += " ";
    options += "-processors " + std::to_string(std::thread::hardware_concurrency());
    
    std::string command = "mksquashfs \"" + dst_path + "\" \"" + config.mksquash + "\" " + options + " -noappend";
    if (!config.debug) {
        command += " >/dev/null 2>&1";
    }
    
    if (config.verbose) {
        std::cerr << BLUE << "# Creating squashfs archive..." << RESET << "\n";
        if (config.debug) {
            std::cerr << BLUE << "# Command: " << RESET << command << "\n";
        }
    }
    
    int ret = std::system(command.c_str());
    if (ret != 0) {
        std::cerr << RED << "Error: mksquashfs failed with return code " << ret << RESET << "\n";
        if (method != SquashfsCompression::DEFAULT) {
            std::cerr << YELLOW << "Tip: Try using a different compression method or no compression" << RESET << "\n";
        }
        return;
    }

    // Get output file size and calculate statistics
    size_t output_size = getFileSize(config.mksquash);
    double compression_ratio = output_size > 0 ? static_cast<double>(total_input_size) / output_size : 0;
    double space_saving = output_size > 0 ? (1.0 - static_cast<double>(output_size) / total_input_size) * 100.0 : 0;

    // Print detailed summary
    std::cerr << "\n" << BOLD << BLUE 
              << "╔══════════════════════════════════╗\n"
              << "║     Squashfs Creation Summary    ║\n"
              << "╚══════════════════════════════════╝" << RESET << "\n\n"
              
              << YELLOW << "Content Statistics:" << RESET << "\n"
              << BLUE << "  ⊢ Files:       " << RESET << total_files << "\n"
              << BLUE << "  ⊢ Directories: " << RESET << total_dirs << "\n"
              << BLUE << "  └ Symlinks:    " << RESET << total_links << "\n\n"
              
              << YELLOW << "Size Information:" << RESET << "\n"
              << BLUE << "  ⊢ Input size:  " << RESET << formatSize(total_input_size) 
              << " (" << total_input_size << " bytes)\n"
              << BLUE << "  └ Output size: " << RESET << formatSize(output_size)
              << " (" << output_size << " bytes)\n\n"
              
              << YELLOW << "Compression Results:" << RESET << "\n"
              << BLUE << "  ⊢ Method:      " << RESET 
              << (method == SquashfsCompression::DEFAULT ? "default" : compressionToString(method)) << "\n"
              << BLUE << "  ⊢ Ratio:       " << RESET << std::fixed << std::setprecision(2) 
              << compression_ratio << ":1\n"
              << BLUE << "  └ Space saved: " << GREEN << std::fixed << std::setprecision(1) 
              << space_saving << "%" << RESET << "\n\n";
}

/**
 * @brief Write list of sparse files to output file
 * 
 * Creates a text file containing the relative paths of all files
 * that were treated as sparse during the backup operation.
 */
void Backup::writeSparseFileList() {
    if (config.verbose) {
        std::cerr << BLUE << "# Writing sparse file list..." << RESET << "\n";
    }

    if (config.sparsefilelist.empty()) return;

    std::ofstream ofs(config.sparsefilelist);
    if (!ofs) {
        std::cerr << RED << "Error: cannot write sparse file list to '" 
                  << config.sparsefilelist << "'" << RESET << "\n";
        return;
    }

    for (const auto& relpath : sparse_files) {
        ofs << relpath << "\n";
    }

    ofs.close();
    if (config.verbose) {
        std::cerr << GREEN << "✓ " << RESET 
                  << "Wrote " << sparse_files.size() 
                  << " entries to " << config.sparsefilelist << "\n\n";
    }
}

}}
