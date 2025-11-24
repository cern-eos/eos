// ----------------------------------------------------------------------
// File: zstdtail.cc
// Author: EOS Team - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

// zstdtail.cpp — "tail -f" semantics for .zst files (frame-aware)
// Build: g++ -std=c++17 -O2 zstdtail.cpp -o zstdtail -lzstd
//
// Usage:
//   zstdtail /path/to/file-or-symlink.zst           # default: follow-only (do not read existing content)
//   zstdtail -f /path/to/file-or-symlink.zst        # same as default: follow-only
//   zstdtail -100 /path/to/file-or-symlink.zst      # print last 100 decompressed lines, then exit
//   zstdtail -100f /path/to/file-or-symlink.zst     # print last 100 decompressed lines, then follow
//   zstdtail -n 200 -f /path/to/file-or-symlink.zst # print last 200 lines, then follow (alternate form)
//
// Notes:
// - Follow-only mode intentionally does NOT read or decompress the current file’s existing content.
//   Due to ZSTD frame structure, decoding cannot start mid-frame. Therefore, follow-only will wait
//   for rotation (new file / new symlink target) and start from the beginning of the new segment.
// - Tail-N modes (-N or -n N) must decompress from the beginning to find the last N lines. A ring
//   buffer is used to bound memory, and nothing is printed until the initial scan reaches EOF.
//
// Behavior:
// - Decompresses all complete frames currently in the file.
// - If the file grows: attempts to decode newly appended frames.
// - If inside-frame and more bytes are needed, waits for more data.
// - If the symlink retargets or file rotates (inode changes): starts from the beginning of the new file.
// Limitations:
// - Like any zstd decoder, it cannot decode an *incomplete* frame. It will stall until the frame is closed.
// - If the writer appends in-place to an existing frame, we must re-feed from the beginning of that frame.
//   (This program handles that by retaining any "leftover" bytes between reads and waiting for more.)

#include <zstd.h>

#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <deque>
#include <cctype>

namespace {

volatile std::sig_atomic_t g_stop = 0;

void on_signal(int) { g_stop = 1; }

struct DevIno {
    dev_t dev{};
    ino_t ino{};
    bool operator==(const DevIno& o) const { return dev == o.dev && ino == o.ino; }
    bool operator!=(const DevIno& o) const { return !(*this == o); }
};

bool lstat_dev_ino(const std::string& path, DevIno& outDI, std::string* resolved = nullptr) {
    // Resolve symlink each time to follow retargets.
    char buf[PATH_MAX];
    ssize_t n = readlink(path.c_str(), buf, sizeof(buf)-1);
    std::string targetPath = path;
    if (n >= 0) {
        buf[n] = '\0';
        if (resolved) *resolved = std::string(buf);
        // If relative symlink, interpret relative to dirname(path)
        if (buf[0] != '/') {
            auto slash = path.find_last_of('/');
            std::string dir = (slash == std::string::npos) ? "." : path.substr(0, slash);
            targetPath = dir + "/" + buf;
        } else {
            targetPath = std::string(buf);
        }
    } else {
        if (resolved) *resolved = path;
    }

    struct stat st{};
    if (stat(targetPath.c_str(), &st) != 0) {
        return false;
    }
    outDI.dev = st.st_dev;
    outDI.ino = st.st_ino;
    if (resolved) *resolved = targetPath;
    return true;
}

// Open file for reading (no O_APPEND). Return fd or -1.
int open_follow(const std::string& path) {
    // Follow symlink here; if it disappears, we’ll retry.
    std::string resolved;
    DevIno di{};
    if (!lstat_dev_ino(path, di, &resolved)) return -1;
    int fd = ::open(resolved.c_str(), O_RDONLY | O_CLOEXEC);
    return fd;
}

// (intentionally no unused helpers)

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr,
                     "usage:\n"
                     "  %s [-f] <file.zst>\n"
                     "  %s -N[f] <file.zst>         (e.g. -100 or -100f)\n"
                     "  %s -n N [-f] <file.zst>\n",
                     argv[0], argv[0], argv[0]);
        return 2;
    }
    // Parse args
    bool wantFollow = false;          // follow after initial action
    bool followOnly = false;          // do not read existing content; follow new segments/frames only
    long tailLines = -1;              // -1 => no tail-N priming; >=0 => keep last N lines then (maybe) follow
    std::string path;

    auto parseInt = [](const char* s, long& out) -> bool {
        char* end = nullptr;
        long v = std::strtol(s, &end, 10);
        if (end == s || v < 0) return false;
        out = v;
        return true;
    };

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "-f") { wantFollow = true; continue; }
        if (a == "-n") {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "-n requires a number\n");
                return 2;
            }
            long n = -1;
            if (!parseInt(argv[i + 1], n)) {
                std::fprintf(stderr, "invalid number for -n: %s\n", argv[i + 1]);
                return 2;
            }
            tailLines = n;
            ++i;
            continue;
        }
        if (!a.empty() && a[0] == '-' && a.size() > 1 && std::isdigit(static_cast<unsigned char>(a[1]))) {
            // Compact -N or -Nf form
            size_t pos = 1;
            while (pos < a.size() && std::isdigit(static_cast<unsigned char>(a[pos]))) pos++;
            long n = -1;
            if (!parseInt(a.substr(1, pos - 1).c_str(), n)) {
                std::fprintf(stderr, "invalid number in %s\n", a.c_str());
                return 2;
            }
            tailLines = n;
            if (pos < a.size() && a[pos] == 'f' && pos + 1 == a.size()) {
                wantFollow = true;
            } else if (pos < a.size()) {
                std::fprintf(stderr, "invalid suffix in %s (only 'f' allowed)\n", a.c_str());
                return 2;
            }
            continue;
        }
        // First non-option is the path
        path = a;
        // Remaining args ignored
        break;
    }
    if (path.empty()) {
        std::fprintf(stderr, "missing <file.zst> argument\n");
        return 2;
    }
    // Default behavior: follow-only if no -n/-N given
    followOnly = (tailLines < 0);
    if (followOnly) wantFollow = true; // follow-only implies follow loop

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
#ifdef SIGQUIT
    std::signal(SIGQUIT, on_signal);
#endif

    // Decoder stream
    ZSTD_DStream* dstream = ZSTD_createDStream();
    if (!dstream) {
        std::fprintf(stderr, "zstd: failed to create DStream\n");
        return 1;
    }
    size_t zr = ZSTD_initDStream(dstream);
    if (ZSTD_isError(zr)) {
        std::fprintf(stderr, "zstd: init error: %s\n", ZSTD_getErrorName(zr));
        ZSTD_freeDStream(dstream);
        return 1;
    }

    const size_t inChunk = ZSTD_DStreamInSize();   // recommended
    const size_t outChunk = ZSTD_DStreamOutSize(); // recommended
    std::vector<char> inBuf(inChunk * 4);          // allow some carry-over
    std::vector<char> outBuf(outChunk);

    size_t inSize = 0;      // bytes valid in inBuf
    size_t inPos  = 0;      // current read position in inBuf
    bool priming = (tailLines >= 0); // in tail-N mode, delay printing until initial EOF
    std::string lineBuf;
    std::deque<std::string> lastN;
    auto emit_line = [&](const char* data, size_t len) {
        if (tailLines >= 0 && priming) {
            lastN.emplace_back(data, len);
            while (static_cast<long>(lastN.size()) > tailLines) lastN.pop_front();
        } else {
            (void)fwrite(data, 1, len, stdout);
            fflush(stdout);
        }
    };
    auto flush_lastN = [&]() {
        for (const auto& s : lastN) {
            (void)fwrite(s.data(), 1, s.size(), stdout);
        }
        fflush(stdout);
        lastN.clear();
    };

    DevIno lastDI{};
    bool haveLast = false;
    int fd = -1;

    auto reopen = [&]() -> bool {
        if (fd != -1) { ::close(fd); fd = -1; }
        // reset decoder state when we switch files
        ZSTD_freeDStream(dstream);
        dstream = ZSTD_createDStream();
        if (!dstream) return false;
        size_t zr2 = ZSTD_initDStream(dstream);
        if (ZSTD_isError(zr2)) return false;

        // Reset input buffer
        inSize = 0;
        inPos = 0;
        // Reset tailing state for new file if we are following after priming has completed
        lineBuf.clear();
        // In follow mode keep priming=false after initial dump

        // Open (follow symlink)
        for (;;) {
            if (g_stop) return false;
            fd = open_follow(path);
            if (fd >= 0) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // record dev:ino of the resolved target
        std::string dummy;
        DevIno di{};
        if (!lstat_dev_ino(path, di, &dummy)) return false;
        lastDI = di; haveLast = true;
        return true;
    };

    // First open (or wait for rotation if follow-only)
    if (!followOnly) {
        if (!reopen()) {
            std::fprintf(stderr, "error: cannot open %s\n", path.c_str());
            ZSTD_freeDStream(dstream);
            return 1;
        }
    } else {
        // Follow-only: resolve and remember current target inode, but do not read/decode its content
        std::string dummy;
        DevIno di{};
        if (!lstat_dev_ino(path, di, &dummy)) {
            std::fprintf(stderr, "error: cannot stat %s\n", path.c_str());
            ZSTD_freeDStream(dstream);
            return 1;
        }
        lastDI = di; haveLast = true;
        // We will wait for rotation and then reopen+start decoding the new file from the beginning
    }

    // Main follow loop
    while (!g_stop) {
        // Check rotation: if symlink retargets or file replaced (inode change), reopen from beginning
        std::string resolvedNow;
        DevIno diNow{};
        if (lstat_dev_ino(path, diNow, &resolvedNow)) {
            if (haveLast && diNow != lastDI) {
                std::fprintf(stderr, "== rotation detected: %s ==\n", resolvedNow.c_str());
                if (!reopen()) break;
                priming = false; // after rotation, stream continues live
                continue; // start reading new file from beginning
            }
        }

        // If follow-only and we have not yet opened (waiting for rotation), just sleep briefly
        if (followOnly && fd == -1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        // If input buffer is consumed, try to read more bytes
        if (inPos >= inSize) {
            inPos = inSize = 0;
            // Read some more bytes (non-blocking tail-ish logic)
            // For regular files, read() blocks only until kernel has data or EOF; we poll on size increase.
            ssize_t r = ::read(fd, inBuf.data(), inBuf.size());
            if (r < 0 && errno == EINTR) continue;
            if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            if (r < 0) {
                std::perror("read");
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            if (r == 0) {
                // EOF: no new bytes *right now*. Wait briefly and retry.
                // If we were priming for tail-N and reached EOF the first time, flush the ring and switch to live mode.
                if (tailLines >= 0 && priming) {
                    flush_lastN();
                    priming = false;
                    if (!wantFollow) {
                        break; // exit after printing last N lines (no -f)
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(80));
                continue;
            }
            inSize = static_cast<size_t>(r);
        }

        // Decompress whatever we have; if we run out mid-frame, we’ll loop and read more.
        ZSTD_inBuffer zin{ inBuf.data(), inSize, inPos };
        for (;;) {
            ZSTD_outBuffer zout{ outBuf.data(), outBuf.size(), 0 };
            size_t ret = ZSTD_decompressStream(dstream, &zout, &zin);
            if (ZSTD_isError(ret)) {
                // Likely we hit the middle of an incomplete frame or corrupted input.
                // Strategy: if we have no more input, wait for more; otherwise, treat as fatal corruption.
                std::fprintf(stderr, "zstd decode error: %s\n", ZSTD_getErrorName(ret));
                // Try to resync by waiting for more data.
                break;
            }

            // Assemble lines from decompressed bytes
            if (zout.pos > 0) {
                size_t start = 0;
                const char* data = outBuf.data();
                for (size_t i = 0; i < zout.pos; ++i) {
                    if (data[i] == '\n') {
                        // complete line = (lineBuf if any) + data[start..i] + '\n'
                        if (!lineBuf.empty()) {
                            std::string combined;
                            combined.reserve(lineBuf.size() + (i - start + 1));
                            combined.append(lineBuf);
                            combined.append(data + start, i - start + 1);
                            emit_line(combined.data(), combined.size());
                            lineBuf.clear();
                        } else {
                            emit_line(data + start, i - start + 1);
                        }
                        start = i + 1;
                    }
                }
                // remainder (partial line)
                if (start < zout.pos) {
                    lineBuf.append(data + start, zout.pos - start);
                }
            }

            inPos = zin.pos;

            if (ret == 0) {
                // ret==0 means a frame ended and the decoder is ready for the next frame.
                // Keep looping: there might be another frame already buffered (zstd file concatenation).
                if (zin.pos >= zin.size) break; // need more input to proceed
                // else: continue with next frame in current buffer
            } else {
                // ret > 0 means the number of bytes expected to end the current frame (upper bound).
                // If we still have input, keep feeding; if not, break to read more.
                if (zin.pos >= zin.size) break; // need more data
            }
        }

        // If buffer fully consumed, next loop iteration will read more.
    }

    // On exit: if we were priming and have a partial line buffered, treat it as a line
    if (tailLines >= 0 && !lineBuf.empty()) {
        (void)fwrite(lineBuf.data(), 1, lineBuf.size(), stdout);
        fflush(stdout);
    }

    if (fd != -1) ::close(fd);
    ZSTD_freeDStream(dstream);
    return 0;
}
