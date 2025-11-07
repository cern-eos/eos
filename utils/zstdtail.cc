// zstdtail.cpp — "tail -f" semantics for .zst files (frame-aware)
// Build: g++ -std=c++17 -O2 zstdtail.cpp -o zstdtail -lzstd
//
// Usage:
//   zstdtail /path/to/file-or-symlink.zst
//   zstdtail /path/to/file-or-symlink.zst | jq '.'
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

size_t safe_read(int fd, void* buf, size_t n) {
    for (;;) {
        ssize_t r = ::read(fd, buf, n);
        if (r > 0) return static_cast<size_t>(r);
        if (r == 0) return 0; // EOF
        if (errno == EINTR) continue;
        if (errno == EAGAIN) { std::this_thread::sleep_for(std::chrono::milliseconds(5)); continue; }
        return static_cast<size_t>(-1);
    }
}

bool is_regular_open(int fd) {
    struct stat st{};
    if (fstat(fd, &st) != 0) return false;
    return S_ISREG(st.st_mode);
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <file-or-symlink.zst>\n", argv[0]);
        return 2;
    }
    std::string path = argv[1];

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

    // First open
    if (!reopen()) {
        std::fprintf(stderr, "error: cannot open %s\n", path.c_str());
        ZSTD_freeDStream(dstream);
        return 1;
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
                continue; // start reading new file from beginning
            }
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

            // Write decompressed bytes to stdout
            if (zout.pos > 0) {
                size_t wrote = fwrite(outBuf.data(), 1, zout.pos, stdout);
                (void)wrote;
                fflush(stdout);
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

    if (fd != -1) ::close(fd);
    ZSTD_freeDStream(dstream);
    return 0;
}
