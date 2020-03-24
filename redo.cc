#include <array>
#include <chrono>
#include <vector>

#include <fmt/format.h>
#include <llfio.hpp>

#include "redo/jthread.hpp"
#include "redo/stop_token.hpp"

namespace llfio = LLFIO_V2_NAMESPACE;

static const llfio::file_handle::extent_type kFileSize = 128 * 1024 * 1024;
static const char kFileName[] = "circular_file";

class CircularFile {
public:
  CircularFile(const char *file_name, llfio::file_handle::extent_type size)
      : fh_(llfio::file({}, file_name, llfio::handle::mode::write,
                        llfio::handle::creation::always_new)
                .value()),
        size_(size) {
    llfio::truncate(fh_, kFileSize).value();

    std::array<std::byte, 1024 * 1024> buf;
    buf.fill(std::byte{0});

    for (size_t i = 0; i < kFileSize; i += buf.size())
      llfio::write(fh_, i, {{buf.data(), buf.size()}}).value();
  }

  ~CircularFile() noexcept { llfio::unlink(fh_).value(); }

  void Append(llfio::io_handle::const_buffer_type buf) {
    std::lock_guard<std::mutex> _(mutex_);

    if (offset_ + buf.size() > size_) {
      auto partial_size = size_ - offset_;
      llfio::write(fh_, offset_, {{buf.data(), partial_size}}).value();
      buf = llfio::io_handle::const_buffer_type(buf.data() + partial_size,
                                                buf.size() - partial_size);
      offset_ = 0;
    }

    llfio::write(fh_, offset_, {buf}).value();
    offset_ = (offset_ + buf.size()) % size_;
  }

private:
  llfio::file_handle fh_;
  const llfio::file_handle::extent_type size_;
  llfio::io_handle::extent_type offset_{0};
  std::mutex mutex_;
};

int main() {
  using namespace std::chrono_literals;

  fmt::print("Hello world\n");

  CircularFile cf(kFileName, kFileSize);

  auto func = [&cf](std::stop_token stop_token, std::byte b) {
    std::array<std::byte, 1024 * 1024> buf;
    buf.fill(std::byte{b});

    while (!stop_token.stop_requested()) {
      fmt::print("Writing {}...\n", b);
      cf.Append({buf.data(), buf.size()});
    }
  };

  std::vector<std::jthread> threads;

  for (int i = 0; i < 3; i++) {
    threads.emplace_back(func, static_cast<std::byte>(i + 1));
  }

  std::this_thread::sleep_for(5ms);
}
