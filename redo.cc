#include <array>
#include <chrono>
#include <mutex>
#include <random>
#include <vector>

#include <cassert>

#include <fmt/format.h>
#include <llfio.hpp>

#include "redo/jthread.hpp"
#include "redo/span.hpp"
#include "redo/stop_token.hpp"

namespace llfio = LLFIO_V2_NAMESPACE;

static const llfio::file_handle::extent_type kFileSize = 128 * 1024 * 1024;
static const char kFileName[] = "circular_file";

class CircularFile {
public:
  CircularFile(const char *file_name, llfio::file_handle::extent_type size)
      : fh_(llfio::file({}, file_name, llfio::handle::mode::write,
                        llfio::handle::creation::always_new,
                        llfio::handle::caching::reads_and_metadata)
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

    appends_performed_++;
  }

  auto AppendsPerformed() const {
    std::lock_guard<std::mutex> _(mutex_);
    return appends_performed_;
  }

private:
  llfio::file_handle fh_;
  const llfio::file_handle::extent_type size_;
  llfio::io_handle::extent_type offset_{0};
  mutable std::mutex mutex_;
  size_t appends_performed_{0};
};

void ThreadFunction(std::stop_token st, std::byte b, CircularFile &f) {
  std::array<std::byte, 20> small;
  small.fill(b);
  std::array<std::byte, 200> medium;
  medium.fill(b);
  std::array<std::byte, 2000> big;
  big.fill(b);

  std::array<tcb::span<std::byte>, 3> buffers{small, medium, big};

  std::random_device r;
  std::default_random_engine e1(r());
  std::uniform_int_distribution<size_t> uniform_dist(0, 2);

  while (!st.stop_requested()) {
    auto span = buffers[uniform_dist(e1)];
    fmt::print("Writing {} bytes of {}\n", span.size(), span[0]);
    f.Append({span.data(), span.size()});
  }
}

int main() {
  using namespace std::chrono_literals;

  fmt::print("Hello world\n");

  CircularFile cf(kFileName, kFileSize);

  std::vector<std::jthread> threads;
  for (int i = 0; i < 3; i++) {
    threads.emplace_back(ThreadFunction, static_cast<std::byte>(i + 1),
                         std::ref(cf));
  }

  std::this_thread::sleep_for(30s);
  for (auto &t : threads)
    t.request_stop();
  for (auto &t : threads)
    t.join();

  fmt::print("Appends performed: {}\n", cf.AppendsPerformed());
}
