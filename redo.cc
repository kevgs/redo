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
using llfio::io_handle;

static const llfio::file_handle::extent_type kFileSize = 128 * 1024 * 1024;
static const char kFileName[] = "circular_file";

void WriteAll(
    io_handle &fd,
    io_handle::io_request<io_handle::const_buffers_type> reqs) noexcept {
  const static size_t kMaxBuffers = 128;

  assert(reqs.buffers.size() <= kMaxBuffers);

  do {
    auto written_buffers = llfio::write(fd, reqs).value();

    reqs.offset += std::accumulate(
        written_buffers.begin(), written_buffers.end(), static_cast<size_t>(0),
        [](auto a, auto b) { return a + b.size(); });

    auto [_, it] = std::mismatch(
        reqs.buffers.begin(), reqs.buffers.end(), written_buffers.begin(),
        [](auto a, auto b) { return a.size() == b.size(); });

    // forget fully written buffers
    reqs.buffers =
        reqs.buffers.subspan(std::distance(written_buffers.begin(), it));

    // shrink partially written buffer
    if (it != written_buffers.end()) {
      assert(std::next(it) == written_buffers.end());

      reqs.buffers.front() = {reqs.buffers.front().data() + it->size(),
                              reqs.buffers.front().size() - it->size()};
    }

  } while (!reqs.buffers.empty());
}

void WriteAll(
    io_handle &fd, llfio::file_handle::extent_type offset,
    std::initializer_list<io_handle::const_buffer_type> lst) noexcept {
  const static size_t kMaxBuffers = 64;

  assert(std::distance(lst.begin(), lst.end()) <= kMaxBuffers);

  std::array<io_handle::const_buffer_type, kMaxBuffers> buffer;
  std::copy(lst.begin(), lst.end(), buffer.begin());

  io_handle::const_buffers_type write_me{
      buffer.data(),
      static_cast<size_t>(std::distance(lst.begin(), lst.end()))};

  WriteAll(fd, {write_me, offset});
}

void WriteAll(io_handle &fd, llfio::file_handle::extent_type offset,
              io_handle::const_buffer_type buffer) noexcept {
  io_handle::const_buffers_type write_me{&buffer, 1};

  do {
    io_handle::io_request<io_handle::const_buffers_type> reqs{write_me, offset};
    auto written_buffer = llfio::write(fd, reqs).value().front();

    offset += written_buffer.size();

    // shrink partially written buffer
    buffer = {buffer.data() + written_buffer.size(),
              buffer.size() - written_buffer.size()};

  } while (buffer.size() != 0);
}

class ScopedFile {
public:
  ScopedFile(const char *file_name, llfio::file_handle::extent_type size,
             llfio::file_handle::caching caching)
      : fh_(llfio::file({}, file_name, llfio::handle::mode::write,
                        llfio::handle::creation::always_new, caching)
                .value()) {
    llfio::truncate(fh_, size).value();

    std::array<std::byte, 1024 * 1024> buf;
    buf.fill(std::byte{0});

    auto time = std::chrono::steady_clock::now();

    std::vector<llfio::io_handle::const_buffer_type> v(
        size / buf.size(), {buf.data(), buf.size()});
    ::WriteAll(fh_, {v, 0});

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - time);
    fmt::print("Filling {} bytes with zeroes took {}ms\n", size,
               duration.count());
  }

  ~ScopedFile() { llfio::unlink(fh_).value(); }

  void WriteAll(llfio::file_handle::extent_type offset,
                io_handle::const_buffer_type buffer) {
    ::WriteAll(fh_, offset, buffer);
  }

  void Flush() { fh_.barrier().value(); }

private:
  llfio::file_handle fh_;
};

class CircularFile {
public:
  CircularFile(const char *file_name, llfio::file_handle::extent_type size,
               llfio::handle::caching caching)
      : file_(file_name, size, caching), size_(size) {}

  void Append(llfio::io_handle::const_buffer_type buf) {
    if (offset_ + buf.size() > size_) {
      auto partial_size = size_ - offset_;
      file_.WriteAll(offset_, {buf.data(), partial_size});
      buf = llfio::io_handle::const_buffer_type(buf.data() + partial_size,
                                                buf.size() - partial_size);
      offset_ = 0;
    }

    file_.WriteAll(offset_, {buf});
    offset_ = (offset_ + buf.size()) % size_;
  }

  void Flush() { file_.Flush(); }

private:
  ScopedFile file_;
  const llfio::file_handle::extent_type size_;
  llfio::io_handle::extent_type offset_{0};
};

class Redo {
public:
  virtual ~Redo() {}

  void AppendDurable(tcb::span<std::byte> buffer) {
    AppendDurableImpl(buffer);
    appends_handled_.fetch_add(1, std::memory_order_relaxed);
  }

  size_t AppendsHandled() {
    return appends_handled_.load(std::memory_order_relaxed);
  }

protected:
  virtual void AppendDurableImpl(tcb::span<std::byte> buffer) = 0;

private:
  std::atomic<size_t> appends_handled_{0};
};

class RedoSimplest final : public Redo {
private:
  void AppendDurableImpl(tcb::span<std::byte> buffer) override {
    std::lock_guard<std::mutex> _(mutex_);
    file_.Append({buffer.data(), buffer.size()});
  }

  CircularFile file_{kFileName, kFileSize,
                     llfio::handle::caching::reads_and_metadata};
  std::mutex mutex_;
};

void ThreadFunction(std::stop_token st, std::byte b, Redo &redo) {
  std::array<std::byte, 2000> buffer;
  buffer.fill(b);

  std::array<tcb::span<std::byte>, 3> buffers{
      tcb::span{buffer.data(), 20}, tcb::span{buffer.data(), 200}, buffer};

  std::random_device r;
  std::default_random_engine e1(r());
  std::uniform_int_distribution<size_t> uniform_dist(0, 2);

  while (!st.stop_requested()) {
    auto span = buffers[uniform_dist(e1)];
    fmt::print("Writing {} bytes of {}\n", span.size(), span[0]);
    redo.AppendDurable(span);
  }
}

int main() {
  using namespace std::chrono_literals;

  fmt::print("Hello world\n");

  RedoSimplest redo;

  std::vector<std::jthread> threads;
  for (int i = 0; i < 3; i++) {
    threads.emplace_back(ThreadFunction, static_cast<std::byte>(i + 1),
                         std::ref(redo));
  }

  std::this_thread::sleep_for(30s);
  for (auto &t : threads)
    t.request_stop();
  for (auto &t : threads)
    t.join();

  fmt::print("Appends performed: {}\n", redo.AppendsHandled());
}
