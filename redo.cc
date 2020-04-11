#include <array>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <string_view>
#include <utility>
#include <vector>

#include <cassert>

#include <fmt/color.h>
#include <fmt/format.h>
#include <llfio.hpp>

#include "redo/jthread.hpp"
#include "redo/span.hpp"
#include "redo/stop_token.hpp"

namespace {

namespace llfio = LLFIO_V2_NAMESPACE;
using llfio::io_handle;

static const llfio::file_handle::extent_type kFileSize = 128 * 1024 * 1024;
static const char kFileName[] = "circular_file";
static const size_t kThreads = 64;
static const auto kDuration = std::chrono::seconds(20);

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
                .value())
#ifndef __linux__
        ,
        size_(size)
#endif
  {
    llfio::truncate(fh_, size).value();

    alignas(4096) std::array<std::byte, 1024 * 1024> buf;
    buf.fill(std::byte{0});

    std::vector<llfio::io_handle::const_buffer_type> v(
        size / buf.size(), {buf.data(), buf.size()});
    ::WriteAll(fh_, {v, 0});
  }

  ~ScopedFile() { llfio::unlink(fh_).value(); }

  void WriteAll(llfio::file_handle::extent_type offset,
                io_handle::const_buffer_type buffer) {
    ::WriteAll(fh_, offset, buffer);
  }

  void Flush() {
#ifdef __linux__
    int ret = fsync(fh_.native_handle().fd);
    (void)ret;
    assert(ret == 0);
#else
    std::array<std::byte, 1> buf;
    io_handle::const_buffer_type buf2(buf.data(), size_);
    fh_.barrier(io_handle::io_request<io_handle::const_buffers_type>(
                    io_handle::const_buffers_type(&buf2, 1), 0),
                io_handle::barrier_kind::wait_data_only);
#endif
  }

private:
  llfio::file_handle fh_;
#ifndef __linux__
  llfio::file_handle::extent_type size_;
#endif
};

class ScopedMappedFile {
public:
  ScopedMappedFile(const char *path, llfio::file_handle::extent_type size,
                   llfio::mapped_file_handle::caching caching)
      : fh_{llfio::mapped_file({}, path, llfio::mapped_file_handle::mode::write,
                               llfio::mapped_file_handle::creation::always_new,
                               caching)
                .value()},
        size_{size} {
    fh_.truncate(size).value();

    alignas(4096) std::array<std::byte, 1024 * 1024> buf;
    buf.fill(std::byte{0});

    std::vector<llfio::io_handle::const_buffer_type> v(
        size / buf.size(), {buf.data(), buf.size()});
    ::WriteAll(fh_, {v, 0});
  }

  ~ScopedMappedFile() { llfio::unlink(fh_).value(); }

  void WriteAll(llfio::file_handle::extent_type offset,
                io_handle::const_buffer_type buffer) {
    ::WriteAll(fh_, offset, buffer);
  }

  void Flush() {
    int ret = msync(fh_.address(), size_, MS_SYNC);
    (void)ret;
    assert(ret == 0);
  }

private:
  llfio::mapped_file_handle fh_;
  llfio::file_handle::extent_type size_;
};

template <class FILE> class CircularFile {
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
  FILE file_;
  const llfio::file_handle::extent_type size_;
  llfio::io_handle::extent_type offset_{0};
};

class AlignedBuffer {
public:
  AlignedBuffer(size_t capacity, size_t alignment)
      : capacity_{capacity}, alignment_{alignment},
        buffer_{
            static_cast<std::byte *>(std::aligned_alloc(capacity, alignment)),
            std::free} {
    assert(capacity % alignment == 0);
  }

  AlignedBuffer(const AlignedBuffer &) = delete;
  AlignedBuffer &operator=(const AlignedBuffer &) = delete;

  AlignedBuffer(AlignedBuffer &&rhs)
      : alignment_(rhs.alignment_), capacity_(rhs.capacity_),
        buffer_{std::move(rhs.buffer_)}, size_{rhs.size_} {}

  AlignedBuffer &operator=(AlignedBuffer &&rhs) {
    alignment_ = rhs.alignment_;
    capacity_ = rhs.capacity_;
    buffer_ = std::move(rhs.buffer_);
    size_ = rhs.size_;
    return *this;
  }

  void Append(tcb::span<const std::byte> buf) {
    assert(Size() + buf.size() < capacity_);
    std::copy(buf.begin(), buf.end(), &buffer_[size_]);
    size_ += buf.size();
  }

  void Clear() { size_ = 0; }

  const std::byte *Data() const { return &buffer_[0]; }
  size_t Size() const { return size_; }
  size_t Capacity() const { return capacity_; }

private:
  size_t alignment_;
  size_t capacity_;
  std::unique_ptr<std::byte[], decltype(&std::free)> buffer_;
  size_t size_{0};
};

class Redo {
public:
  virtual ~Redo() {}

  virtual std::string_view Name() = 0;

  virtual size_t Append(tcb::span<std::byte> buffer) = 0;

  virtual void Commit(size_t lsn) = 0;

  virtual size_t CommitsHandled() const = 0;
};

class RedoSync final : public Redo {
public:
  std::string_view Name() final { return "RedoSync"; };

  size_t Append(tcb::span<std::byte> buffer) final {
    std::lock_guard<std::mutex> _(mutex_);
    file_.Append({buffer.data(), buffer.size()});
    return committed_lsn_.fetch_add(1, std::memory_order_relaxed) + 1;
  }

  void Commit(size_t) final {}

  size_t CommitsHandled() const final {
    return committed_lsn_.load(std::memory_order_relaxed);
  }

private:
  CircularFile<ScopedFile> file_{kFileName, kFileSize,
                                 llfio::handle::caching::reads};
  std::atomic<size_t> committed_lsn_{0};
  std::mutex mutex_;
};

class RedoSyncBuffer final : public Redo {
public:
  std::string_view Name() final { return "RedoSyncBuffer"; };

  size_t Append(tcb::span<std::byte> buffer) final {
    std::lock_guard<std::mutex> _(mutex_);

    if (buffer_.Size() + buffer.size() > buffer_.Capacity()) {
      AppendBufferToFile();
      committed_lsn_.store(lsn_);
    }

    buffer_.Append(buffer);

    return ++lsn_;
  }

  void Commit(size_t lsn) final {
    if (lsn <= committed_lsn_.load(std::memory_order_relaxed))
      return;

    std::lock_guard<std::mutex> _(mutex_);

    if (lsn <= committed_lsn_.load(std::memory_order_relaxed))
      return;

    AppendBufferToFile();
    committed_lsn_.store(lsn_);
  }

  size_t CommitsHandled() const final {
    return committed_lsn_.load(std::memory_order_relaxed);
  }

private:
  static const size_t kBufferSize = 10 * 1024 * 1024;
  static const size_t kAlignment = 4096;

  void AppendBufferToFile() {
    file_.Append({buffer_.Data(), buffer_.Size()});
    buffer_.Clear();
  }

  std::mutex mutex_;
  std::atomic<size_t> lsn_{0};
  std::atomic<size_t> committed_lsn_{0};
  AlignedBuffer buffer_{kBufferSize, kAlignment};
  CircularFile<ScopedFile> file_{kFileName, kFileSize,
                                 llfio::handle::caching::reads};
};

class RedoOverlappedFsync final : public Redo {
public:
  std::string_view Name() final { return "RedoOverlappedFsync"; };

  size_t Append(tcb::span<std::byte> buffer) final {
    std::lock_guard<std::mutex> _(mutex_);
    file_.Append({buffer.data(), buffer.size()});
    return ++lsn_;
  }

  void Commit(size_t lsn) final {
    if (lsn <= committed_lsn_.load(std::memory_order_relaxed))
      return;

    file_.Flush();

    for (;;) {
      auto stored_lsn = committed_lsn_.load(std::memory_order_relaxed);

      if (lsn <= stored_lsn)
        break;

      committed_lsn_.compare_exchange_weak(stored_lsn, lsn);
    }
  }

  size_t CommitsHandled() const final {
    return committed_lsn_.load(std::memory_order_relaxed);
  }

private:
  CircularFile<ScopedFile> file_{kFileName, kFileSize,
                                 llfio::handle::caching::all};
  size_t lsn_{0};
  std::atomic<size_t> committed_lsn_{0};
  std::mutex mutex_;
};

class RedoOverlappedMsync final : public Redo {
public:
  RedoOverlappedMsync() { file_.Flush(); }

  std::string_view Name() final { return "RedoOverlappedMsync"; };

  size_t Append(tcb::span<std::byte> buffer) final {
    std::lock_guard<std::mutex> _(mutex_);
    file_.Append({buffer.data(), buffer.size()});
    return ++lsn_;
  }

  void Commit(size_t lsn) final {
    if (lsn <= committed_lsn_.load(std::memory_order_relaxed))
      return;

    file_.Flush();

    for (;;) {
      auto stored_lsn = committed_lsn_.load(std::memory_order_relaxed);

      if (lsn <= stored_lsn)
        break;

      committed_lsn_.compare_exchange_weak(stored_lsn, lsn);
    }
  }

  size_t CommitsHandled() const final {
    return committed_lsn_.load(std::memory_order_relaxed);
  }

private:
  CircularFile<ScopedMappedFile> file_{kFileName, kFileSize,
                                       llfio::handle::caching::all};
  size_t lsn_{0};
  std::atomic<size_t> committed_lsn_{0};
  std::mutex mutex_;
};

class RedoGroupCommit final : public Redo {
public:
  std::string_view Name() final { return "RedoGroupCommit"; };

  size_t Append(tcb::span<std::byte> buffer) final {
    std::lock_guard<std::mutex> _(append_mutex_);
    file_.Append({buffer.data(), buffer.size()});
    return ++lsn_;
  }

  void Commit(size_t lsn) final {
    if (lsn < committed_lsn_.load(std::memory_order_relaxed))
      return;

    std::lock_guard<std::mutex> _(flush_mutex_);

    if (lsn < committed_lsn_.load(std::memory_order_relaxed))
      return;

    file_.Flush();
    committed_lsn_.store(lsn, std::memory_order_relaxed);
  }

  size_t CommitsHandled() const final {
    return committed_lsn_.load(std::memory_order_relaxed);
  }

private:
  std::mutex append_mutex_;
  size_t lsn_{0};

  std::mutex flush_mutex_;
  std::atomic<size_t> committed_lsn_{0};

  CircularFile<ScopedFile> file_{kFileName, kFileSize,
                                 llfio::handle::caching::all};
};

class RedoODirectSparse final : public Redo {
public:
  RedoODirectSparse() { zeroes_.fill(std::byte{0}); }

  std::string_view Name() final { return "RedoODirectSparse"; };

  size_t Append(tcb::span<std::byte> buffer) final {
    std::lock_guard<std::mutex> _(mutex_);

    buffer_.Append(buffer);
    auto tail_size = kAlignment - buffer.size() % zeroes_.size();
    buffer_.Append({zeroes_.data(), tail_size});
    file_.Append({buffer_.Data(), buffer_.Size()});
    buffer_.Clear();

    return ++committed_lsn_;
  }

  void Commit(size_t lsn) final {}

  size_t CommitsHandled() const final {
    return committed_lsn_.load(std::memory_order_relaxed);
  }

private:
  static const size_t kBufferSize = 1 * 1024 * 1024;
  static const size_t kAlignment = 4096;

  std::mutex mutex_;
  std::atomic<size_t> committed_lsn_{0};
  AlignedBuffer buffer_{kBufferSize, kAlignment};
  std::array<std::byte, kAlignment> zeroes_;
  CircularFile<ScopedFile> file_{kFileName, kFileSize,
                                 llfio::handle::caching::none};
};

class RedoODirectBuffer final : public Redo {
public:
  RedoODirectBuffer() { zeroes_.fill(std::byte{0}); }

  std::string_view Name() final { return "RedoODirectBuffer"; };

  size_t Append(tcb::span<std::byte> buffer) final {
    std::lock_guard<std::mutex> _(mutex_);

    if (buffer_.Size() + buffer.size() > buffer_.Capacity()) {
      AppendBufferToFile();
      committed_lsn_.store(lsn_);
    }

    buffer_.Append(buffer);

    return ++lsn_;
  }

  void Commit(size_t lsn) final {
    if (lsn <= committed_lsn_.load(std::memory_order_relaxed))
      return;

    std::lock_guard<std::mutex> _(mutex_);

    if (lsn <= committed_lsn_.load(std::memory_order_relaxed))
      return;

    AppendBufferToFile();
    committed_lsn_.store(lsn_);
  }

  size_t CommitsHandled() const final {
    return committed_lsn_.load(std::memory_order_relaxed);
  }

private:
  static const size_t kBufferSize = 10 * 1024 * 1024;
  static const size_t kAlignment = 4096;

  void AppendBufferToFile() {
    size_t tail_size = kAlignment - buffer_.Size() % kAlignment;
    buffer_.Append({zeroes_.data(), tail_size});
    file_.Append({buffer_.Data(), buffer_.Size()});
    buffer_.Clear();
  }

  std::mutex mutex_;
  std::atomic<size_t> lsn_{0};
  std::atomic<size_t> committed_lsn_{0};
  AlignedBuffer buffer_{kBufferSize, kAlignment};
  std::array<std::byte, kAlignment> zeroes_;
  CircularFile<ScopedFile> file_{kFileName, kFileSize,
                                 llfio::handle::caching::none};
};

class RedoODirectTwoBuffers final : public Redo {
public:
  RedoODirectTwoBuffers() { zeroes_.fill(std::byte{0}); }

  std::string_view Name() final { return "RedoODirectTwoBuffers"; };

  size_t Append(tcb::span<std::byte> buffer) final {
    std::unique_lock<std::mutex> flush_lock(flush_mutex_);
    std::lock_guard<std::mutex> _(append_mutex_);

    if (buffer_.Size() + buffer.size() > buffer_.Capacity()) {
      AppendBufferToFile(buffer_);
      committed_lsn_.store(lsn_);
    }
    flush_lock.unlock();

    buffer_.Append(buffer);

    return ++lsn_;
  }

  void Commit(size_t lsn) final {
    if (lsn <= committed_lsn_.load(std::memory_order_relaxed))
      return;

    std::lock_guard<std::mutex> _(flush_mutex_);

    if (lsn <= committed_lsn_.load(std::memory_order_relaxed))
      return;

    size_t flushing_up_to;
    {
      std::lock_guard<std::mutex> _(append_mutex_);
      std::swap(buffer_, other_buffer_);
      flushing_up_to = lsn_;
    }

    AppendBufferToFile(other_buffer_);
    committed_lsn_.store(flushing_up_to, std::memory_order_relaxed);
  }

  size_t CommitsHandled() const final {
    return committed_lsn_.load(std::memory_order_relaxed);
  }

private:
  static const size_t kBufferSize = 10 * 1024 * 1024;
  static const size_t kAlignment = 4096;

  void AppendBufferToFile(AlignedBuffer &buffer) {
    size_t tail_size = kAlignment - buffer.Size() % kAlignment;
    buffer.Append({zeroes_.data(), tail_size});
    file_.Append({buffer.Data(), buffer.Size()});
    buffer.Clear();
  }

  std::mutex append_mutex_;
  size_t lsn_{0};
  AlignedBuffer buffer_{kBufferSize, kAlignment};
  AlignedBuffer other_buffer_{kBufferSize, kAlignment};

  std::mutex flush_mutex_;
  std::atomic<size_t> committed_lsn_{0};
  std::array<std::byte, kAlignment> zeroes_;
  CircularFile<ScopedFile> file_{kFileName, kFileSize,
                                 llfio::handle::caching::none};
};

void ThreadFunction(std::stop_token st, std::byte b, Redo &redo) {
  std::array<std::byte, 2000> buffer;
  buffer.fill(b);

  std::array<tcb::span<std::byte>, 3> buffers{
      tcb::span{buffer.data(), 20}, tcb::span{buffer.data(), 200}, buffer};

  std::random_device r;
  std::default_random_engine e1(r());
  std::uniform_int_distribution<size_t> uniform_dist(0, 2);

  // One iteration is one use transaction.
  while (!st.stop_requested()) {
    size_t how_much_appends = uniform_dist(e1);

    // User transaction consists of several MTR.
    size_t lsn = 0;
    for (size_t i = 0; i < how_much_appends; ++i) {
      auto span = buffers[uniform_dist(e1)];
      lsn = redo.Append(span);
    }

    redo.Commit(lsn);
  }
}

template <class REDO> void Test() {
  using namespace std::chrono_literals;

  REDO redo;

  std::vector<std::jthread> threads;
  for (int i = 0; i < kThreads; i++) {
    threads.emplace_back(ThreadFunction, static_cast<std::byte>(i + 1),
                         std::ref(redo));
  }

  std::this_thread::sleep_for(kDuration);
  for (auto &t : threads)
    t.request_stop();
  for (auto &t : threads)
    t.join();

  fmt::print("{} handled", redo.Name());
  fmt::print(fg(fmt::color::white), " {} ", redo.CommitsHandled());
  fmt::print("commits\n", redo.CommitsHandled());
}

} // namespace

int main() {
  fmt::print("File size: {}, threads: {}, duration: {}s\n", kFileSize, kThreads,
             kDuration.count());
  fmt::print("\n");

  Test<RedoSync>();
  Test<RedoSyncBuffer>();
  Test<RedoODirectSparse>();
  Test<RedoODirectBuffer>();
  Test<RedoODirectTwoBuffers>();
  Test<RedoOverlappedFsync>();
  Test<RedoOverlappedMsync>();
  Test<RedoGroupCommit>();
}
