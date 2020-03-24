#include <array>
#include <chrono>

#include <fmt/format.h>
#include <llfio.hpp>

#include "redo/stop_token.hpp"
#include "redo/jthread.hpp"

namespace llfio = LLFIO_V2_NAMESPACE;

static const size_t kFileSize = 128 * 1024 * 1024;
static const char kFileName[] = "circular_file";

int main() {
  using namespace std::chrono_literals;

  fmt::print("Hello world\n");

  llfio::file_handle fh = llfio::file({}, kFileName, llfio::handle::mode::write,
                                      llfio::handle::creation::always_new)
                              .value();

  llfio::truncate(fh, kFileSize).value();

  std::array<std::byte, 1024 * 1024> buf;
  buf.fill(std::byte{0});

  for (size_t i = 0; i < kFileSize; i += buf.size())
    llfio::write(fh, i, {{buf.data(), buf.size()}}).value();

  auto func = [&fh](std::stop_token stop_token, std::byte b) {
    std::array<std::byte, 1024 * 1024> buf;
    buf.fill(std::byte{b});

    while (!stop_token.stop_requested()) {
      fmt::print("Writing...\n");
      llfio::write(fh, 0, {{buf.data(), buf.size()}}).value();
    }
  };

  std::jthread t(func, std::byte{1});

  std::this_thread::sleep_for(1ms);
}
