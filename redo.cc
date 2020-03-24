#include <array>

#include <fmt/format.h>
#include <llfio.hpp>

namespace llfio = LLFIO_V2_NAMESPACE;

static const size_t kFileSize = 128 * 1024 * 1024;
static const char kFileName[] = "circular_file";

int main() {
  fmt::print("Hello world\n");

  llfio::file_handle fh = llfio::file({}, kFileName, llfio::handle::mode::write,
                                      llfio::handle::creation::always_new)
                              .value();

  llfio::truncate(fh, kFileSize).value();

  std::array<std::byte, 1024 * 1024> buf;
  buf.fill(std::byte{1});

  for (size_t i = 0; i < kFileSize; i += buf.size())
    llfio::write(fh, i, {{buf.data(), buf.size()}}).value();
}
