#include <iostream>
#include <fmt/format.h>
#include <llfio.hpp>

namespace llfio = LLFIO_V2_NAMESPACE;


int main() { fmt::print("Hello world\n");



  llfio::file_handle fh = llfio::file(  //
    {},       // path_handle to base directory
    "redo"    // path_view to path fragment relative to base directory
              // default mode is read only
              // default creation is open existing
              // default caching is all
              // default flags is none
  ).value();  // If failed, throw a filesystem_error exception
}
