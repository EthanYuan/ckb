#include <stdint.h>
#include <string.h>

#include "ckb_syscalls.h"

int main() {
  uint64_t success = ckb_spawn(8, 1, 3, 0, 0, NULL, NULL, NULL, NULL);
  if (success == 0) {
    return 1;
  }
  return 0;
}
