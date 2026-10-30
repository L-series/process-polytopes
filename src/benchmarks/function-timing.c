/* Instrumented timing only; never link this into the native instruction build. */
#define _POSIX_C_SOURCE 200809L
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#define CAPACITY 8192
#define DEPTH 4096
struct Stat {
  void *address;
  uint64_t calls, self_ns, inclusive_ns;
};
struct Frame {
  struct Stat *stat;
  uint64_t start, children;
};
static struct Stat stats[CAPACITY];
static struct Frame stack[DEPTH];
static size_t depth;
static uint64_t timestamp(void) {
  struct timespec t;
  if (clock_gettime(CLOCK_MONOTONIC, &t))
    abort();
  return (uint64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}
static struct Stat *lookup(void *address) {
  size_t i = ((uintptr_t)address >> 4) % CAPACITY;
  for (size_t n = 0; n < CAPACITY; n++, i = (i + 1) % CAPACITY) {
    if (!stats[i].address)
      stats[i].address = address;
    if (stats[i].address == address)
      return &stats[i];
  }
  abort();
}
void __cyg_profile_func_enter(void *address, void *caller) {
  (void)caller;
  uint64_t t = timestamp();
  if (depth == DEPTH)
    abort();
  struct Stat *stat = lookup(address);
  stat->calls++;
  stack[depth++] = (struct Frame){stat, t, 0};
}
void __cyg_profile_func_exit(void *address, void *caller) {
  (void)caller;
  uint64_t t = timestamp();
  if (!depth || stack[depth - 1].stat->address != address)
    abort();
  struct Frame frame = stack[--depth];
  uint64_t elapsed = t - frame.start;
  if (elapsed < frame.children)
    abort();
  frame.stat->inclusive_ns += elapsed;
  frame.stat->self_ns += elapsed - frame.children;
  if (depth)
    stack[depth - 1].children += elapsed;
}
static void write_stats(void) {
  const char *path = getenv("CWS_FUNCTION_TIMING");
  if (!path || depth)
    abort();
  FILE *f = fopen(path, "w");
  if (!f)
    abort();
  for (size_t i = 0; i < CAPACITY; i++)
    if (stats[i].address)
      fprintf(f, "%p %llu %llu %llu\n", stats[i].address, (unsigned long long)stats[i].calls,
              (unsigned long long)stats[i].self_ns, (unsigned long long)stats[i].inclusive_ns);
  if (fclose(f))
    abort();
}
__attribute__((constructor)) static void setup(void) {
  if (atexit(write_stats))
    abort();
}
