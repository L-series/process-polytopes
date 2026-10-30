#include "weight_pool.h"
#include "Global.h"
#include "Rat.h"
#include <ctype.h>
#include <errno.h>
#include <limits.h>

void ProjectCWSError(const char *message) {
  fprintf(stderr, "dim-5 CWS: %s\n", message);
  exit(EXIT_FAILURE);
}

/* Read minimal weight-only files and feed existing PALP selection routines.
   Pools may be bounded for tests; require nonempty, sorted, primitive rows. */
FILE *ProjectReadWeightPool(const char *directory, int size) {
  char path[4096], line[512];
  FILE *source, *pool = tmpfile();
  int nonempty = 0;
  if (!pool)
    ProjectCWSError("cannot create weight pool");
  if (size == 2) {
    fputs("2 1 1\n", pool);
    rewind(pool);
    return pool;
  }
  if (snprintf(path, sizeof(path), "%s/%dd.txt", directory, size - 1) >= (int)sizeof(path))
    ProjectCWSError("weight directory path too long");
  source = fopen(path, "r");
  if (!source) {
    fprintf(stderr, "%s: %s\n", path, strerror(errno));
    ProjectCWSError("cannot open weight pool; use --weights-dir DIRECTORY");
  }
  while (fgets(line, sizeof(line), source)) {
    char *cursor = line, *end;
    long values[5], degree = 0, gcd = 0;
    int i;
    for (i = 0; i < size; i++) {
      errno = 0;
      values[i] = strtol(cursor, &end, 10);
      if (end == cursor || errno || values[i] <= 0 || values[i] > INT_MAX - degree ||
          (i && values[i] < values[i - 1]) || (*end && !isspace((unsigned char)*end)))
        ProjectCWSError("invalid weight row (expected sorted positive integers)");
      degree += values[i];
      gcd = Fgcd(gcd, values[i]);
      cursor = end;
    }
    while (isspace((unsigned char)*cursor))
      cursor++;
    if (*cursor || gcd != 1)
      ProjectCWSError("invalid weight row (wrong width or nonprimitive)");
    fprintf(pool, "%ld", degree);
    for (i = 0; i < size; i++)
      fprintf(pool, " %ld", values[i]);
    fputc('\n', pool);
    nonempty = 1;
  }
  if (ferror(source) || !nonempty || ferror(pool))
    ProjectCWSError("empty or unreadable weight pool");
  fclose(source);
  rewind(pool);
  return pool;
}
