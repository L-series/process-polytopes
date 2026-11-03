#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Global.h"

_Static_assert(sizeof(Long) >= 8, "five-dimensional CPU completion requires 64-bit PALP Long");
FILE *inFILE = NULL;
FILE *outFILE = NULL;
void Make_CWS_Points(CWS *, PolyPointList *);

static int fail(const char *message) {
    fprintf(stderr,"CPU completion failed: %s\n",message);
    return 1;
}

int main(void) {
    CWS *cw = malloc(sizeof(*cw));
    PolyPointList *points = malloc(sizeof(*points));
    EqList *equations = malloc(sizeof(*equations));
    VertexNumList *vertices = malloc(sizeof(*vertices));
    if (!cw || !points || !equations || !vertices) return fail("allocation");
    outFILE = stderr;  // PALP diagnostics must never be mistaken for accepted rows.
    char *line = NULL;
    size_t capacity = 0;
    ssize_t length;
    unsigned long long processed = 0, accepted = 0;
    while ((length = getline(&line,&capacity,stdin)) >= 0) {
        if ((size_t)length != strlen(line)) return fail("embedded NUL in CWS record");
        long long tokens[55];
        int count = 0;
        char *cursor = line;
        while (1) {
            while (*cursor==' ' || *cursor=='\t' || *cursor=='\r' || *cursor=='\n') ++cursor;
            if (!*cursor) break;
            if (count == 55) return fail("too many CWS fields");
            char *end;
            errno = 0;
            tokens[count] = strtoll(cursor,&end,10);
            if (end==cursor || errno || tokens[count]<0 || tokens[count]>INT_MAX)
                return fail("invalid CWS integer");
            ++count;
            cursor = end;
        }
        int rows = 0;
        for (int r=1;r<=5;++r) if (count==r*(r+6)) rows=r;
        if (!rows) return fail("invalid CWS record width (blank lines are errors)");
        const int ambient = rows+5;
        if (ambient>AMBI_Dmax || rows>AMBI_Dmax) return fail("PALP ambient capacity");
        memset(cw,0,sizeof(*cw));
        cw->nw=rows; cw->N=ambient; cw->index=1;
        int used[10]={0};
        for (int r=0;r<rows;++r) {
            const int offset=r*(ambient+1);
            cw->d[r]=tokens[offset];
            long long sum=0;
            for (int i=0;i<ambient;++i) {
                cw->W[r][i]=tokens[offset+i+1];
                sum+=tokens[offset+i+1];
                used[i] |= tokens[offset+i+1]>0;
            }
            if (cw->d[r]<=0 || sum!=cw->d[r]) return fail("CWS degree mismatch");
        }
        for (int i=0;i<ambient;++i) if (!used[i]) return fail("uncovered CWS coordinate");
        Make_CWS_Points(cw,points);
        if (points->n != 5) return fail("unexpected kernel dimension");
        const int ip = points->np>=6 && IP_Check(points,vertices,equations);
        if (ip) {
            if (fwrite(line,1,(size_t)length,stdout)!=(size_t)length)
                return fail("accepted output write");
            if (length==0 || line[length-1]!='\n')
                if (fputc('\n',stdout)==EOF) return fail("accepted newline write");
            ++accepted;
        }
        ++processed;
    }
    if (ferror(stdin) || fflush(stdout)==EOF) return fail("input/output stream");
    // PALP has error paths that exit(0). They cannot produce this completion
    // receipt; the runner requires it and checks every count before committing.
    if (fprintf(stderr,"CWS_COMPLETION_V1 {\"processed\":%llu,\"accepted\":%llu,\"rejected\":%llu}\n",
        processed,accepted,processed-accepted)<0 || fflush(stderr)==EOF) return 1;
    free(line); free(cw); free(points); free(equations); free(vertices);
    return 0;
}
