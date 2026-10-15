# Primary-source cache

This ignored directory can hold primary PDFs and available author TeX cited by
the [literature notes](../README.md). Cached documents are not included in a
clone; retrieve the exact editions from the notes' primary URLs when needed.
The cache is not an input to the Lean build.

Use one directory per source and version:

```text
sources/
  kreuzer-skarke-math-0001106/
    v1/
      paper.pdf
      source.tar.gz
      tex/
      provenance.json
```

Preserve original downloaded bytes; extracted TeX and text are supplementary.
Record title/authors, primary URL, verified arXiv ID or DOI, edition/version,
retrieval date, license information and original-file SHA-256 in
`provenance.json`. The corresponding note records the primary citation,
version, cache-relative path and hash so another checkout can identify the
same source.

Check existing provenance and hashes before fetching again. Keep editions
separate, do not overwrite a cited version, and distinguish inaccessible
material from sources actually read. The separate `../SINGLE_WS_5d.tex` is not
part of this cache.

Downloaded documents, archives, extracted files and local provenance remain
ignored by Git. Only this index and the ignore policy are tracked.
