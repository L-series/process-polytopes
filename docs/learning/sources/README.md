# Reusable literature sources

Use this directory as a local shared cache for primary papers consulted during
approved literature/audit work. PDFs and author-provided TeX sources may be saved
here for later agents to read without fetching the same document repeatedly.
The directory sits alongside the separate single-weight paper and learning notes.
The completed [literature review](../README.md#literature-review) records primary
PDFs and available author TeX sources in this checkout's ignored cache, with
exact versions, hashes, and provenance. These cached files are not included in a
clone; use the notes' citations to retrieve matching sources when needed.

## Organization

Store each paper in a clearly named, versioned directory, for example:

```text
sources/
  kreuzer-skarke-math-0001106/
    v1/
      paper.pdf
      source.tar.gz
      tex/
      provenance.json
```

Only save formats actually available and useful for the assignment. Keep the
original downloaded bytes; extracted TeX is supplementary and must remain
traceable to its original archive. Keep different editions/preprint versions
separate rather than overwriting a source cited by a previous note. The existing
`../SINGLE_WS_5d.tex` stays in place and need not be copied into this cache.

For each cached document, record its title/authors, primary download URL,
arXiv ID/DOI where verified, exact version or edition, retrieval date and SHA-256
of each original downloaded file in local `provenance.json`. Include any source
license information supplied with the document. The learning note records the
version, cache-relative path and original-file hash alongside its primary citation
so another checkout can retrieve the same source when the local cache is absent.

## Reuse and ownership

Check the cache before downloading. Read a matching verified local source first;
check its provenance and hash against the assigned/cited version. Fetch only when
the required source is missing, damaged, has uncertain provenance, or a different
version is needed. A cached PDF/TeX file supports reading the actual primary
document; it does not replace verifying the bibliographic record or theorem
context. Report inaccessible material instead of inventing it.

The integrator gives each researcher an owned source/version directory in addition
to its owned note. Reading existing cached sources is shared; creating cache
files is allowed in assigned directories. Coordinate papers needed by multiple
agents so one downloads and the others reuse it. Do not overwrite another agent's
cache files or edit this shared policy/index as part of a topic assignment.
Give the natural-language-to-formal checker relevant cached sources as inputs;
the blind formal-to-natural-language checker still receives only its prepared
formal packet and must not read this literature cache.

## Git boundary

Downloaded papers, TeX archives/extractions and local provenance files are ignored
by Git. This README and ignore policy are tracked; original research notes and
verified citations/provenance references remain tracked in the learning notes.
The cache persists on this checkout for later sessions, but is not distributed by
cloning the repository. Do not force-add cached documents. Saving sources during
an approved research chunk is authorized; this policy does not start a chunk.
