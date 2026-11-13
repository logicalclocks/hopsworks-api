---
name: hops-unstructured-data
description: Parses unstructured data from files and writes it to a feature group.
---

# Unstructured Data Extraction

This is a **feature pipeline** task. Extracting structured fields from unstructured files is a Model-Independent Transformation (MIT): it produces reusable features that many models can later consume from the feature store, not features tied to one model. The lowest-cost feature pipeline is the one you don't have to create, so favour a clean, reusable schema over per-model shortcuts.

## Contract
- **Input:** raw unstructured files (emails, PDFs, logs, transcripts, scraped HTML) and a target DataFrame schema (Pandas, Polars, PySpark).
- **Output:** a populated Hopsworks feature group holding the structured (reusable) features extracted from the files.
- **Pre-condition:** the raw files are accessible and a target schema has been agreed with the developer.

## Documents to text: `hops fg parse`

PDF, DOCX, PPTX, XLSX, HTML and images (scans included) are read with `hops fg parse`, never with a parser installed in the terminal. It runs as a job in the project and writes one row per page (`path`, `page`, `text` as Markdown with tables as Markdown tables):

```bash
hops fg parse Resources/hopsable/uploads --to uploads_pages      # chat attachments; a Drive or SharePoint sync's dataset works the same
hops job wait uploads_pages_parse --execution <id> --timeout 3600
hops fg preview uploads_pages
hops fg embed uploads_pages --text text                          # chunks and vectors for search
```

- A rerun parses only changed files and deletes the rows of files that are gone. One group parses one path; parse another path into another group.
- PDFs with a text layer skip OCR; `--ocr on` also reads text inside images. `--accurate` uses the slower table model.
- A run stops on the cluster's caps (file size, pages, minutes) and names what it did not parse; run it again to go on.
- Extract structured fields (below) from the `text` column of the pages group.

## Steps

You first examine the unstructured text to identify and propose schemas for the classes of unstructured files. Once you have agreed with the developer on a schema, you extract structured data from unstructured text. Given raw input (emails, PDFs, logs, transcripts, scraped HTML) and the target schema for a DataFrame (Pandas, Polars, PySpark):

  1. Read the schema first. Note required vs optional fields, enums, and format constraints (dates, currencies, IDs). The schema is the contract — never emit a key it doesn't define.
  2. Scan the input for each field. Prefer explicit values over inferred ones. If a required field is genuinely absent, use null rather than guessing.
  3. Normalize as you extract: trim whitespace, coerce dates to ISO 8601, strip currency symbols into numeric + code, collapse enum synonyms to their canonical value.
  4. For every type of unstructured file, create a DataFrame based on its schema and populate it with the data from the unstructured files.
  5. Write the DataFrame to a feature group in Hopsworks (get_or_create the feature group).

  When the input is ambiguous, pick the most conservative interpretation and note the ambiguity in a top-level "_extraction_notes" field.

## Next Steps

- Create and write the target feature group: **hops-fg**.
- Pick the SharePoint files to extract from by searching a sync's file index: **hops-sharepoint-search**.
- For RAG over documents: `hops fg parse`, then `hops fg embed` on the pages group (above), and serve similarity (kNN) search via **hops-fv**.
