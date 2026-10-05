# Web Crawling & WARC Archiving with Apache Nutch 1.23

An end-to-end web crawling and digital preservation pipeline using **Apache Nutch 1.23** on Ubuntu Linux. This repository details the setup, configuration, execution, and export of multi-depth crawls targeting `quotes.toscrape.com`, culminating in the production of standard **WARC (Web ARChive)** packages.

---

## Table of Contents
- [Architecture & How Apache Nutch Works](#architecture--how-apache-nutch-works)
  - [Core Databases](#core-databases)
  - [The Crawl Loop Lifecycle](#the-crawl-loop-lifecycle)
- [Project Directory & File Structure](#project-directory--file-structure)
- [Prerequisites & Environment Configuration](#prerequisites--environment-configuration)
- [Crawl Execution Walkthrough](#crawl-execution-walkthrough)
  - [1. Seed URL Generation](#1-seed-url-generation)
  - [2. Crawler Politeness & Identity Configuration](#2-crawler-politeness--identity-configuration)
  - [3. Automated Crawl Loop Execution](#3-automated-crawl-loop-execution)
  - [4. Inspecting Segment Data](#4-inspecting-segment-data)
  - [5. Exporting to WARC (Web ARChive)](#5-exporting-to-warc-web-archive)
- [What Is Inside the Generated Project Files](#what-is-inside-the-generated-project-files)
- [Troubleshooting & Solved Issues](#troubleshooting--solved-issues)
- [CLI Command Cheat Sheet](#cli-command-cheat-sheet)

---

## Architecture & How Apache Nutch Works

Apache Nutch is an enterprise-grade, extensible web crawler that executes its data pipeline as discrete **Hadoop MapReduce** batch jobs, even when executing in standalone/local mode.

### Core Databases
1. **CrawlDb (`crawl/crawldb`)**: The master database containing all URLs known to the crawler. It tracks page status (unfetched, fetched, gone), scheduled fetch intervals, signature hashes, and scoring metrics.
2. **LinkDb (`crawl/linkdb`)**: An inverted index of hyperlinks recording which pages link to a target URL, along with their respective anchor text.
3. **Segments (`crawl/segments`)**: Immutable, timestamped partitions containing the raw content, parsed data, and extracted outlinks for each specific crawl round.

### The Crawl Loop Lifecycle

```text
  [ Seed URLs ]
        │
        ▼
  ┌───────────┐     ┌────────────┐     ┌───────────┐     ┌───────────┐
  │  Inject   │ ──► │  Generate  │ ──► │   Fetch   │ ──► │   Parse   │
  └───────────┘     └────────────┘     └───────────┘     └───────────┘
                                                               │
  ┌───────────┐     ┌────────────┐     ┌───────────┐           │
  │   Dedup   │ ◄── │ InvertLinks│ ◄── │  UpdateDB │ ◄─────────┘
  └───────────┘     └────────────┘     └───────────┘
        │
        ▼
  ┌───────────┐
  │   WARC    │
  │  Exporter │
  └───────────┘
```

* **Inject (`bin/nutch inject`)**: Ingests new URLs from seed text files into the CrawlDb.
* **Generate (`bin/nutch generate`)**: Reads the CrawlDb, filters candidates due for fetching, and partitions them into a new segment.
* **Fetch (`bin/nutch fetch`)**: Multi-threaded network workers download page content while enforcing crawl delays and `robots.txt` compliance.
* **Parse (`bin/nutch parse`)**: Runs parsers (`parse-html`, `parse-tika`) over downloaded content to extract text and identify outlinks.
* **UpdateDb (`bin/nutch updatedb`)**: Ingests discovered outlinks back into the CrawlDb and updates status records for fetched pages.
* **InvertLinks (`bin/nutch invertlinks`)**: Collects incoming hyperlinks into the LinkDb for graph and anchor analysis.
* **Dedup (`bin/nutch dedup`)**: Identifies duplicate pages in the CrawlDb based on MD5 signature hashes.
* **WARC Export (`bin/nutch warc`)**: Packages crawled HTTP requests, responses, and metadata into ISO 28500 compliant `.warc` archives.

---

## Project Directory & File Structure

```text
apache-nutch-1.23/
├── bin/
│   ├── crawl                          # High-level shell orchestrator for multi-iteration crawls
│   └── nutch                          # Master CLI router for standalone MapReduce jobs
├── conf/
│   ├── nutch-site.xml                 # Custom user overrides (User-Agent, delays, timeouts)
│   ├── nutch-default.xml              # Base system configuration
│   ├── regex-urlfilter.txt            # Regex inclusion/exclusion filters
│   └── log4j2.xml                     # Logging configuration
├── urls/
│   └── seed.txt                       # Initial seed URL list
├── crawl/
│   ├── crawldb/                       # Master URL repository
│   │   └── current/                   # SequenceFiles storing CrawlDatum records
│   ├── linkdb/                        # Inbound link graph and anchor text index
│   │   └── current/                   # SequenceFiles storing Inlinks records
│   └── segments/
│       ├── 20260915123109/             # Iteration 1 segment (seed fetch)
│       └── 20260915123422/             # Iteration 2 segment (discovered depth-1 outlinks)
└── warc-files/
    ├── part-r-00000.seg-00000.attempt-00000.warc      # Archival WARC container
    └── .part-r-00000.seg-00000.attempt-00000.warc.crc  # MapReduce checksum file
```

---

## Prerequisites & Environment Configuration

### System Environment
* **OS**: Ubuntu Linux (VirtualBox)
* **Java Runtime**: OpenJDK 21 LTS (`/usr/lib/jvm/java-21-openjdk-amd64`)
* **Nutch Version**: Apache Nutch 1.23 (Binary release)

### Setting `JAVA_HOME`
Nutch requires the `JAVA_HOME` environment variable to locate the JDK binaries.

```bash
# Locate the active Java installation
readlink -f $(which java)
# Output: /usr/lib/jvm/java-21-openjdk-amd64/bin/java

# Export JAVA_HOME for the current terminal session
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Persist variable across all future sessions
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

Verify that Nutch detects the environment properly:
```bash
./bin/nutch
```

---

## Crawl Execution Walkthrough

### 1. Seed URL Generation
Create the directory and define the root entry point:
```bash
mkdir -p urls
echo "[https://quotes.toscrape.com](https://quotes.toscrape.com)" > urls/seed.txt
```

### 2. Crawler Politeness & Identity Configuration
Edit `conf/nutch-site.xml` using `nano`:
```bash
nano conf/nutch-site.xml
```

Add the following configuration properties:
```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
    <!-- Custom Crawler User-Agent -->
    <property>
        <name>http.agent.name</name>
        <value>StudentCrawler</value>
    </property>

    <property>
        <name>http.robots.agents</name>
        <value>studentcrawler,*</value>
    </property>

    <!-- Politeness: Wait interval between requests to the same host (seconds) -->
    <property>
        <name>fetcher.server.delay</name>
        <value>5.0</value>
    </property>

    <!-- Maximum simultaneous worker threads hitting the same host -->
    <property>
        <name>fetcher.threads.per.host</name>
        <value>1</value>
    </property>
</configuration>
```

### 3. Automated Crawl Loop Execution
Launch a 2-iteration crawl cycle using the wrapper script:
```bash
./bin/crawl -s urls crawl 2
```

**What occurred during this run:**
* **Iteration 1**: Injected `https://quotes.toscrape.com/` into `crawl/crawldb`. Generated and fetched segment `20260915123109`. Extracted 48 unique internal and external outlinks.
* **Iteration 2**: Generated segment `20260915123422` with the top-scoring candidate links. Fetcher threads retrieved 48 pages (author pages, tag archives, page pagination) adhering to the 5-second polite crawl delay.

### 4. Inspecting Segment Data
Nutch segment directories store records inside binary Hadoop SequenceFiles. To inspect content in human-readable plain text, use `readseg -dump` targeting a single segment:

```bash
./bin/nutch readseg -dump crawl/segments/20260915123109 output_segment_1
```

### 5. Exporting to WARC (Web ARChive)
To convert the crawl data into standard archival files, run the `warc` command pointing to the segments directory:

```bash
./bin/nutch warc warc-files -dir crawl/segments
```

> **Warning:** Do not create the output folder (`warc-files`) beforehand. Hadoop MapReduce will fail if the destination directory already exists.

Verify the generated output:
```bash
find warc-files -type f
```
Output:
```text
warc-files/part-r-00000.seg-00000.attempt-00000.warc
warc-files/.part-r-00000.seg-00000.attempt-00000.warc.crc
```

---

## What Is Inside the Generated Project Files

| Directory / File | Type | Description |
| :--- | :--- | :--- |
| `crawl/crawldb/current` | Hadoop MapFile | Persistent database of all URLs known to the crawler, their fetch status (`db_unfetched`, `db_fetched`, `db_duplicate`), timestamps, and retry counts. |
| `crawl/linkdb/current` | Hadoop MapFile | Inverted link graph recording all incoming links and anchor texts for each indexed URL. |
| `crawl/segments/<timestamp>/crawl_generate` | SequenceFile | Batch list of URLs selected from CrawlDb for fetching during this specific iteration. |
| `crawl/segments/<timestamp>/crawl_fetch` | SequenceFile | Fetch status codes, fetch timestamps, and raw HTTP response headers. |
| `crawl/segments/<timestamp>/content` | SequenceFile | Complete raw binary payloads (raw HTML markup, images, assets) downloaded from the web. |
| `crawl/segments/<timestamp>/parse_text` | SequenceFile | Clean text stripped of HTML tags, extracted during the parse phase. |
| `crawl/segments/<timestamp>/crawl_parse` | SequenceFile | Outlinks and structural metadata discovered on the fetched pages. |
| `warc-files/*.warc` | ISO 28500 File | Self-contained, standard web archive storing WARC headers, HTTP request records, response payloads, and metadata. |

---

## Troubleshooting & Solved Issues

### 1. `Error: JAVA_HOME is not set.`
* **Cause**: Nutch shell scripts cannot run Hadoop MapReduce jobs without knowing where the Java Development Kit is located.
* **Solution**:
  ```bash
  export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
  ```

### 2. `WARCExporter job failed: Output directory ... already exists`
* **Cause**: Pre-creating the output directory using `mkdir -p warc-output` causes MapReduce's `FileOutputFormat` validation to abort to prevent accidental overwrites.
* **Solution**: Delete the folder and allow Nutch to create it during execution:
  ```bash
  rm -r warc-output
  ./bin/nutch warc warc-files -dir crawl/segments
  ```

### 3. `readseg -dump` Returns Nothing
* **Cause**: Running `./bin/nutch readseg -dump crawl/segments/* output` expands the wildcard shell operator into multiple directory arguments, leading Nutch to interpret the second segment as the output target.
* **Solution**: Target one segment directory at a time:
  ```bash
  ./bin/nutch readseg -dump crawl/segments/20260915123109 output_seg1
  ```

### 4. `SSL connect failed with: Read timed out`
* **Log Entry**:
  ```text
  org.apache.nutch.protocol.http.api.HttpException: SSL connect to [https://quotes.toscrape.com/tag/life/](https://quotes.toscrape.com/tag/life/) failed with: Read timed out
  ```
* **Cause**: Remote server socket stalls or throttled connections during multi-threaded crawling.
* **Resolution**: Expected behavior in web crawling. Nutch catches the socket timeout, automatically delays subsequent requests to the host queue, and reschedules the failed URL for retry in subsequent iterations.

---

## CLI Command Cheat Sheet

```bash
# Inject seed URLs into CrawlDb
./bin/nutch inject crawl/crawldb urls

# Generate a fetch list
./bin/nutch generate crawl/crawldb crawl/segments -topN 5000

# Fetch active segment
./bin/nutch fetch crawl/segments/<segment_id> -threads 50

# Parse downloaded content
./bin/nutch parse crawl/segments/<segment_id>

# Update CrawlDb with parsed links
./bin/nutch updatedb crawl/crawldb crawl/segments/<segment_id>

# Invert discovered links into LinkDb
./bin/nutch invertlinks crawl/linkdb crawl/segments/<segment_id>

# Deduplicate pages in CrawlDb
./bin/nutch dedup crawl/crawldb

# Print crawl statistics
./bin/nutch readdb crawl/crawldb -stats

# Package segments into WARC
./bin/nutch warc warc-output -dir crawl/segments
```
