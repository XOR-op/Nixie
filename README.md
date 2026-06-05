<p align="center"
  <picture>
    <img src="./assets/nixie.svg" alt="Nixie">
  </picture>
</p>

<h3 align="center">
Fast, transparent and memory-efficient GPU multiplexing
</h3>

<p align="center">
<a href="https://github.com/XOR-op/Nixie/actions">
<img src="https://img.shields.io/github/actions/workflow/status/XOR-op/Nixie/check.yml?style=flat-square" alt="GitHub Actions">
</a>
<a href="./LICENSE">
<img src="https://img.shields.io/github/license/XOR-op/Nixie?style=flat-square&color=blue" alt="License">
</a>
</p>

## About

Nixie is an efficient service for transparent GPU multiplexing without worrying about insufficient VRAM/DRAM capacity on Linux.

Our highlighted features include:

- Optimizing for modern large AI models.
- Transparent GPU multiplexing, supporting popular applications like llama.cpp, SGLang, ComfyUI and more out of the box.
- Low task switching latency
- Configurable maximum memory size depending on user needs.

## Getting Started

### Installation

Prerequisites:

- Rust (>=1.90 stable)

Build the project with:

```bash
git clone https://github.com/XOR-op/nixie
cd nixie
cargo build --release
```

### Launch Applications With Nixie

First, we need to start Nixie daemon:

```bash
nixie daemon
```

To configure the capacity of memory used, run with

```bash
nixie daemon --shmem <pinned-memory-size> --hostmem <paged-memory-size>
# For example, to use 16GB of pinned memory and 32GB of paged memory:
nixie daemon --shmem 16g --hostmem 32g
```

Then, we can launch applications with Nixie:

```bash
nixie run <app-name> <app-args>
```

To specify which GPU to use, assuming we use GPU 0:

```bash
nixie run -d 0 <app-name> <app-args>
```

### CLI Reference

See [CLI Reference](./docs/cli.md) for more details on the available commands and options.
