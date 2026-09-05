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

Check our [paper](https://www.usenix.org/conference/osdi26/presentation/xu-yechen) for technical details.

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

## Citation
If you find Nixie useful, please consider citing our research work:
```bibtex
@inproceedings{xu2026nixie,
  title={Nixie: Efficient, Transparent Temporal Multiplexing for Consumer $\{$GPUs$\}$},
  author={Xu, Yechen and Wang, Yifei and Ren, Nathanael and Chen, Yiran and Zhuo, Danyang},
  booktitle={20th USENIX Symposium on Operating Systems Design and Implementation (OSDI 26)},
  pages={2085--2101},
  year={2026}
}
```
