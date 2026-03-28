# Nixie

---

## Build

Prerequisites:
- Rust (>=1.90 stable)

Build the project with:
```bash
cargo build --release
```

## Usage

Run the compiled binary:
```bash
./target/release/nixie daemon
```
to start the daemon.

For application, use:
```bash
LD_PRELOAD=<REPLACE_WITH_THIS_PATH>/target/release/libnixiesidecar.so <your_application>
```

Check with:
```bash
./target/release/nixie list 
```

More details can be found with:
```bash
./target/release/nixie --help
```