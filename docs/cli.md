# Nixie CLI Guide

This guide documents the current `nixie` command-line interface as implemented in the codebase. It is workflow-first: start the daemon, run workloads under Nixie, inspect runtime state, and then use the control commands when needed.

## How the CLI works

- `nixie daemon` starts the Nixie daemon and loads its startup configuration.
- `nixie run` launches a command with `LD_PRELOAD=libnixiesidecar.so`. If you pass `-d` or `--device`, it also sets `CUDA_VISIBLE_DEVICES`.
- `nixie status`, `nixie priority`, `nixie history`, `nixie prefetch`, and `nixie config` talk to the daemon over `/tmp/nixie-ctl.sock`, so they require the daemon to be running.
- The CLI is Linux-oriented: it uses a Unix socket, `LD_PRELOAD`, and the shared library `libnixiesidecar.so`.

## Common workflow

### 1. Start the daemon

Start with the default configuration:

```bash
nixie daemon
```

Override memory sizes and device limits on the command line:

```bash
# 32GB pinned memory, 64GB paged memory, and a global device limit of 31GB with a per-device override of 24GB for GPU 3
nixie daemon --shmem 32g --hostmem 64g --device-limit 'g:31g/3:24g'
```

Load a TOML config file and then override part of it from the CLI:

```bash
nixie daemon --config-path /path/to/nixie.toml --auto-prefetch false
```

Options:

- `-c, --config-path <CONFIG_PATH>`: path to a TOML config file.
- `--shmem <SIZE>`: shared-memory pool size. Alias: `--shm`.
- `--hostmem <SIZE>`: host-memory pool size. Aliases: `--host`, `--ram`, `--paged`.
- `-l, --device-limit <SPEC>`: GPU memory limit policy. Alias: `--dlimit`.
- `--auto-prefetch <true|false>`: enable or disable automatic prefetching.

Code-backed defaults:

- Shared memory: `32 GiB`
- Host memory: `32 GiB`
- Device limit: global `g:0.95`
- Auto prefetch: enabled

Size parsing rules:

- CLI size arguments accept raw bytes or integer suffixes `k`, `kb`, `m`, `mb`, `g`, `gb`, `t`, `tb`.
- Examples: `33554432`, `1024m`, `32g`.
- The daemon validates `shmem_size_mb` and `hostmem_size_mb` as multiples of 2 MiB, so odd-MiB values are rejected.

Device-limit syntax:

- A global entry is required.
- Use `g:<value>` or `global:<value>` for the default limit.
- Use `/<gpu_index>:<value>` for per-device overrides.
- Values can be ratios in `[0.0, 1.0]` or absolute sizes such as `24g`.

Valid examples:

```text
g:0.95
g:31g
g:31g/3:24g
```

### 2. Run an application under Nixie

Run a command with the Nixie sidecar injected:

```bash
nixie run python train.py
```

Restrict the launched process to a specific GPU:

```bash
nixie run -d 0 python train.py
```

Pass multiple visible devices:

```bash
nixie run -d 0,1 ./my_binary --flag value
```

Notes:

- `nixie run` does not talk to `/tmp/nixie-ctl.sock`; it spawns a child process directly.
- If the library cannot be found, `nixie run` exits with an error instead of launching the child process.
- The command exits with the child process exit code.

Sidecar library lookup order:

1. the `nixie` executable directory
2. `../lib` relative to the executable
3. each directory in `LD_LIBRARY_PATH`
4. standard library paths such as `/usr/local/lib`, `/usr/lib`, `/usr/lib64`, `/lib`, `/lib64`

### 3. Inspect runtime state

Show the high-level runtime summary:

```bash
nixie status
```

Include more per-process detail in the summary view:

```bash
nixie status -v
```

Show tracked processes and aggregated allocation information:

```bash
nixie status process
```

Show per-allocation details for each tracked process:

```bash
nixie status process -v
```

Show tracked processes as JSON:

```bash
nixie status process --json
```

What these commands do:

- `nixie status` shows GPU, SHM, host-memory, and disk usage totals.
- `nixie status -v` adds a per-process breakdown for data stored on GPU, SHM, host memory, and storage.
- `nixie status process` lists tracked processes, their runtime state, priority, and per-device allocation totals.
- `nixie status process -v` expands each device entry into individual allocations.
- `nixie status process --json` prints a pretty-printed JSON array.

Important caveat:

- `--json` cannot be combined with `-v`. The CLI exits with an error if both are supplied.

### 4. Manage scheduling priority and history

Nixie accepts either a raw PID or a tracked index as a process selector:

- PID: `1234`
- Tracked index: `idx3`
- Short tracked index: `i3`

Tracked indexes come from the process list order shown by `nixie status process`.

Set a fixed priority:

```bash
nixie priority set --pid 1234 interactive
nixie priority set --pid idx0 low-interactive
nixie priority set --pid i2 batch
nixie priority set --pid 4321 background
```

Return a process to dynamic scheduling:

```bash
nixie priority unset --pid 1234
```

Show recent scheduling history for a process:

```bash
nixie history --pid idx0
```

Notes:

- Fixed priority levels are `interactive`, `low-interactive`, `batch`, and `background`.
- `priority unset` switches a fixed-priority process back to dynamic priority management.
- `history` prints recent transitions, durations, and stop reasons for the selected process.

### 5. Move data manually with `prefetch`

The CLI syntax is:

```text
<pid>:<src>-><dest>=<size>
```

Multiple operations are comma-separated, and each operation must repeat the PID or tracked index:

```bash
nixie prefetch '1100:storage->hostmem=1g,1100:hostmem->shm=1g'
```

This grouped form is rejected by the actual CLI parser because the second operation has no PID:

```text
1100:storage->hostmem=1g,hostmem->shm=1g
```

Accepted locations:

- `gpu`, which the parser treats as `gpu0`
- `gpuN`, such as `gpu1`
- `shm`
- `hostmem`
- `storage`

Accepted aliases:

- `host` for `hostmem`
- `disk` for `storage`

Syntax example accepted by the parser:

```bash
nixie prefetch '1100:gpu->shm=10g'
```

Runtime caveats enforced by the daemon:

- GPU sources are rejected at runtime, so `gpu->...` forms parse but fail when sent to the daemon.
- GPU-destination moves must all belong to the same process.
- The active process cannot be prefetched.

Practical example that matches both the parser and runtime rules:

```bash
nixie prefetch '1100:storage->hostmem=1g,1100:hostmem->shm=1g'
```

### 6. Generate shell completions

Generate a completion script and redirect it to a file:

```bash
nixie completion bash > nixie.bash
nixie completion zsh > _nixie
nixie completion fish > nixie.fish
```

Notes:

- Supported shells are `bash`, `zsh`, and `fish`.
- The script is written to standard output.

### 7. Inspect and update daemon config

Show the current in-memory config:

```bash
nixie config show
```

Update the device-limit policy on a running daemon:

```bash
nixie config update --device-limit 'g:0.95/1:24g'
```

Set a scheduling cooldown in milliseconds:

```bash
nixie config update --schedule-cooldown 500
```

Clear the scheduling cooldown:

```bash
nixie config update --schedule-cooldown 0
```

Notes:

- `config show` prints Rust debug output, not a stable machine-oriented format.
- `config update` currently exposes only `--schedule-cooldown` and `--device-limit`.
- `--schedule-cooldown 0` clears the cooldown by setting it back to `None`.
- There is currently no CLI flag for changing `automatic_prefetch` on a running daemon.

## Config files

`nixie daemon --config-path <PATH>` loads a TOML file into the daemon's `InitConfig`.

Precedence:

1. Built-in defaults
2. Config file
3. Daemon CLI flags

Supported config keys:

- `shmem_size_mb`
- `hostmem_size_mb`
- `device_memory_mb`
- `device_limit`
- `schedule_cooldown`
- `automatic_prefetch`
- `preallocate_hostmem`

Conservative example:

```toml
shmem_size_mb = 32768
hostmem_size_mb = 32768
device_limit = "g:0.95/3:24g"
automatic_prefetch = true
preallocate_hostmem = false
```

Notes:

- The config file uses MiB-based integer fields for `shmem_size_mb` and `hostmem_size_mb`, unlike the CLI flags, which accept size strings like `32g`.
- `device_limit` is a string field using the same format as `--device-limit`.
- `device_memory_mb` is supported by the loader, but most users should not need it because the daemon already probes GPU memory through NVML at startup.
- `schedule_cooldown` is supported by the config type, but this guide intentionally does not show a TOML example because the serialized duration format is not obvious from the code alone.

## Reference

### `nixie`

```text
nixie <COMMAND>
```

Commands:

- `daemon`
- `prefetch`
- `status`
- `priority`
- `completion`
- `history`
- `run`
- `config`

Options:

- `-h, --help`
- `-V, --version`

### `nixie daemon`

```text
nixie daemon [OPTIONS]
```

Options:

- `-c, --config-path <CONFIG_PATH>`
- `--shmem <SHMEM>`; alias: `--shm`
- `--hostmem <HOSTMEM>`; aliases: `--host`, `--ram`, `--paged`
- `-l, --device-limit <DEVICE_LIMIT>`; alias: `--dlimit`
- `--auto-prefetch <AUTO_PREFETCH>`

### `nixie run`

```text
nixie run [OPTIONS] <COMMAND> [ARGS]...
```

Options:

- `-d, --device <DEVICE>`

### `nixie status`

```text
nixie status [OPTIONS]
nixie status process [OPTIONS]
```

Options:

- `nixie status`: `-v, --verbose`
- `nixie status process`: `-v, --verbose`
- `nixie status process`: `--json`

### `nixie priority`

```text
nixie priority set --pid <PID> <interactive|low-interactive|batch|background>
nixie priority unset --pid <PID>
```

Options:

- `-p, --pid <PID>` where `<PID>` can be a real PID, `idxN`, or `iN`

### `nixie history`

```text
nixie history --pid <PID>
```

Options:

- `-p, --pid <PID>` where `<PID>` can be a real PID, `idxN`, or `iN`

### `nixie prefetch`

```text
nixie prefetch <MOVE_OPS>
```

Argument:

- `<MOVE_OPS>` in the form `<pid>:<src>-><dest>=<size>`

### `nixie completion`

```text
nixie completion <bash|zsh|fish>
```

### `nixie config`

```text
nixie config show
nixie config update [OPTIONS]
```

Options for `config update`:

- `-c, --schedule-cooldown <SCHEDULE_COOLDOWN>`
- `-l, --device-limit <DEVICE_LIMIT>`
