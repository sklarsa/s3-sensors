# s3-sensors

`no_std` Rust firmware (esp-hal + embassy) that streams an LSM303AGR accelerometer at its
maximum rate into [QuestDB](https://questdb.io) over
[QWP](https://questdb.com/docs/connect/wire-protocols/qwp-ingress-websocket/), QuestDB's
binary columnar wire protocol, with a minimal `no_std` QWP encoder in [`src/qwp.rs`](src/qwp.rs).

## QWP vs ILP, measured on the device

Same stream (5.4 kHz accel + 100 Hz mag + 1 Hz health), from the `health` table, measured
against an ILP/HTTP sender (since removed, see git history):

| | ILP/HTTP | QWP/WebSocket |
|---|---|---|
| Bytes on the wire | 271 KB/s | **72 KB/s** (3.8x less) |
| Encoding CPU | 116 ms/s | **33 ms/s** (3.5x less) |
| Rows/s | 5,487 | 5,485 |

QWP ships the floats as raw little-endian columns and the designated timestamps
Gorilla-encoded (~1 bit/row for a steady stream), so an accel row costs ~12 bytes against ~39
as ILP text. Messages are pipelined (8 in flight) and acked by sequence number; ring records
are dropped only on ack.

After a 30 s QuestDB outage the ring drained at ~805 KB/s (~62k rows/s, 11x the live rate) and
caught up in ~4 s, with no rows lost.

- **Accel**: 5.376 kHz nominal (low-power 8-bit, ±4 g), drained from the 32-sample FIFO
- **Mag**: 100 Hz
- **Health**: 1 Hz die temperature, FIFO overruns, dropped records
- **Timestamps** are taken on the device (SNTP-synced), so network delay and retries don't
  move them. Accel samples are chained at the measured sample period (the chip's
  oscillator runs a few % off spec) and phase-locked to the read time.
- **Buffering**: a ~1.8 MB ring in PSRAM (~50 s at full rate) rides out network or
  QuestDB outages; records are only dropped from it once QuestDB acks them.

The sensor tasks run on core 1, WiFi and the QWP sender on core 0.

## Hardware

- Adafruit Feather ESP32-S3 with 4 MB flash / 2 MB PSRAM
- Adafruit LSM303AGR on the STEMMA QT port (I2C 0x19 accel, 0x1E mag)

## Build and flash

Needs the Xtensa Rust toolchain (`espup install --targets esp32s3`) and `espflash`.
Configuration is compiled in from the environment:

```sh
. ~/export-esp.sh
WIFI_SSID=... WIFI_PASSWORD=... QUESTDB_ADDR=192.168.1.10:9000 cargo run --release
```

The QWP encoder and record encoding are core-only, so their tests run on the host:

```sh
rustc +stable --edition 2024 --test src/lib.rs -o /tmp/s3-tests && /tmp/s3-tests --nocapture
```

## QuestDB tables

QWP creates the tables on first write, but with DOUBLE columns and no TTL. Creating them
first halves the storage. At full rate `accel` is ~20 GB/day on disk and ~45 GB/day of
writes, so put QuestDB on an SSD, not an SD card.

```sql
CREATE TABLE IF NOT EXISTS accel (ts TIMESTAMP, x FLOAT, y FLOAT, z FLOAT) -- m/s²
  TIMESTAMP(ts) PARTITION BY HOUR TTL 3 DAYS WAL;
CREATE TABLE IF NOT EXISTS mag (ts TIMESTAMP, x FLOAT, y FLOAT, z FLOAT) -- µT
  TIMESTAMP(ts) PARTITION BY DAY TTL 30 DAYS WAL;
CREATE TABLE IF NOT EXISTS health (ts TIMESTAMP, die_temp_c FLOAT, fifo_overruns LONG, dropped LONG,
  tx_bytes LONG, encode_us LONG) -- per-second bytes sent and µs spent encoding
  TIMESTAMP(ts) PARTITION BY MONTH WAL;
```

## Known limits

- **The I2C bus is the bottleneck.** The LSM303AGR tops out at 400 kHz, and each sample is
  6 bytes even in 8-bit mode (the low bytes are padding), so the accel stream uses ~75% of
  the bus. The loop uses the blocking I2C driver on core 1 (no interrupt + wake per 32-byte
  hardware FIFO refill) and drains at ≥4 samples, which keeps the worst gap between drains at
  ~3.7 ms against the 5.95 ms FIFO fill time: no overruns in practice. Overruns, if any, are
  counted in `health.fifo_overruns` and the timestamp chain re-anchors after each. Wiring INT1
  to a GPIO for FIFO watermark interrupts would add more headroom.
- `ACCEL_UMS2_PER_DIGIT` in `src/lib.rs` is the datasheet sensitivity; at rest the magnitude
  reads a few % under 9.81 m/s², so calibrate it if absolute values matter.
