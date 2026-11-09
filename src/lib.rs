#![cfg_attr(not(test), no_std)]

pub mod qwp;

use core::mem::MaybeUninit;

/// Accel FIFO depth on the LSM303AGR.
pub const FIFO: usize = 32;
pub const MAG_BATCH: usize = 8;

/// Values of the `qwp_last_error` health column.
pub const QWP_ERR_CONNECT: u32 = 1;
pub const QWP_ERR_READ: u32 = 2;
pub const QWP_ERR_WRITE: u32 = 3;
pub const QWP_ERR_SERVER: u32 = 4;
/// any other error; the codes below split out the ones we know
pub const QWP_ERR_PROTOCOL: u32 = 5;
/// an ack's sequence wasn't the next one expected: `qwp_err_got`, `qwp_err_want`
pub const QWP_ERR_SEQ: u32 = 6;
/// a binary frame that isn't a QWP response: first byte, frame length
pub const QWP_ERR_BAD_RESPONSE: u32 = 7;
pub const QWP_ERR_FRAME_HEADER: u32 = 8;
pub const QWP_ERR_UPGRADE: u32 = 9;
pub const QWP_ERR_TOO_BIG: u32 = 10;

/// One ring slot. Fixed size so the ring is a plain array in PSRAM.
#[derive(Clone, Copy)]
pub enum Rec {
    /// `n` accel samples; the last one was taken at `t_us`, earlier ones are `dt_ns` apart.
    Accel {
        t_us: i64,
        dt_ns: u32,
        n: u8,
        xyz: [[i16; 3]; FIFO],
    },
    Mag {
        n: u8,
        t_us: [i64; MAG_BATCH],
        xyz_nt: [[i32; 3]; MAG_BATCH],
    },
    Health {
        t_us: i64,
        die_temp_c: f32,
        fifo_overruns: u32,
        dropped: u32,
        /// bytes put on the wire and µs spent encoding them, over the last second
        tx_bytes: u32,
        encode_us: u32,
        /// signal from the AP's last beacon; 0 when not associated
        rssi_dbm: i32,
        /// connected AP's MAC as a 48-bit integer and its channel; 0 when not associated
        ap_bssid: u64,
        wifi_channel: u8,
        heap_free: u32,
        /// cumulative since boot
        wifi_disconnects: u32,
        qwp_reconnects: u32,
        /// records waiting to be acked by QuestDB
        ring_len: u32,
        /// cumulative since boot, at the boundary between the TCP stack and the WiFi driver
        drv_rx_frames: u32,
        drv_tx_frames: u32,
        drv_tx_bytes: u32,
        /// times the driver had no room to transmit
        drv_tx_busy: u32,
        /// QWP messages acked by QuestDB, cumulative since boot
        qwp_acks: u32,
        /// QWP messages QuestDB rejected as bad data, which are dropped; cumulative since boot
        qwp_rejected: u32,
        /// why the last QWP connection ended: see `QWP_ERR_*`, 0 if none has
        qwp_last_error: u32,
        /// values behind `qwp_last_error`, see `QWP_ERR_*`
        qwp_err_got: i64,
        qwp_err_want: i64,
        uptime_s: u32,
        /// esp-hal's SocResetReason for the last boot
        reset_reason: u32,
    },
}

/// Drop-newest ring: the sender may be reading the head while the sensor task pushes.
pub struct Ring {
    buf: &'static mut [MaybeUninit<Rec>],
    head: usize,
    len: usize,
    pub dropped: u32,
}

impl Ring {
    pub fn new(buf: &'static mut [MaybeUninit<Rec>]) -> Self {
        Self {
            buf,
            head: 0,
            len: 0,
            dropped: 0,
        }
    }
    pub fn push(&mut self, r: Rec) {
        if self.len == self.buf.len() {
            self.dropped += 1; // ponytail: drops newest when full, keeps in-flight head stable
            return;
        }
        let i = (self.head + self.len) % self.buf.len();
        self.buf[i].write(r);
        self.len += 1;
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }
    pub fn get(&self, i: usize) -> Option<Rec> {
        // SAFETY: slots [head, head+len) were written by push
        (i < self.len)
            .then(|| unsafe { self.buf[(self.head + i) % self.buf.len()].assume_init_read() })
    }
    pub fn pop(&mut self, n: usize) {
        let n = n.min(self.len);
        self.head = (self.head + n) % self.buf.len();
        self.len -= n;
    }
}

/// Low-power mode is 8-bit, left-justified: at ±4 g one digit of the high byte is 31.26 mg (datasheet table 3).
pub const ACCEL_UMS2_PER_DIGIT: i64 = 31_260 * 980_665 / 100_000; // µm/s²

fn accel_ums2(raw: i16) -> i64 {
    (raw >> 8) as i64 * ACCEL_UMS2_PER_DIGIT
}

fn accel_ms2(raw: i16) -> f32 {
    accel_ums2(raw) as f32 / 1e6
}

/// Timestamp of sample `i` of an accel record whose newest sample is at `t_us`.
fn sample_us(t_us: i64, dt_ns: u32, n: u8, i: usize) -> i64 {
    t_us - (n as i64 - 1 - i as i64) * dt_ns as i64 / 1000
}

/// Encodes records as one QWP message with up to three table blocks: accel, mag, health.
pub fn qwp_message(out: &mut [u8], recs: &[Rec]) -> Result<usize, qwp::Full> {
    use qwp::{FLOAT, LONG};
    let mut e = qwp::Encoder::new(out)?;

    let accel = || {
        recs.iter()
            .filter_map(|r| match r {
                Rec::Accel {
                    t_us,
                    dt_ns,
                    n,
                    xyz,
                } => Some((*t_us, *dt_ns, *n, xyz)),
                _ => None,
            })
            .flat_map(|(t, dt, n, xyz)| {
                xyz[..n as usize]
                    .iter()
                    .enumerate()
                    .map(move |(i, s)| (sample_us(t, dt, n, i), s))
            })
    };
    let rows = accel().count();
    if rows > 0 {
        e.table("accel", rows, &[("x", FLOAT), ("y", FLOAT), ("z", FLOAT)])?;
        for a in 0..3 {
            e.f32s(accel().map(|(_, s)| accel_ms2(s[a])))?;
        }
        e.timestamps(accel().map(|(t, _)| t))?;
    }

    let mag = || {
        recs.iter()
            .filter_map(|r| match r {
                Rec::Mag { n, t_us, xyz_nt } => Some((*n, t_us, xyz_nt)),
                _ => None,
            })
            .flat_map(|(n, t, xyz)| (0..n as usize).map(move |i| (t[i], xyz[i])))
    };
    let rows = mag().count();
    if rows > 0 {
        e.table("mag", rows, &[("x", FLOAT), ("y", FLOAT), ("z", FLOAT)])?;
        for a in 0..3 {
            e.f32s(mag().map(|(_, v)| v[a] as f32 / 1000.0))?; // nT -> µT
        }
        e.timestamps(mag().map(|(t, _)| t))?;
    }

    let health = || {
        recs.iter().filter_map(|r| match *r {
            Rec::Health {
                t_us,
                die_temp_c,
                fifo_overruns,
                dropped,
                tx_bytes,
                encode_us,
                rssi_dbm,
                ap_bssid,
                wifi_channel,
                heap_free,
                wifi_disconnects,
                qwp_reconnects,
                ring_len,
                drv_rx_frames,
                drv_tx_frames,
                drv_tx_bytes,
                drv_tx_busy,
                qwp_acks,
                qwp_rejected,
                qwp_last_error,
                qwp_err_got,
                qwp_err_want,
                uptime_s,
                reset_reason,
            } => Some((
                t_us,
                die_temp_c,
                [
                    fifo_overruns as i64,
                    dropped as i64,
                    tx_bytes as i64,
                    encode_us as i64,
                    rssi_dbm as i64,
                    ap_bssid as i64,
                    wifi_channel as i64,
                    heap_free as i64,
                    wifi_disconnects as i64,
                    qwp_reconnects as i64,
                    ring_len as i64,
                    drv_rx_frames as i64,
                    drv_tx_frames as i64,
                    drv_tx_bytes as i64,
                    drv_tx_busy as i64,
                    qwp_acks as i64,
                    qwp_rejected as i64,
                    qwp_last_error as i64,
                    qwp_err_got,
                    qwp_err_want,
                    uptime_s as i64,
                    reset_reason as i64,
                ],
            )),
            _ => None,
        })
    };
    let rows = health().count();
    if rows > 0 {
        let cols = [
            ("die_temp_c", FLOAT),
            ("fifo_overruns", LONG),
            ("dropped", LONG),
            ("tx_bytes", LONG),
            ("encode_us", LONG),
            ("rssi_dbm", LONG),
            ("ap_bssid", LONG),
            ("wifi_channel", LONG),
            ("heap_free", LONG),
            ("wifi_disconnects", LONG),
            ("qwp_reconnects", LONG),
            ("ring_len", LONG),
            ("drv_rx_frames", LONG),
            ("drv_tx_frames", LONG),
            ("drv_tx_bytes", LONG),
            ("drv_tx_busy", LONG),
            ("qwp_acks", LONG),
            ("qwp_rejected", LONG),
            ("qwp_last_error", LONG),
            ("qwp_err_got", LONG),
            ("qwp_err_want", LONG),
            ("uptime_s", LONG),
            ("reset_reason", LONG),
        ];
        e.table("health", rows, &cols)?;
        e.f32s(health().map(|h| h.1))?;
        for c in 0..cols.len() - 1 {
            e.i64s(health().map(|h| h.2[c]))?;
        }
        e.timestamps(health().map(|h| h.0))?;
    }

    Ok(e.finish())
}

/// Why a QWP connection ended, from the sender's error message.
pub fn qwp_err_code(e: &str) -> u32 {
    match e {
        "connect" => QWP_ERR_CONNECT,
        "read" => QWP_ERR_READ,
        "write" => QWP_ERR_WRITE,
        "server error" | "server closed" => QWP_ERR_SERVER,
        "seq" => QWP_ERR_SEQ,
        "bad response" => QWP_ERR_BAD_RESPONSE,
        "frame header" => QWP_ERR_FRAME_HEADER,
        "upgrade refused" => QWP_ERR_UPGRADE,
        "record exceeds max payload" => QWP_ERR_TOO_BIG,
        _ => QWP_ERR_PROTOCOL,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accel_rec(t_us: i64) -> Rec {
        Rec::Accel {
            t_us,
            dt_ns: 182_000,
            n: FIFO as u8,
            xyz: [[1 << 8, -(32 << 8), 0]; FIFO],
        }
    }

    /// 1024 rows of steady-rate accel: ~13 bytes/row (3 floats + ~1 bit of timestamp).
    #[test]
    fn qwp_size() {
        let recs: std::vec::Vec<Rec> = (0..32)
            .map(|i| accel_rec(1_000_000 + i * 32 * 182))
            .collect();
        let mut buf = std::vec![0u8; 64 * 1024];
        let qwp = qwp_message(&mut buf, &recs).unwrap();
        let rows = 32 * FIFO;
        std::println!(
            "{rows} rows: {qwp} B ({:.1} B/row)",
            qwp as f64 / rows as f64
        );
        assert!(qwp < rows * 13, "{qwp}");
        assert_eq!(buf[6], 1); // one table
    }

    /// RSSI must keep its sign and the BSSID its full 48 bits on the way into LONG columns.
    #[test]
    fn health_wifi_columns() {
        let rec = Rec::Health {
            t_us: 1_000_000,
            die_temp_c: 24.0,
            fifo_overruns: 0,
            dropped: 0,
            tx_bytes: 0,
            encode_us: 0,
            rssi_dbm: -57,
            ap_bssid: 0xfedc_ba98_7654,
            wifi_channel: 1,
            heap_free: 0,
            wifi_disconnects: 0,
            qwp_reconnects: 0,
            ring_len: 0,
            drv_rx_frames: 0,
            drv_tx_frames: 0,
            drv_tx_bytes: 0,
            drv_tx_busy: 0,
            qwp_acks: 0,
            qwp_rejected: 7_654_321,
            qwp_last_error: 6_543_217,
            qwp_err_got: -5_432_171,
            qwp_err_want: 4_321_715,
            uptime_s: 0,
            reset_reason: 0,
        };
        let mut buf = std::vec![0u8; 1024];
        let n = qwp_message(&mut buf, &[rec]).unwrap();
        let has = |v: i64| buf[..n].windows(8).any(|w| w == v.to_le_bytes());
        assert!(has(-57));
        assert!(has(0xfedc_ba98_7654));
        assert!(has(7_654_321));
        assert!(has(6_543_217));
        assert!(has(-5_432_171));
        assert!(has(4_321_715));
    }

    #[test]
    fn err_codes_distinct() {
        let names = [
            "connect",
            "read",
            "write",
            "server error",
            "seq",
            "bad response",
            "frame header",
            "upgrade refused",
            "record exceeds max payload",
            "ring",
        ];
        let codes: std::vec::Vec<u32> = names.iter().map(|n| qwp_err_code(n)).collect();
        assert_eq!(codes, [1, 2, 3, 4, 6, 7, 8, 9, 10, 5]);
    }
}
