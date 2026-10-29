#![cfg_attr(not(test), no_std)]

pub mod qwp;

use core::fmt::Write;
use core::mem::MaybeUninit;

/// Accel FIFO depth on the LSM303AGR.
pub const FIFO: usize = 32;
pub const MAG_BATCH: usize = 8;

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

/// Write `v / 10^decimals` as a decimal without touching floats (fast on the ESP32).
fn fixed(out: &mut impl Write, v: i64, decimals: u32) -> core::fmt::Result {
    let p = 10i64.pow(decimals);
    let sign = if v < 0 { "-" } else { "" };
    let a = v.unsigned_abs();
    write!(
        out,
        "{sign}{}.{:0w$}",
        a / p as u64,
        a % p as u64,
        w = decimals as usize
    )
}

/// Append a record as ILP lines (timestamps in µs, POST with `precision=u`).
pub fn ilp(out: &mut impl Write, r: &Rec) -> core::fmt::Result {
    match *r {
        Rec::Accel {
            t_us,
            dt_ns,
            n,
            xyz,
        } => {
            for (i, s) in xyz[..n as usize].iter().enumerate() {
                out.write_str("accel x=")?;
                fixed(out, accel_ums2(s[0]) / 1000, 3)?; // mm/s², resolution is ~0.3 m/s² anyway
                out.write_str(",y=")?;
                fixed(out, accel_ums2(s[1]) / 1000, 3)?;
                out.write_str(",z=")?;
                fixed(out, accel_ums2(s[2]) / 1000, 3)?;
                writeln!(out, " {}", sample_us(t_us, dt_ns, n, i))?;
            }
        }
        Rec::Mag { n, t_us, xyz_nt } => {
            for i in 0..n as usize {
                let [x, y, z] = xyz_nt[i];
                out.write_str("mag x=")?;
                fixed(out, x as i64, 3)?; // nT -> µT
                out.write_str(",y=")?;
                fixed(out, y as i64, 3)?;
                out.write_str(",z=")?;
                fixed(out, z as i64, 3)?;
                writeln!(out, " {}", t_us[i])?;
            }
        }
        Rec::Health {
            t_us,
            die_temp_c,
            fifo_overruns,
            dropped,
            tx_bytes,
            encode_us,
        } => writeln!(
            out,
            "health die_temp_c={die_temp_c:.1},fifo_overruns={fifo_overruns}i,dropped={dropped}i,tx_bytes={tx_bytes}i,encode_us={encode_us}i {t_us}"
        )?,
    }
    Ok(())
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
            } => Some((
                t_us,
                die_temp_c,
                [fifo_overruns, dropped, tx_bytes, encode_us],
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
        ];
        e.table("health", rows, &cols)?;
        e.f32s(health().map(|h| h.1))?;
        for c in 0..4 {
            e.i64s(health().map(|h| h.2[c] as i64))?;
        }
        e.timestamps(health().map(|h| h.0))?;
    }

    Ok(e.finish())
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

    /// 1024 rows of steady-rate accel: QWP should be ~13 bytes/row (3 floats + ~1 bit of
    /// timestamp) against ~45 for the same rows as ILP text.
    #[test]
    fn qwp_vs_ilp_size() {
        let recs: std::vec::Vec<Rec> = (0..32)
            .map(|i| accel_rec(1_000_000 + i * 32 * 182))
            .collect();
        let mut buf = std::vec![0u8; 64 * 1024];
        let qwp = qwp_message(&mut buf, &recs).unwrap();
        let mut ilp_text = std::string::String::new();
        recs.iter().for_each(|r| ilp(&mut ilp_text, r).unwrap());
        let rows = 32 * FIFO;
        std::println!(
            "{rows} rows: qwp {qwp} B ({:.1} B/row), ilp {} B ({:.1} B/row)",
            qwp as f64 / rows as f64,
            ilp_text.len(),
            ilp_text.len() as f64 / rows as f64
        );
        assert!(qwp < rows * 13, "{qwp}");
        assert_eq!(buf[6], 1); // one table
    }
}
