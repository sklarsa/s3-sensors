#![no_std]

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
                let back_ns = (n as i64 - 1 - i as i64) * dt_ns as i64;
                out.write_str("accel x=")?;
                fixed(out, accel_ums2(s[0]) / 1000, 3)?; // mm/s², resolution is ~0.3 m/s² anyway
                out.write_str(",y=")?;
                fixed(out, accel_ums2(s[1]) / 1000, 3)?;
                out.write_str(",z=")?;
                fixed(out, accel_ums2(s[2]) / 1000, 3)?;
                writeln!(out, " {}", t_us - back_ns / 1000)?;
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
        } => writeln!(
            out,
            "health die_temp_c={die_temp_c:.1},fifo_overruns={fifo_overruns}i,dropped={dropped}i {t_us}"
        )?,
    }
    Ok(())
}
