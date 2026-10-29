#![no_std]
#![no_main]
#![deny(
    clippy::mem_forget,
    reason = "mem::forget is generally not safe to do with esp_hal types, especially those \
    holding buffers for the duration of a data transfer."
)]

//! Feather ESP32-S3: LSM303AGR accel (5.376 kHz FIFO) + mag (100 Hz) + 1 Hz health
//! -> PSRAM ring buffer -> QWP over WebSocket to QuestDB. Timestamps are taken on the device.

use core::cell::{Cell, RefCell};
use core::fmt::Write as _;
use core::mem::MaybeUninit;
use core::net::{IpAddr, SocketAddr, SocketAddrV4};
use core::sync::atomic::{AtomicU32, Ordering::Relaxed};

use embassy_embedded_hal::shared_bus::asynch::i2c::I2cDevice;
use embassy_executor::Spawner;
use embassy_net::{
    Runner, Stack, StackResources,
    dns::DnsQueryType,
    tcp::TcpSocket,
    udp::{PacketMetadata, UdpSocket},
};
use embassy_sync::blocking_mutex::{Mutex as BlockingMutex, raw::CriticalSectionRawMutex};
use embassy_sync::{blocking_mutex::raw::NoopRawMutex, mutex::Mutex, signal::Signal};
use embassy_time::{Delay, Duration, Instant, Timer};
use embedded_hal_async::i2c::I2c as _;
use embedded_io_async::Read as _;
use embedded_io_async::Write as _;
use esp_backtrace as _;
use esp_hal::{
    Blocking,
    clock::CpuClock,
    gpio::{Level, Output, OutputConfig},
    i2c::master::{Config as I2cConfig, I2c},
    psram::Psram,
    rng::Rng,
    system::Stack as CoreStack,
    time::Rate,
    timer::timg::TimerGroup,
};
use esp_radio::wifi::{
    AuthenticationMethodConfig, Config, ControllerConfig, Interface, PowerSaveMode, WifiController,
    sta::StationConfig,
};
use log::{error, info, warn};
use lsm303agr::{
    AccelMode, AccelOutputDataRate, AccelScale, FifoMode, Lsm303agr, MagMode, MagOutputDataRate,
};
use s3_sensors::qwp;
use s3_sensors::{FIFO, MAG_BATCH, Rec, Ring};
use sntpc::{NtpContext, NtpTimestampGenerator, get_time};
use sntpc_net_embassy::UdpSocketWrapper;

esp_bootloader_esp_idf::esp_app_desc!();

macro_rules! mk_static {
    ($t:ty,$val:expr) => {{
        static STATIC_CELL: static_cell::StaticCell<$t> = static_cell::StaticCell::new();
        #[deny(unused_attributes)]
        let x = STATIC_CELL.uninit().write($val);
        x
    }};
}

const SSID: &str = env!("WIFI_SSID");
const PASSWORD: &str = env!("WIFI_PASSWORD");
const QUESTDB: &str = env!("QUESTDB_ADDR"); // ip:port of QuestDB's HTTP endpoint, e.g. 192.168.1.10:9000
const NTP_SERVER: &str = "pool.ntp.org";

const ACC: u8 = 0x19;
const FIFO_SRC_REG_A: u8 = 0x2F;
const OUT_X_L_A: u8 = 0x28;
const AUTO_INC: u8 = 0x80;
const NOMINAL_DT_NS: u32 = 1_000_000_000 / 5376;

const BODY_LEN: usize = 128 * 1024;
const TCP_TX_LEN: usize = 64 * 1024;
const SCRATCH_RECS: usize = 256; // records per QWP message (8192 accel rows max)

type Bus = Mutex<NoopRawMutex, SyncI2c>;

/// The blocking driver behind the async trait the lsm303agr crate wants. Core 1 runs nothing but the
/// sensor loop, so spinning is free, and it skips the async driver's interrupt + task wake every
/// 32-byte hardware FIFO refill (~0.2 ms each, ~1 ms per full drain).
struct SyncI2c(I2c<'static, Blocking>);

impl embedded_hal::i2c::ErrorType for SyncI2c {
    type Error = esp_hal::i2c::master::Error;
}

impl embedded_hal_async::i2c::I2c for SyncI2c {
    async fn transaction(
        &mut self,
        address: u8,
        operations: &mut [embedded_hal::i2c::Operation<'_>],
    ) -> Result<(), Self::Error> {
        embedded_hal::i2c::I2c::transaction(&mut self.0, address, operations)
    }
}
/// Shared across cores; every access is a short copy so the critical section stays tiny.
type Shared = &'static BlockingMutex<CriticalSectionRawMutex, RefCell<Ring>>;

/// Unix µs minus `Instant` µs. Instant runs off the 40 MHz crystal; the RTC's RC oscillator drifts too much.
static OFFSET_US: critical_section::Mutex<Cell<i64>> = critical_section::Mutex::new(Cell::new(0));
static SYNCED: Signal<CriticalSectionRawMutex, ()> = Signal::new();
static FIFO_OVERRUNS: AtomicU32 = AtomicU32::new(0);
// bytes on the wire and µs spent encoding; swapped into the 1 Hz health row
static TX_BYTES: AtomicU32 = AtomicU32::new(0);
static ENCODE_US: AtomicU32 = AtomicU32::new(0);
// debug: worst-case µs per 10 s window
static MAX_GAP_US: AtomicU32 = AtomicU32::new(0);
static MAX_BURST_US: AtomicU32 = AtomicU32::new(0);
static MAX_MAG_US: AtomicU32 = AtomicU32::new(0);

fn now_us() -> i64 {
    Instant::now().as_micros() as i64 + critical_section::with(|cs| OFFSET_US.borrow(cs).get())
}

#[esp_rtos::main]
async fn main(spawner: Spawner) -> ! {
    esp_println::logger::init_logger_from_env();
    let peripherals = esp_hal::init(esp_hal::Config::default().with_cpu_clock(CpuClock::max()));

    esp_alloc::heap_allocator!(#[esp_hal::ram(reclaimed)] size: 73744);
    let timg0 = TimerGroup::new(peripherals.TIMG0);
    esp_rtos::start(timg0.timer0, peripherals.FROM_CPU_INTR0);

    // PSRAM: HTTP body + TCP tx buffer up front, the rest is the ring.
    let psram = mk_static!(Psram, Psram::new(peripherals.PSRAM, Default::default()));
    let (ptr, size) = psram.raw_parts();
    // SAFETY: PSRAM is mapped for the program's lifetime and only handed out here, once.
    let mem: &'static mut [u8] = unsafe { core::slice::from_raw_parts_mut(ptr, size) };
    let (body, rest) = mem.split_at_mut(BODY_LEN);
    let (tcp_tx, rest) = rest.split_at_mut(TCP_TX_LEN);
    let (pre, slots, _) = unsafe { rest.align_to_mut::<MaybeUninit<Rec>>() };
    assert!(pre.is_empty());
    let (scratch, slots) = slots.split_at_mut(SCRATCH_RECS);
    let ring: Shared = mk_static!(
        BlockingMutex<CriticalSectionRawMutex, RefCell<Ring>>,
        BlockingMutex::new(RefCell::new(Ring::new(slots)))
    );
    let cap = ring.lock(|r| r.borrow().capacity());
    info!("ring: {cap} records (~{} s at 5.376 kHz)", slots_secs(cap));

    // STEMMA QT port power, then the bus at the LSM303AGR's 400 kHz max.
    let i2c_power = mk_static!(
        Output<'static>,
        Output::new(peripherals.GPIO7, Level::High, OutputConfig::default())
    );
    let _ = i2c_power;
    Timer::after_millis(10).await;
    // Sensors get core 1 to themselves: on core 0, WiFi and the sender stall the FIFO drain past 5.95 ms.
    let (i2c0, sda, scl) = (peripherals.I2C0, peripherals.GPIO3, peripherals.GPIO4);
    esp_rtos::start_second_core(
        peripherals.CPU_CTRL,
        peripherals.FROM_CPU_INTR1,
        mk_static!(CoreStack<16384>, CoreStack::new()),
        move || {
            // created on core 1 so only the sensor core ever touches the bus
            let i2c = I2c::new(
                i2c0,
                I2cConfig::default().with_frequency(Rate::from_khz(400)),
            )
            .unwrap()
            .with_sda(sda)
            .with_scl(scl);
            let bus: &'static Bus = mk_static!(Bus, Mutex::new(SyncI2c(i2c)));
            let executor = mk_static!(
                esp_rtos::embassy::Executor,
                esp_rtos::embassy::Executor::new()
            );
            executor.run(|spawner| {
                spawner.spawn(accel_task(bus, ring).unwrap());
            });
        },
    );

    // WiFi
    let station = Config::Station(
        StationConfig::default()
            .with_ssid(SSID.try_into().unwrap())
            .with_authentication(AuthenticationMethodConfig::Wpa2Personal(
                PASSWORD.try_into().unwrap(),
            )),
    );
    let mut controller = WifiController::new(
        peripherals.WIFI,
        ControllerConfig::default().with_initial_config(station),
    )
    .unwrap();
    controller.set_power_saving(PowerSaveMode::None).unwrap(); // modem sleep adds ~100 ms RTT
    let rng = Rng::new();
    let seed = (rng.random() as u64) << 32 | rng.random() as u64;
    let (stack, runner) = embassy_net::new(
        Interface::station(),
        embassy_net::Config::dhcpv4(Default::default()),
        mk_static!(StackResources<4>, StackResources::<4>::new()),
        seed,
    );

    spawner.spawn(connection(controller).unwrap());
    spawner.spawn(net_task(runner).unwrap());
    spawner.spawn(sntp_task(stack).unwrap());
    spawner.spawn(sender(stack, ring, body, tcp_tx, scratch).unwrap());

    loop {
        Timer::after_secs(10).await;
        let (len, dropped) = ring.lock(|r| (r.borrow().len(), r.borrow().dropped));
        info!(
            "ring {len}/{cap} dropped={dropped} fifo_overruns={} max_gap={}µs max_burst={}µs max_mag={}µs",
            FIFO_OVERRUNS.load(Relaxed),
            MAX_GAP_US.swap(0, Relaxed),
            MAX_BURST_US.swap(0, Relaxed),
            MAX_MAG_US.swap(0, Relaxed)
        );
    }
}

fn slots_secs(cap: usize) -> usize {
    cap * FIFO / 5376
}

#[embassy_executor::task]
async fn connection(mut controller: WifiController<'static>) {
    loop {
        match controller.connect_async().await {
            Ok(_) => {
                info!("wifi connected");
                let _ = controller.wait_for_disconnect_async().await;
                warn!("wifi disconnected");
            }
            Err(e) => error!("wifi connect failed: {e:?}"),
        }
        Timer::after_secs(5).await
    }
}

#[embassy_executor::task]
async fn net_task(mut runner: Runner<'static, Interface>) {
    runner.run().await
}

#[derive(Clone, Copy)]
struct NtpNow(i64);

impl NtpTimestampGenerator for NtpNow {
    fn init(&mut self) {
        self.0 = now_us();
    }
    fn timestamp_sec(&self) -> u64 {
        (self.0 / 1_000_000) as u64
    }
    fn timestamp_subsec_micros(&self) -> u32 {
        (self.0 % 1_000_000) as u32
    }
}

/// Slews nothing: each sync steps the offset. ponytail: steps are ~ms on a LAN, slew if that ever matters.
#[embassy_executor::task]
async fn sntp_task(stack: Stack<'static>) {
    let (mut rx_meta, mut tx_meta) = ([PacketMetadata::EMPTY; 4], [PacketMetadata::EMPTY; 4]);
    let (mut rx, mut tx) = ([0u8; 512], [0u8; 512]);
    stack.wait_config_up().await;
    let mut sock = UdpSocket::new(stack, &mut rx_meta, &mut rx, &mut tx_meta, &mut tx);
    sock.bind(123).unwrap();
    let sock = UdpSocketWrapper::new(sock);
    loop {
        let synced = match stack.dns_query(NTP_SERVER, DnsQueryType::A).await {
            Ok(addrs) if !addrs.is_empty() => {
                let server = SocketAddr::from((IpAddr::from(addrs[0]), 123));
                match get_time(server, &sock, NtpContext::new(NtpNow(0))).await {
                    Ok(t) => {
                        critical_section::with(|cs| {
                            let o = OFFSET_US.borrow(cs);
                            o.set(o.get() + t.offset());
                        });
                        info!(
                            "sntp: offset step {} µs, rtt {} µs",
                            t.offset(),
                            t.roundtrip()
                        );
                        SYNCED.signal(());
                        true
                    }
                    Err(e) => {
                        warn!("sntp: {e:?}");
                        false
                    }
                }
            }
            r => {
                warn!("sntp dns: {r:?}");
                false
            }
        };
        Timer::after_secs(if synced { 300 } else { 5 }).await;
    }
}

#[embassy_executor::task]
async fn accel_task(bus: &'static Bus, ring: Shared) {
    SYNCED.wait().await; // no timestamps before the clock is set

    let mut lsm = Lsm303agr::new_with_i2c(I2cDevice::new(bus));
    lsm.init().await.unwrap();
    lsm.set_accel_mode_and_odr(
        &mut Delay,
        AccelMode::LowPower,
        AccelOutputDataRate::Khz5_376LowPower,
    )
    .await
    .unwrap();
    lsm.set_accel_scale(AccelScale::G4).await.unwrap();
    lsm.acc_set_fifo_mode(FifoMode::Stream, 0).await.unwrap();
    let Ok(mut lsm) = lsm.into_mag_continuous().await else {
        panic!("mag continuous")
    };
    lsm.set_mag_mode_and_odr(
        &mut Delay,
        MagMode::HighResolution,
        MagOutputDataRate::Hz100,
    )
    .await
    .unwrap();
    info!("lsm303agr: accel 5376 Hz ±4g FIFO stream, mag 100 Hz");

    let mut raw = I2cDevice::new(bus);
    let mut buf = [0u8; FIFO * 6];
    let mut dt_ns = NOMINAL_DT_NS;
    let mut rate_from: Option<(i64, u64)> = None; // (first read µs, samples since)
    let mut chain_ns: Option<i64> = None; // timestamp of the newest sample, advanced by dt per sample
    // Drains average ~10 samples; fill ring records to all 32 so the ring holds ~3x more time.
    let (mut pend, mut pend_n, mut pend_t_ns) = ([[0i16; 3]; FIFO], 0usize, 0i64);
    let (mut mag_n, mut mag_t, mut mag_xyz) = (0, [0i64; MAG_BATCH], [[0i32; 3]; MAG_BATCH]);
    let mut next_temp = Instant::now();
    let mut next_mag = Instant::now();

    let mut last_src = Instant::now();
    loop {
        let now = Instant::now();
        MAX_GAP_US.fetch_max((now - last_src).as_micros() as u32, Relaxed);
        last_src = now;
        let mut src = [0u8];
        if raw
            .write_read(ACC, &[FIFO_SRC_REG_A], &mut src)
            .await
            .is_err()
        {
            continue;
        }
        let overrun = src[0] & 0x40 != 0;
        let n = if overrun {
            FIFO
        } else {
            (src[0] & 0x1F) as usize
        };
        if overrun {
            FIFO_OVERRUNS.fetch_add(1, Relaxed);
            rate_from = None; // lost samples would skew the rate estimate
            chain_ns = None; // and break the timestamp chain
        }
        if n > 0 {
            let t_us = now_us(); // before the burst: newest sample is at most one period old
            // FIFO mode rolls the auto-increment address back to OUT_X_L after OUT_Z_H, so one read drains n samples
            let b0 = Instant::now();
            let ok = raw
                .write_read(ACC, &[OUT_X_L_A | AUTO_INC], &mut buf[..n * 6])
                .await
                .is_ok();
            MAX_BURST_US.fetch_max(b0.elapsed().as_micros() as u32, Relaxed);
            if ok {
                // The chip's oscillator is off spec by a few %; measure the real sample period.
                match rate_from {
                    None => rate_from = Some((t_us, 0)),
                    Some((t0, ref mut count)) => {
                        *count += n as u64;
                        if t_us - t0 > 2_000_000 {
                            dt_ns = ((t_us - t0) as u64 * 1000 / *count) as u32;
                        }
                    }
                }
                // Read times jitter by ~±150 µs; chain samples dt apart and only nudge toward read time,
                // so timestamps stay evenly spaced and monotonic but can't drift from the real clock.
                let read_ns = t_us * 1000;
                let (newest, continuous) = match chain_ns {
                    Some(c) => {
                        let c = c + n as i64 * dt_ns as i64;
                        let err = read_ns - c;
                        if err.abs() > 2_000_000 {
                            (read_ns, false)
                        } else {
                            (c + err / 32, true)
                        }
                    }
                    None => (read_ns, false),
                };
                chain_ns = Some(newest);

                let push = |t_ns: i64, n: usize, xyz| {
                    ring.lock(|r| {
                        r.borrow_mut().push(Rec::Accel {
                            t_us: t_ns / 1000,
                            dt_ns,
                            n: n as u8,
                            xyz,
                        })
                    })
                };
                if !continuous && pend_n > 0 {
                    push(pend_t_ns, pend_n, pend); // a record must not span a chain break
                    pend_n = 0;
                }
                for (i, c) in buf[..n * 6].chunks_exact(6).enumerate() {
                    pend[pend_n] = [
                        i16::from_le_bytes([c[0], c[1]]),
                        i16::from_le_bytes([c[2], c[3]]),
                        i16::from_le_bytes([c[4], c[5]]),
                    ];
                    pend_n += 1;
                    pend_t_ns = newest - (n - 1 - i) as i64 * dt_ns as i64;
                    if pend_n == FIFO {
                        push(pend_t_ns, FIFO, pend);
                        pend_n = 0;
                    }
                }
            }
        }
        // Draining 32 samples takes ~4.4 ms of the 400 kHz bus and the FIFO fills in 5.95 ms: drain
        // early and often, and do the side reads right after a drain when there's the most headroom.
        // ponytail: polled; wire INT1 to a GPIO for FIFO watermark interrupts.
        if n < 4 {
            Timer::after_micros(500).await;
            continue;
        }

        let m0 = Instant::now();
        if m0 >= next_mag
            && let Ok(st) = lsm.mag_status().await
            && st.xyz_new_data()
            && let Ok(f) = lsm.magnetic_field().await
        {
            next_mag = Instant::now() + Duration::from_millis(5); // 100 Hz ODR; poll at 2x so no sample is missed
            let (x, y, z) = f.xyz_nt();
            mag_t[mag_n] = now_us();
            mag_xyz[mag_n] = [x, y, z];
            mag_n += 1;
            if mag_n == MAG_BATCH {
                ring.lock(|r| {
                    r.borrow_mut().push(Rec::Mag {
                        n: mag_n as u8,
                        t_us: mag_t,
                        xyz_nt: mag_xyz,
                    })
                });
                mag_n = 0;
            }
        }
        MAX_MAG_US.fetch_max(m0.elapsed().as_micros() as u32, Relaxed);

        if Instant::now() >= next_temp {
            next_temp += Duration::from_secs(1);
            let die_temp_c = lsm
                .temperature()
                .await
                .map(|t| t.degrees_celsius())
                .unwrap_or(f32::NAN);
            ring.lock(|r| {
                let mut r = r.borrow_mut();
                let dropped = r.dropped;
                r.push(Rec::Health {
                    t_us: now_us(),
                    die_temp_c,
                    fifo_overruns: FIFO_OVERRUNS.load(Relaxed),
                    dropped,
                    tx_bytes: TX_BYTES.swap(0, Relaxed),
                    encode_us: ENCODE_US.swap(0, Relaxed),
                });
            });
        }
    }
}

struct SliceWriter<'a> {
    buf: &'a mut [u8],
    len: usize,
}

impl core::fmt::Write for SliceWriter<'_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let end = self.len + s.len();
        self.buf
            .get_mut(self.len..end)
            .ok_or(core::fmt::Error)?
            .copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

/// Reads an HTTP/1.1 response head into `buf`: `(status code, head length, bytes read)`.
async fn read_head(
    sock: &mut TcpSocket<'_>,
    buf: &mut [u8],
) -> Result<(u16, usize, usize), embassy_net::tcp::Error> {
    let mut n = 0;
    loop {
        if n == buf.len() {
            return Err(embassy_net::tcp::Error::ConnectionReset);
        }
        let r = sock.read(&mut buf[n..]).await?;
        if r == 0 {
            return Err(embassy_net::tcp::Error::ConnectionReset);
        }
        n += r;
        if let Some(i) = buf[..n].windows(4).position(|w| w == b"\r\n\r\n") {
            let code = core::str::from_utf8(&buf[9..12])
                .ok()
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            return Ok((code, i + 4, n));
        }
    }
}

fn header<'a>(head: &'a [u8], name: &str) -> Option<&'a str> {
    core::str::from_utf8(head).ok()?.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

/// QWP over WebSocket: upgrade, then stream binary messages (up to MAX_INFLIGHT unacked). Records
/// are popped from the ring only when QuestDB acks the message that carried them; on reconnect
/// everything unacked is re-sent.
#[embassy_executor::task]
async fn sender(
    stack: Stack<'static>,
    ring: Shared,
    body: &'static mut [u8],
    tcp_tx: &'static mut [u8],
    scratch: &'static mut [MaybeUninit<Rec>],
) {
    const MAX_INFLIGHT: usize = 8;
    let questdb: SocketAddrV4 = QUESTDB.parse().expect("QUESTDB_ADDR must be ipv4:port");
    let rng = Rng::new();
    let mut tcp_rx = [0u8; 2048];
    let mut resp = [0u8; 512];
    stack.wait_config_up().await;
    loop {
        let mut sock = TcpSocket::new(stack, &mut tcp_rx, &mut *tcp_tx);
        sock.set_timeout(Some(Duration::from_secs(10)));
        let result: Result<(), &str> = async {
            sock.connect((*questdb.ip(), questdb.port()))
                .await
                .map_err(|_| "connect")?;
            let max_payload = qwp_upgrade(&mut sock, &mut resp, &rng)
                .await?
                .min(body.len() - 14);
            info!("qwp connected, max payload {max_payload} B");

            // record counts of unacked messages, oldest first; `sent` = their sum
            let (mut inflight, mut nin, mut sent, mut next_seq) =
                ([0usize; MAX_INFLIGHT], 0, 0, 0i64);
            let mut last_send = Instant::now();
            loop {
                let unsent = ring.lock(|r| r.borrow().len()) - sent;
                // batch like the reference client: ~100 ms or a full scratch of records
                let due = unsent >= SCRATCH_RECS / 4
                    || (unsent > 0 && last_send.elapsed() >= Duration::from_millis(100));
                if nin > 0 && (nin == MAX_INFLIGHT || !due || sock.can_recv()) {
                    let mut h = [0u8; 10];
                    sock.read_exact(&mut h[..2]).await.map_err(|_| "read")?;
                    let ext = match h[1] & 0x7F {
                        126 => 2,
                        127 => 8,
                        _ => 0,
                    };
                    sock.read_exact(&mut h[2..2 + ext])
                        .await
                        .map_err(|_| "read")?;
                    let (op, _, len) = qwp::ws_parse_header(&h).ok_or("frame header")?;
                    let take = len.min(resp.len());
                    sock.read_exact(&mut resp[..take])
                        .await
                        .map_err(|_| "read")?;
                    for _ in take..len {
                        sock.read_exact(&mut [0u8]).await.map_err(|_| "read")?; // never for QWP responses
                    }
                    match op {
                        qwp::WS_BINARY => match qwp::parse_response(&resp[..take]) {
                            Some((qwp::STATUS_DURABLE_ACK, ..)) => {}
                            Some((status, Some(seq), msg)) => {
                                if seq != next_seq {
                                    error!("qwp ack seq {seq}, expected {next_seq}");
                                    return Err("seq");
                                }
                                if status != qwp::STATUS_OK {
                                    let msg = core::str::from_utf8(msg).unwrap_or("");
                                    error!("qwp status {status} on seq {seq}: {msg}");
                                    // bad data never succeeds: drop it rather than wedge the ring
                                    if status != qwp::STATUS_PARSE_ERROR
                                        && status != qwp::STATUS_SCHEMA_MISMATCH
                                    {
                                        return Err("server error");
                                    }
                                }
                                next_seq += 1;
                                ring.lock(|r| r.borrow_mut().pop(inflight[0]));
                                sent -= inflight[0];
                                inflight.copy_within(1..nin, 0);
                                nin -= 1;
                            }
                            _ => return Err("bad response"),
                        },
                        qwp::WS_PING => {
                            let mask = rng.random().to_le_bytes();
                            let mut f = [0u8; 14];
                            let hl = qwp::ws_header(&mut f, qwp::WS_PONG, take, mask);
                            qwp::ws_mask(&mut resp[..take], mask);
                            sock.write_all(&f[..hl]).await.map_err(|_| "write")?;
                            sock.write_all(&resp[..take]).await.map_err(|_| "write")?;
                        }
                        qwp::WS_CLOSE => return Err("server closed"),
                        _ => {}
                    }
                    continue;
                }
                if !due {
                    Timer::after_millis(10).await;
                    continue;
                }

                let mut k = unsent.min(scratch.len());
                for (i, slot) in scratch[..k].iter_mut().enumerate() {
                    let r = ring.lock(|r| r.borrow().get(sent + i)).ok_or("ring")?;
                    slot.write(r);
                }
                // SAFETY: the first k slots were just written
                let recs =
                    unsafe { core::slice::from_raw_parts(scratch.as_ptr().cast::<Rec>(), k) };
                let t0 = Instant::now();
                let len = loop {
                    match s3_sensors::qwp_message(&mut body[14..14 + max_payload], &recs[..k]) {
                        Ok(n) => break n,
                        Err(_) if k > 1 => k /= 2,
                        Err(_) => return Err("record exceeds max payload"),
                    }
                };
                let mask = rng.random().to_le_bytes();
                qwp::ws_mask(&mut body[14..14 + len], mask);
                let mut f = [0u8; 14];
                let hl = qwp::ws_header(&mut f, qwp::WS_BINARY, len, mask);
                body[14 - hl..14].copy_from_slice(&f[..hl]);
                ENCODE_US.fetch_add(t0.elapsed().as_micros() as u32, Relaxed);
                sock.write_all(&body[14 - hl..14 + len])
                    .await
                    .map_err(|_| "write")?;
                TX_BYTES.fetch_add((hl + len) as u32, Relaxed);
                inflight[nin] = k;
                nin += 1;
                sent += k;
                last_send = Instant::now();
            }
        }
        .await;
        if let Err(e) = result {
            warn!("qwp: {e}, reconnecting");
        }
        sock.abort();
        let _ = sock.flush().await;
        Timer::after_secs(1).await;
    }
}

/// WebSocket upgrade to QWP; returns the payload budget (90% of the server's advertised max).
async fn qwp_upgrade(
    sock: &mut TcpSocket<'_>,
    buf: &mut [u8],
    rng: &Rng,
) -> Result<usize, &'static str> {
    let mut nonce = [0u8; 16];
    nonce
        .chunks_mut(4)
        .for_each(|c| c.copy_from_slice(&rng.random().to_le_bytes()));
    let key = qwp::ws_key(nonce);
    let mut req = [0u8; 256];
    let mut w = SliceWriter {
        buf: &mut req,
        len: 0,
    };
    write!(
        w,
        "GET /write/v4 HTTP/1.1\r\nHost: questdb\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\nX-QWP-Max-Version: 1\r\n\
         X-QWP-Client-Id: s3-sensors/{}\r\n\r\n",
        core::str::from_utf8(&key).unwrap(),
        env!("CARGO_PKG_VERSION")
    )
    .map_err(|_| "request too long")?;
    let n = w.len;
    sock.write_all(&req[..n]).await.map_err(|_| "write")?;
    // ponytail: Sec-WebSocket-Accept isn't verified (needs SHA-1); the server is a known QuestDB
    let (code, head_end, _) = read_head(sock, buf).await.map_err(|_| "read")?;
    let head = &buf[..head_end];
    if code != 101 {
        warn!("qwp upgrade: HTTP {code}");
        return Err("upgrade refused");
    }
    if header(head, "x-qwp-version") != Some("1") {
        return Err("unsupported QWP version");
    }
    let max = header(head, "x-qwp-max-batch-size")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_900_000);
    Ok(max / 10 * 9)
}
