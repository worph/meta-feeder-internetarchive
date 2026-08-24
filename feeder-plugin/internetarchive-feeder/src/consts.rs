//! Constants for `internetarchive-feeder`.

/// User-Agent for outbound HTTP.
///
/// ⚠ **Not cosmetic.** Several upstreams reject a generic or absent
/// User-Agent, and a 403 there looks exactly like a rate-limit — easy to
/// misdiagnose. Identify the application and carry contact information.
pub const USER_AGENT: &str = concat!(
    "internetarchive-feeder/",
    env!("CARGO_PKG_VERSION"),
    " ( https://github.com/worph/meta-feeder-internetarchive )"
);

/// Internet Archive advanced-search endpoint. Keyless.
pub const IA_SEARCH_URL: &str = "https://archive.org/advancedsearch.php";
/// Internet Archive per-item file-listing endpoint (`/metadata/<identifier>`).
pub const IA_METADATA_BASE: &str = "https://archive.org/metadata";
/// Internet Archive file-download base (`/download/<identifier>/<file>`).
pub const IA_DOWNLOAD_BASE: &str = "https://archive.org/download";
/// The archive's own thumbnail service, keyed by item identifier.
///
/// ⚠ **Without this the entire free tier is invisible.** The consumer's quality
/// gate requires a cover, and an archive item's artwork is an ordinary file in
/// its file list under an unpredictable name — so a first cut emitted no
/// artwork field at all and every free-tier track landed in the raw bucket. The
/// cold-start row, whose whole purpose is to be full on a client with nothing
/// configured, was permanently empty.
///
/// This endpoint always answers for a real item (verified: 200 image/png), so
/// it is the one predictable cover source the archive has.
pub const IA_THUMB_BASE: &str = "https://archive.org/services/img";
/// Sustained Internet Archive request rate. They publish no hard number and
/// ask for reasonable behaviour; this is deliberately modest because a single
/// item listing can be large.
pub const IA_RATE_PER_SEC: f64 = 3.0;
/// Internet Archive burst ceiling.
pub const IA_BURST: f64 = 6.0;
/// Request timeout for an Internet Archive metadata/search call (seconds).
pub const IA_TIMEOUT_SECS: u64 = 30;
/// Timeout for downloading one audio file's bytes (seconds). Generous: a
/// lossless live-set track can be 100 MB.
pub const IA_FETCH_TIMEOUT_SECS: u64 = 600;
/// Hard ceiling on the bytes this feeder will pull for a single track.
///
/// The gateway core holds the whole outcome in memory before seeding it, so an
/// unbounded fetch is an OOM waiting to happen on a 128 MB feeder. A 200 MB
/// ceiling clears any normal lossless track (a 24/96 FLAC of a 10-minute piece
/// is ~200 MB at the extreme) while refusing a whole-concert single file.
pub const IA_MAX_TRACK_BYTES: u64 = 200 * 1024 * 1024;
/// The Internet Archive collections this feeder searches.
///
/// Deliberately an allow-list rather than "all of `mediatype:audio`". The
/// archive holds a great deal of audio whose redistribution status is unclear
/// (uploads of commercial recordings, radio airchecks). These four are the
/// collections whose licensing is either explicit per item or structurally
/// clean, which is what makes the `licence` field on the emitted record
/// meaningful rather than decorative:
///
/// - `etree` — the Live Music Archive: taper recordings of bands who have
///   explicitly permitted taping and distribution.
/// - `netlabels` — net-label releases, uniformly Creative Commons.
/// - `audio_music` — the general music collection; per-item `licenseurl` is
///   what gates an individual record (see `internetarchive.rs`).
/// - `78rpm` — digitised 78s, overwhelmingly public domain by age.
pub const IA_COLLECTIONS: &[&str] = &["etree", "netlabels", "audio_music", "78rpm"];
/// Archive `mediatype` values that hold music.
///
/// ⚠ **`etree` is a mediatype, not only a collection**, and missing that is the
/// easiest way to silently lose most of the free tier. Live Music Archive items
/// are `mediatype:etree`; a query restricted to `mediatype:(audio)` matches
/// **zero** of them. Measured on the live endpoint:
///
/// | query | numFound |
/// |---|---|
/// | `mediatype:(audio) AND collection:(etree)` | **0** |
/// | `mediatype:(etree)` | 293,738 |
/// | `collection:(etree) AND creator:("Grateful Dead")` | 18,334 |
///
/// The failure is quiet — an `audio`-only query still returns a handful of
/// `audio_music` items, so the search looks like it works and merely appears to
/// have a thin catalogue.
pub const IA_MEDIATYPES: &[&str] = &["audio", "etree"];
/// Audio file extensions the Internet Archive tier will emit as tracks,
/// preferred order (best quality first). The archive stores several
/// derivatives of the same recording; emitting all of them would publish four
/// records for one track.
pub const IA_AUDIO_EXTS: &[&str] = &["flac", "ogg", "opus", "m4a", "mp3"];
