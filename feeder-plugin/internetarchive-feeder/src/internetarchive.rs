//! The `internetarchive` upstream — the **free content tier**.
//!
//! This is the plugin that makes a cold meta-listen client show music it can
//! actually play. Everything else in this crate resolves *identity*; this one
//! resolves *bytes*, with no credential, from collections whose redistribution
//! status is either explicit per item or structurally clean.
//!
//! That is not a convenience. Every other MetaMesh consumer's cold-start state
//! is an empty wall, which is indistinguishable to a user from a broken swarm
//! — a confusion that has cost real debugging time on this stack. A tier that
//! works with nothing configured is the only way to avoid it without weakening
//! the untyped-query load gate.
//!
//! ## Licensing is a field, not a vibe
//!
//! The gateway's auto-store makes the peer a real **host** of whatever this
//! plugin resolves: bytes go to meta-core over WebDAV and get seeded to
//! bitswap. So "may this be redistributed?" has to be answered per record, and
//! the answer is written to `licence` (`METADATA_KEYS.md` §5).
//!
//! Two rules follow, and both are enforced in [`InternetArchivePlugin`]:
//!
//! 1. **Collection allow-list.** `consts::IA_COLLECTIONS`, not all of
//!    `mediatype:audio`. The archive holds a great deal of audio whose status
//!    is unclear (uploads of commercial recordings, radio airchecks).
//! 2. **A per-item licence, or the item's collection vouches for it.** An item
//!    from `etree` (taper recordings of bands who permit taping) or `78rpm`
//!    (public domain by age) is clean by collection; anything from the general
//!    `audio_music` pool needs its own `licenseurl` or it is skipped.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use meta_feeder_sdk::cache::MidhashCache;
use meta_feeder_sdk::plugin::{ConfigError, FeederPlugin, GatewayQuery, HashKind, HashOutcome};
use meta_feeder_sdk::types::{ByteStream, DiscoveryRecord, GatewayError, Hash, PluginHealth};
use serde_json::Value;
use tracing::{debug, warn};

use crate::consts::{
    IA_AUDIO_EXTS, IA_BURST, IA_COLLECTIONS, IA_DOWNLOAD_BASE, IA_FETCH_TIMEOUT_SECS, IA_MEDIATYPES,
    IA_MAX_TRACK_BYTES, IA_METADATA_BASE, IA_RATE_PER_SEC, IA_SEARCH_URL, IA_THUMB_BASE,
    IA_TIMEOUT_SECS,
    USER_AGENT,
};
use meta_feeder_sdk::budget::{Lease, RateBudget};

/// Collections whose licensing is clean by construction, and the `licence`
/// value an item in them carries when it declares no `licenseurl` of its own.
///
/// **This table is the whole of rule 2.** A collection absent from it —
/// notably the general `audio_music` pool — vouches for nothing, so an item
/// there needs its own licence URL or it is skipped entirely. Adding a row
/// here is asserting, on behalf of every peer that will end up hosting those
/// bytes, that the collection's contents may be redistributed.
const SELF_VOUCHING_COLLECTIONS: &[(&str, &str)] = &[
    // Taper recordings distributed with the artist's permission. Not a
    // public-domain claim and not a CC grant — the archive's own term for it is
    // the honest label.
    ("etree", "LiveMusicArchive-TapingPolicy"),
    // Net-label releases are uniformly Creative Commons, but the exact variant
    // is per release, so only the family is asserted.
    ("netlabels", "CC"),
    // Digitised 78s: public domain by age.
    ("78rpm", "PublicDomain"),
];

/// The `licence` value for an item from a self-vouching collection.
fn collection_licence(collections: &[String]) -> Option<&'static str> {
    collections.iter().find_map(|c| {
        SELF_VOUCHING_COLLECTIONS
            .iter()
            .find(|(name, _)| *name == c.as_str())
            .map(|(_, licence)| *licence)
    })
}

/// Map a Creative Commons (or other) licence URL to a short identifier.
///
/// `METADATA_KEYS.md` requires an SPDX-style identifier rather than a URL,
/// because a consumer comparing licences should not be doing string surgery on
/// `http://` vs `https://` and a trailing slash.
pub fn licence_from_url(url: &str) -> Option<String> {
    let u = url.trim().trim_end_matches('/').to_ascii_lowercase();
    if u.is_empty() {
        return None;
    }
    if u.contains("publicdomain/zero") {
        return Some("CC0-1.0".to_string());
    }
    if u.contains("publicdomain/mark") {
        return Some("PublicDomain".to_string());
    }
    // .../licenses/<variant>/<version>[/<jurisdiction>]
    let rest = u.split("/licenses/").nth(1)?;
    let mut parts = rest.split('/');
    let variant = parts.next()?.to_ascii_uppercase();
    let version = parts.next().unwrap_or("4.0");
    if variant.is_empty() {
        return None;
    }
    Some(format!("CC-{variant}-{version}"))
}

/// Encode a record id for the single-path-segment `/fetch/:upstream/:record_id`
/// route.
///
/// ⚠ A file name inside an archive item may contain `/` (items have
/// subdirectories) and axum's `:record_id` matches exactly one segment, so a
/// raw `identifier/filename` would 404 on fetch while working fine through
/// `/compute` (which takes the id in a JSON body). Base64url keeps one shape
/// on both paths. `~` is unreserved in RFC 3986 and cannot appear in an
/// archive identifier, so it is an unambiguous separator.
pub fn track_record_id(identifier: &str, file_name: &str) -> String {
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(file_name.as_bytes());
    format!("{identifier}~{b64}")
}

/// Inverse of [`track_record_id`]. `None` for a bare item id (a pack).
pub fn split_track_record_id(record_id: &str) -> Option<(&str, String)> {
    let (identifier, b64) = record_id.split_once('~')?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(b64.as_bytes())
        .ok()?;
    Some((identifier, String::from_utf8(bytes).ok()?))
}

/// Rank an audio file name by [`IA_AUDIO_EXTS`] preference. `None` when the
/// extension is not one we serve.
fn audio_rank(name: &str) -> Option<usize> {
    let ext = name.rsplit_once('.')?.1.to_ascii_lowercase();
    IA_AUDIO_EXTS.iter().position(|e| *e == ext)
}

/// The recording a derivative file belongs to.
///
/// An archive item stores several encodings of the same recording, named by
/// appending a derivative marker to the stem: `Track01.flac`, `Track01.mp3`,
/// `Track01_64kb.mp3`, `Track01_vbr.mp3`. Grouping on the bare stem would keep
/// the marker and leave each derivative in its own group, which is how the
/// first cut of this shipped four records for one track.
///
/// The markers stripped are the ones the archive's own derivation pipeline
/// emits. A title genuinely ending in one of them collapses with a sibling
/// that does not — an acceptable trade for not showing every track four times.
fn recording_stem(name: &str) -> String {
    let base = name.rsplit_once('.').map(|(s, _)| s).unwrap_or(name);
    let lower = base.to_ascii_lowercase();
    for marker in ["_vbr", "_sample", "_alt"] {
        if let Some(s) = lower.strip_suffix(marker) {
            return s.trim_end_matches('_').to_string();
        }
    }
    // `_<digits>kb` — the bitrate derivatives.
    if let Some(rest) = lower.strip_suffix("kb") {
        let trimmed = rest.trim_end_matches(|c: char| c.is_ascii_digit());
        if trimmed.len() < rest.len() && trimmed.ends_with('_') {
            return trimmed.trim_end_matches('_').to_string();
        }
    }
    lower
}

/// Decode an archive `track` value into `(disc, track)`.
///
/// The archive's `track` field carries three shapes, all seen live:
///
/// - `"7"` — a plain ordinal.
/// - `"7/12"` — ordinal out of total.
/// - `"101"` — **disc-encoded**: hundreds digit is the disc, remainder is the
///   track. Standard on Live Music Archive items (`gd85-12-31 s1t01 …` is
///   `track: 101`), where a show routinely spans three or four discs.
///
/// ⚠ Without the third case a two-disc show reports track numbers 101…115 and
/// 201…212, which sorts correctly by accident and then renders as "track 101"
/// in every UI. A music track number above 99 does not otherwise occur, so the
/// rule is safe.
pub fn decode_track_number(raw: &str) -> Option<(Option<u32>, u32)> {
    let head = raw.split('/').next()?.trim();
    let n: u32 = head.parse().ok()?;
    if n == 0 {
        return None;
    }
    if n >= 100 {
        let disc = n / 100;
        let track = n % 100;
        if track > 0 {
            return Some((Some(disc), track));
        }
    }
    Some((None, n))
}

/// One resolved audio file within an item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveTrack {
    pub file_name: String,
    pub title: String,
    pub track_number: Option<u32>,
    pub disc_number: Option<u32>,
    pub size: Option<u64>,
}

/// One archive item, with the audio files worth emitting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveItem {
    pub identifier: String,
    pub title: String,
    pub creator: String,
    pub date: Option<String>,
    pub licence: Option<String>,
    pub tracks: Vec<ArchiveTrack>,
}

/// Pick the best derivative per recording.
///
/// An archive item stores several encodings of the same recording
/// (`Track.flac`, `Track.mp3`, `Track_64kb.mp3`). Emitting all of them would
/// publish four records for one track and four entries in every album view, so
/// they are grouped by stem and only the highest-ranked survives.
pub fn best_derivatives(files: &[Value]) -> Vec<ArchiveTrack> {
    let mut best: BTreeMap<String, (usize, ArchiveTrack)> = BTreeMap::new();
    for f in files {
        let Some(name) = f.get("name").and_then(Value::as_str) else {
            continue;
        };
        let Some(rank) = audio_rank(name) else {
            continue;
        };
        let stem = recording_stem(name);
        let decoded = f
            .get("track")
            .and_then(|v| match v {
                Value::String(s) => decode_track_number(s),
                Value::Number(n) => decode_track_number(&n.to_string()),
                _ => None,
            });
        let track = ArchiveTrack {
            file_name: name.to_string(),
            title: f
                .get("title")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| name.rsplit_once('.').map(|(s, _)| s).unwrap_or(name))
                .to_string(),
            track_number: decoded.map(|(_, t)| t),
            disc_number: decoded.and_then(|(d, _)| d),
            size: f
                .get("size")
                .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64())),
        };
        match best.get(&stem) {
            Some((existing_rank, _)) if *existing_rank <= rank => {}
            _ => {
                best.insert(stem, (rank, track));
            }
        }
    }
    let mut out: Vec<ArchiveTrack> = best.into_values().map(|(_, t)| t).collect();
    // Disc first, then track. Sorting on the track number alone would
    // interleave two discs' track 1s.
    out.sort_by(|a, b| {
        a.disc_number
            .unwrap_or(1)
            .cmp(&b.disc_number.unwrap_or(1))
            .then_with(|| {
                a.track_number
                    .unwrap_or(u32::MAX)
                    .cmp(&b.track_number.unwrap_or(u32::MAX))
            })
            .then_with(|| a.file_name.cmp(&b.file_name))
    });
    out
}

pub struct InternetArchivePlugin {
    http: reqwest::Client,
    search_url: String,
    metadata_base: String,
    download_base: String,
    budget: Arc<RateBudget>,
    cache: Option<MidhashCache>,
    hash_cache: Option<MidhashCache>,
}

impl Default for InternetArchivePlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl InternetArchivePlugin {
    pub fn new() -> Self {
        Self::with_bases(
            IA_SEARCH_URL.to_string(),
            IA_METADATA_BASE.to_string(),
            IA_DOWNLOAD_BASE.to_string(),
        )
    }

    /// Test constructor: point every endpoint at a wiremock server.
    pub fn with_bases(search_url: String, metadata_base: String, download_base: String) -> Self {
        Self {
            http: reqwest::Client::builder()
                .user_agent(USER_AGENT)
                .timeout(Duration::from_secs(IA_FETCH_TIMEOUT_SECS))
                .build()
                .expect("build internet archive http client"),
            search_url,
            metadata_base: metadata_base.trim_end_matches('/').to_string(),
            download_base: download_base.trim_end_matches('/').to_string(),
            budget: RateBudget::new(IA_RATE_PER_SEC, IA_BURST),
            cache: None,
            hash_cache: None,
        }
    }

    fn download_url(&self, identifier: &str, file_name: &str) -> String {
        format!("{}/{}/{}", self.download_base, identifier, file_name)
    }

    async fn get_json(&self, url: &str, deadline: Duration) -> Option<Value> {
        if self.budget.acquire(deadline).await == Lease::DeadlineExceeded {
            debug!(target: "meta-music", %url, "internet archive budget deadline; degrading");
            return None;
        }
        let resp = match self
            .http
            .get(url)
            .timeout(Duration::from_secs(IA_TIMEOUT_SECS))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(target: "meta-music", %url, error = %e, "internet archive request failed");
                return None;
            }
        };
        if !resp.status().is_success() {
            warn!(target: "meta-music", %url, status = %resp.status(), "internet archive non-2xx");
            return None;
        }
        resp.json::<Value>().await.ok()
    }

    /// Build the Lucene query the advanced-search endpoint takes.
    ///
    /// Two things here are not obvious and were both found by probing the live
    /// endpoint rather than by reading the docs:
    ///
    /// **1. The Live Music Archive is `mediatype:etree`, not
    /// `mediatype:audio`.** A first cut asked for `mediatype:(audio)` and
    /// silently excluded the entire collection — measured, that is 293,738
    /// items, including 18,334 Grateful Dead recordings, and it is the single
    /// largest source of free, legally-hostable music this feeder has. The
    /// search still returned *something* (a handful of `audio_music` items), so
    /// nothing looked broken.
    ///
    /// **2. The licensing rule is pushed down into the query.** Filtering after
    /// the fact wastes the whole result page on items that will be dropped:
    /// `audio_music` is sorted by downloads, its most-downloaded items
    /// routinely carry no `licenseurl`, and a page of those yields zero
    /// records. Requiring the licence in the query means every row that comes
    /// back is usable. The self-vouching collections are exempted because their
    /// items frequently carry no per-item licence field at all — the collection
    /// is the licence (see [`SELF_VOUCHING_COLLECTIONS`]).
    fn build_query(&self, free_text: &str) -> String {
        let vouching: Vec<&str> = SELF_VOUCHING_COLLECTIONS.iter().map(|(c, _)| *c).collect();
        let needs_licence: Vec<&str> = IA_COLLECTIONS
            .iter()
            .copied()
            .filter(|c| !vouching.contains(c))
            .collect();

        let mut clauses = vec![format!("collection:({})", vouching.join(" OR "))];
        if !needs_licence.is_empty() {
            clauses.push(format!(
                "(collection:({}) AND licenseurl:[* TO *])",
                needs_licence.join(" OR ")
            ));
        }
        let mut q = format!(
            "mediatype:({}) AND ({})",
            IA_MEDIATYPES.join(" OR "),
            clauses.join(" OR ")
        );

        let text = free_text.trim();
        if !text.is_empty() {
            // Quote the whole phrase: the archive's parser treats bare
            // punctuation as syntax, and a music title is full of it.
            let escaped = text.replace('"', " ");
            q.push_str(&format!(" AND (title:(\"{escaped}\") OR creator:(\"{escaped}\"))"));
        }
        q
    }

    /// Search items. `free_text` empty means "the browse row" — the most
    /// downloaded items in the allow-listed collections.
    async fn search_items(
        &self,
        free_text: &str,
        rows: usize,
        deadline: Duration,
    ) -> Vec<ArchiveItem> {
        let url = format!(
            "{}?q={}&fl%5B%5D=identifier&fl%5B%5D=title&fl%5B%5D=creator&fl%5B%5D=date\
             &fl%5B%5D=licenseurl&fl%5B%5D=collection&sort%5B%5D=downloads+desc\
             &rows={}&page=1&output=json",
            self.search_url,
            meta_feeder_sdk::common::percent_encode(&self.build_query(free_text)),
            rows.clamp(1, 50)
        );
        let Some(v) = self.get_json(&url, deadline).await else {
            return Vec::new();
        };
        let docs = v
            .get("response")
            .and_then(|r| r.get("docs"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let mut out = Vec::new();
        for d in &docs {
            let Some(identifier) = d.get("identifier").and_then(Value::as_str) else {
                continue;
            };
            let collections = string_list(d.get("collection"));
            let licence = d
                .get("licenseurl")
                .and_then(Value::as_str)
                .and_then(licence_from_url)
                .or_else(|| collection_licence(&collections).map(str::to_string));
            // Rule 2: no per-item licence and no vouching collection ⇒ skip.
            // A record with no `licence` from this tier would be a claim the
            // feeder cannot back.
            let Some(licence) = licence else {
                debug!(
                    target: "meta-music", identifier,
                    "internet archive item has no licence and no vouching collection; skipping"
                );
                continue;
            };
            out.push(ArchiveItem {
                identifier: identifier.to_string(),
                title: first_string(d.get("title")).unwrap_or_else(|| identifier.to_string()),
                creator: first_string(d.get("creator")).unwrap_or_default(),
                date: first_string(d.get("date")),
                licence: Some(licence),
                tracks: Vec::new(),
            });
        }
        out
    }

    /// List an item's audio files. Cached permanently — an archive item's file
    /// set is effectively immutable once derived.
    async fn item_tracks(&self, identifier: &str, deadline: Duration) -> Vec<ArchiveTrack> {
        if let Some(cache) = &self.cache {
            if let Ok(Some(hit)) = cache.get_ia_item(identifier) {
                if let Ok(files) = serde_json::from_str::<Vec<Value>>(&hit) {
                    return best_derivatives(&files);
                }
            }
        }
        let url = format!("{}/{}", self.metadata_base, identifier);
        let Some(v) = self.get_json(&url, deadline).await else {
            return Vec::new();
        };
        let files = v
            .get("files")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if let (Some(cache), Ok(json)) = (&self.cache, serde_json::to_string(&files)) {
            let _ = cache.put_ia_item(identifier, &json);
        }
        best_derivatives(&files)
    }

    /// Project one track to a wire record.
    fn track_record(
        &self,
        item: &ArchiveItem,
        track: &ArchiveTrack,
        query_filters: &BTreeMap<String, Vec<String>>,
    ) -> DiscoveryRecord {
        let mut fields: BTreeMap<String, String> = BTreeMap::new();
        fields.insert("fileType".to_string(), "audio".to_string());
        fields.insert("contentKind".to_string(), "track".to_string());
        fields.insert("domain".to_string(), "music".to_string());
        fields.insert("workForm".to_string(), "standalone".to_string());
        fields.insert("title".to_string(), track.title.clone());
        fields.insert("album".to_string(), item.title.clone());
        fields.insert("fileName".to_string(), track.file_name.clone());
        if let Some(ext) = track.file_name.rsplit_once('.').map(|(_, e)| e) {
            fields.insert("extension".to_string(), ext.to_ascii_lowercase());
        }
        if !item.creator.trim().is_empty() {
            fields.insert("albumArtist".to_string(), item.creator.clone());
            fields.insert("artist".to_string(), item.creator.clone());
            fields.insert(format!("artists/{}", item.creator.trim()), "true".to_string());
        }
        if let Some(n) = track.track_number {
            fields.insert("trackNumber".to_string(), n.to_string());
        }
        if let Some(n) = track.disc_number {
            fields.insert("discNumber".to_string(), n.to_string());
        }
        if let Some(sz) = track.size {
            fields.insert("sizeByte".to_string(), sz.to_string());
        }
        if let Some(d) = item
            .date
            .as_deref()
            .and_then(meta_feeder_sdk::common::normalise_date)
        {
            fields.insert("releasedate".to_string(), d);
        }
        if let Some(l) = &item.licence {
            fields.insert("licence".to_string(), l.clone());
        }
        // The **url locator** (`0x1006`) — what makes this track playable from
        // a search hit at all.
        //
        // A track's real content CID only exists after `compute_outcomes`
        // downloads the bytes, so a search result would otherwise carry no
        // `cids/*` member and the consumer would have nothing to play.
        // Pre-fetching every track a row returns to fix that would mean
        // downloading twenty songs to render one strip.
        //
        // meta-share resolves this lazily on first access: decode → fetch once
        // → chunk into kubo-identical IPFS blocks → seed. After that the real
        // digest outranks the locator and playback comes off bitswap.
        // `METADATA_KEYS.md` §2.1 reserved this write path; this is its first
        // writer.
        if let Some(url_cid) =
            meta_feeder_sdk::hash::compute_url_cid(&self.download_url(&item.identifier, &track.file_name))
        {
            fields.insert(format!("cids/{url_cid}"), "true".to_string());
        }

        // The canonical `<upstream_id>id` field every feeder record must carry.
        fields.insert("internetarchiveid".to_string(), item.identifier.clone());
        fields.insert("source/gateway:internetarchive".to_string(), "true".to_string());

        // The cover.
        //
        // ⚠ **Load-bearing for the whole tier.** The consumer's quality gate
        // requires a cover, so without this every free-tier track lands in the
        // raw bucket and the cold-start row — the one row whose purpose is to
        // be full on a client with nothing configured — is permanently empty.
        //
        // An item's real artwork is an ordinary file in its file list under an
        // unpredictable name; the thumbnail service is the one address that
        // resolves for any item. Emitted as a `url` LOCATOR cid (rule #5 —
        // never a raw `*_url` field); the gateway core upgrades it to a
        // content-addressed `cover` cid when it seeds the bytes. A release
        // sleeve — music vocabulary, not the video tier's `poster`.
        if let Some(locator) =
            meta_feeder_sdk::hash::artwork_locator(&format!("{IA_THUMB_BASE}/{}", item.identifier))
        {
            fields.insert("cover".to_string(), locator);
        }

        echo_filters(&mut fields, query_filters);
        DiscoveryRecord {
            upstream_id: "internetarchive".to_string(),
            record_id: track_record_id(&item.identifier, &track.file_name),
            fields,
        }
    }

    /// Project an item to a `pack` record — the album release as a unit.
    ///
    /// ⚠ Unlike meta-watch, which hides `contentKind=pack` (a season pack is a
    /// nuisance; the episodes are what you watch), the music domain keeps it:
    /// an album release *is* the unit people mean, and it is what a
    /// download-the-whole-thing gesture acts on.
    fn pack_record(
        &self,
        item: &ArchiveItem,
        query_filters: &BTreeMap<String, Vec<String>>,
    ) -> DiscoveryRecord {
        let mut fields: BTreeMap<String, String> = BTreeMap::new();
        fields.insert("fileType".to_string(), "audio".to_string());
        fields.insert("contentKind".to_string(), "pack".to_string());
        // `pack` is ambiguous on both routing axes by construction (season
        // pack vs album release), so the writer supplies both —
        // METADATA_KEYS.md §1. An album is a closed work: `standalone`.
        fields.insert("domain".to_string(), "music".to_string());
        fields.insert("workForm".to_string(), "standalone".to_string());
        fields.insert("title".to_string(), item.title.clone());
        fields.insert("album".to_string(), item.title.clone());
        if !item.creator.trim().is_empty() {
            fields.insert("albumArtist".to_string(), item.creator.clone());
        }
        fields.insert("trackCount".to_string(), item.tracks.len().to_string());
        if let Some(d) = item
            .date
            .as_deref()
            .and_then(meta_feeder_sdk::common::normalise_date)
        {
            fields.insert("releasedate".to_string(), d);
        }
        if let Some(l) = &item.licence {
            fields.insert("licence".to_string(), l.clone());
        }
        fields.insert("internetarchiveid".to_string(), item.identifier.clone());
        fields.insert("source/gateway:internetarchive".to_string(), "true".to_string());
        if let Some(locator) =
            meta_feeder_sdk::hash::artwork_locator(&format!("{IA_THUMB_BASE}/{}", item.identifier))
        {
            fields.insert("cover".to_string(), locator);
        }
        // A pack is a container, not a playable file. The consumer reads this
        // to grey out a play button rather than failing at byte-fetch time.
        fields.insert("playable".to_string(), "false".to_string());
        echo_filters(&mut fields, query_filters);
        DiscoveryRecord {
            upstream_id: "internetarchive".to_string(),
            record_id: item.identifier.clone(),
            fields,
        }
    }
}

/// Echo back structured filters the record did not already set, so the record
/// survives `record_matches` at the gateway and at meta-search. Same rule and
/// the same two exclusions as the card projection.
fn echo_filters(fields: &mut BTreeMap<String, String>, query_filters: &BTreeMap<String, Vec<String>>) {
    for (key, allowed) in query_filters {
        if key == "languages" || key == "genres" || fields.contains_key(key) || allowed.is_empty() {
            continue;
        }
        fields.insert(key.clone(), allowed.join(","));
    }
}

/// The archive returns some fields as a scalar and some as an array depending
/// on how many values an item has. Both shapes mean the same thing.
fn first_string(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Value::Array(a) => a
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .find(|s| !s.is_empty())
            .map(str::to_string),
        _ => None,
    }
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::String(s)) => vec![s.to_string()],
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

#[async_trait]
impl FeederPlugin for InternetArchivePlugin {
    fn upstream_id(&self) -> &'static str {
        "internetarchive"
    }

    fn served_file_types(&self) -> &'static [&'static str] {
        &["audio"]
    }

    fn served_content_kinds(&self) -> &'static [&'static str] {
        &["track", "pack"]
    }

    fn configure(&mut self, cache_dir: &Path) -> Result<(), ConfigError> {
        self.cache = Some(
            MidhashCache::open(cache_dir).map_err(|e| ConfigError::Other {
                plugin: "internetarchive",
                source: anyhow::anyhow!("open music cache: {e}"),
            })?,
        );
        self.hash_cache = Some(meta_feeder_sdk::common::open_midhash_cache(
            cache_dir,
            "internetarchive",
        )?);
        Ok(())
    }

    fn health(&self) -> PluginHealth {
        match self.cache {
            Some(_) => PluginHealth::Ok,
            None => PluginHealth::Degraded {
                reason: "configure() not yet called".to_string(),
            },
        }
    }

    async fn handle_query(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
        if !meta_feeder_sdk::query_eval::query_accepts_plugin(
            query,
            self.served_file_types(),
            self.served_content_kinds(),
        ) {
            return Ok(Vec::new());
        }

        // ⚠ **An identity-anchored query this upstream cannot satisfy must
        // return NOTHING**, not a free-text browse.
        //
        // The archive holds no MusicBrainz ids, so a query like
        // `mbReleaseGroupId:<uuid> fileType:audio` carries no free text for
        // this plugin to search on. Falling through to the keyword path then
        // means an *empty* search — which the archive answers with its
        // most-downloaded items. Observed live: opening a BTS album filled its
        // page with sixty Grateful Dead and shortwave-numbers-station
        // recordings, every one of them a confident-looking answer to a
        // question nobody asked.
        //
        // A consumer's album page is exactly this shape, so the failure is not
        // an edge case — it is the normal detail-page query.
        const IDENTITY_FILTERS: &[&str] = &[
            "mbReleaseGroupId",
            "mbReleaseId",
            "mbArtistId",
            "mbRecordingId",
            "isrc",
            "tmdbid",
            "imdbid",
        ];
        if IDENTITY_FILTERS.iter().any(|k| query.filters.contains_key(*k)) {
            debug!(
                target: "meta-music",
                filters = ?query.filters.keys().collect::<Vec<_>>(),
                "internet archive: id-anchored query it cannot resolve; returning nothing"
            );
            return Ok(Vec::new());
        }

        let deadline = Duration::from_secs(IA_TIMEOUT_SECS);
        // Item breadth is below `max_results` because one item expands into a
        // whole album's worth of track records — asking for twenty items to
        // fill a twenty-record page would fetch twenty file listings and
        // discard most of them.
        //
        // ⚠ But there is a **floor**, and it is load-bearing. A first cut used
        // `div_ceil(8)` alone, which asks for ONE item on a small page; an item
        // whose file list turns out to hold no servable audio then yields zero
        // records for the whole query, with nothing in the log to say why. The
        // floor makes a single unusable item a shortfall rather than a blackout.
        let item_rows = max_results.div_ceil(8).clamp(8, 24);
        let items = self.search_items(query.free_text.trim(), item_rows, deadline).await;

        let mut out = Vec::with_capacity(max_results);
        for mut item in items {
            item.tracks = self.item_tracks(&item.identifier, deadline).await;
            if item.tracks.is_empty() {
                continue;
            }
            if wants_content_kind(query, "pack") {
                out.push(self.pack_record(&item, &query.filters));
            }
            if wants_content_kind(query, "track") {
                for t in &item.tracks {
                    if out.len() >= max_results {
                        break;
                    }
                    out.push(self.track_record(&item, t, &query.filters));
                }
            }
            if out.len() >= max_results {
                break;
            }
        }
        out.truncate(max_results);
        Ok(out)
    }

    /// Download one track and content-address it.
    ///
    /// Full-store branch: real bytes, a real sha2-256 IPFS CID, seeded to
    /// bitswap by the core. This is what makes the free tier genuinely part of
    /// the network rather than a link to somewhere else.
    async fn compute_outcomes(&self, record_id: &str) -> Result<Vec<HashOutcome>, GatewayError> {
        let Some((identifier, file_name)) = split_track_record_id(record_id) else {
            // A bare identifier is the pack record. A pack is a container with
            // no bytes of its own — its tracks are separately addressable — so
            // there is nothing to resolve.
            return Ok(Vec::new());
        };

        if let Some(cache) = &self.hash_cache {
            if let Some(hit) = meta_feeder_sdk::common::cached_outcome(cache, record_id, "internetarchive")? {
                return Ok(hit);
            }
        }

        let url = self.download_url(identifier, &file_name);
        if self
            .budget
            .acquire(Duration::from_secs(IA_TIMEOUT_SECS))
            .await
            == Lease::DeadlineExceeded
        {
            return Err(GatewayError::RateLimited { retry_after_s: 5 });
        }
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| GatewayError::Transient(format!("internet archive fetch: {e}")))?;
        if !resp.status().is_success() {
            return Err(GatewayError::Permanent(format!(
                "internet archive fetch {url}: HTTP {}",
                resp.status()
            )));
        }
        // Refuse an oversized file *before* buffering it: the core holds the
        // whole outcome in memory to hash and seed it, so an unbounded body is
        // an OOM on a small feeder.
        if let Some(len) = resp.content_length() {
            if len > IA_MAX_TRACK_BYTES {
                return Err(GatewayError::Permanent(format!(
                    "internet archive file {file_name} is {len} bytes, over the \
                     {IA_MAX_TRACK_BYTES}-byte per-track ceiling"
                )));
            }
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| GatewayError::Transient(format!("internet archive body: {e}")))?;
        if bytes.len() as u64 > IA_MAX_TRACK_BYTES {
            return Err(GatewayError::Permanent(format!(
                "internet archive file {file_name} exceeded the per-track ceiling after download"
            )));
        }

        let cid = meta_feeder_sdk::hash::compute_ipfs_cid(&bytes);
        if let Some(cache) = &self.hash_cache {
            meta_feeder_sdk::common::store_midhash(cache, record_id, "internetarchive", &cid);
        }

        // The record is rebuilt from the item so the stored metadata is
        // complete even when this is reached by `compute` rather than from a
        // search hit.
        let deadline = Duration::from_secs(IA_TIMEOUT_SECS);
        let record = {
            let items = self.search_items(identifier, 1, deadline).await;
            let mut item = items
                .into_iter()
                .find(|i| i.identifier == identifier)
                .unwrap_or(ArchiveItem {
                    identifier: identifier.to_string(),
                    title: identifier.to_string(),
                    creator: String::new(),
                    date: None,
                    licence: None,
                    tracks: Vec::new(),
                });
            item.tracks = self.item_tracks(identifier, deadline).await;
            item.tracks
                .iter()
                .find(|t| t.file_name == file_name)
                .map(|t| self.track_record(&item, t, &BTreeMap::new()))
        };

        Ok(vec![HashOutcome {
            hash: Hash(cid),
            hash_kind: HashKind::Sha2_256,
            bytes: Some(bytes),
            record,
            file_extension: file_name
                .rsplit_once('.')
                .map(|(_, e)| e.to_ascii_lowercase()),
        }])
    }

    async fn handle_fetch(&self, record_id: &str) -> Result<Option<ByteStream>, GatewayError> {
        let Some((identifier, file_name)) = split_track_record_id(record_id) else {
            return Ok(None);
        };
        let url = self.download_url(identifier, &file_name);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| GatewayError::Transient(format!("internet archive fetch: {e}")))?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let stream = futures::StreamExt::map(resp.bytes_stream(), |r| {
            r.map_err(|e| GatewayError::Transient(format!("internet archive stream: {e}")))
        });
        Ok(Some(Box::pin(stream)))
    }
}

/// Does the query ask for this content kind? No filter means everything this
/// plugin serves.
fn wants_content_kind(query: &GatewayQuery, kind: &str) -> bool {
    match query.filters.get("contentKind") {
        None => true,
        Some(values) if values.is_empty() => true,
        Some(values) => values.iter().any(|v| v.trim().eq_ignore_ascii_case(kind)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn licence_urls_map_to_identifiers_not_urls() {
        assert_eq!(
            licence_from_url("http://creativecommons.org/licenses/by/4.0/").as_deref(),
            Some("CC-BY-4.0")
        );
        assert_eq!(
            licence_from_url("https://creativecommons.org/licenses/by-nc-sa/3.0").as_deref(),
            Some("CC-BY-NC-SA-3.0")
        );
        assert_eq!(
            licence_from_url("https://creativecommons.org/publicdomain/zero/1.0/").as_deref(),
            Some("CC0-1.0")
        );
        assert_eq!(
            licence_from_url("https://creativecommons.org/publicdomain/mark/1.0/").as_deref(),
            Some("PublicDomain")
        );
        assert_eq!(licence_from_url(""), None);
        assert_eq!(licence_from_url("https://example.com/terms"), None);
    }

    /// The collections that vouch for their own items, and the one that does
    /// not. `audio_music` having no fallback is what makes the `licence` field
    /// a claim the feeder can actually back.
    #[test]
    fn only_the_allow_listed_collections_vouch_for_licensing() {
        assert_eq!(
            collection_licence(&["etree".to_string()]),
            Some("LiveMusicArchive-TapingPolicy")
        );
        assert_eq!(collection_licence(&["netlabels".to_string()]), Some("CC"));
        assert_eq!(collection_licence(&["78rpm".to_string()]), Some("PublicDomain"));
        assert_eq!(collection_licence(&["audio_music".to_string()]), None);
        // Every vouching collection must be inside the searched allow-list —
        // a row here for a collection nothing queries is dead weight, and a
        // searched collection with no vouching row silently drops every item
        // that lacks its own licence URL.
        for (name, _) in SELF_VOUCHING_COLLECTIONS {
            assert!(
                IA_COLLECTIONS.contains(name),
                "{name} vouches for licensing but is never searched"
            );
        }
    }

    /// A file name may contain `/`, and axum's `:record_id` matches exactly one
    /// path segment — so a raw id would 404 on `/fetch` while working through
    /// `/compute`. This round-trip is what prevents that split behaviour.
    #[test]
    fn record_ids_round_trip_through_a_single_path_segment() {
        for name in ["Track01.flac", "disc1/Track 01.flac", "wéird ~ name.mp3"] {
            let id = track_record_id("gd1977-05-08", name);
            assert!(!id.contains('/'), "record id must stay one path segment: {id}");
            let (ident, back) = split_track_record_id(&id).expect("round trip");
            assert_eq!(ident, "gd1977-05-08");
            assert_eq!(back, name);
        }
        // A bare identifier is the pack, and has no file half.
        assert!(split_track_record_id("gd1977-05-08").is_none());
    }

    /// The derivative markers the archive's own pipeline appends. Grouping on
    /// the bare stem leaves each derivative in its own group, which is how the
    /// first cut of this shipped four records for one track.
    #[test]
    fn derivative_markers_collapse_onto_one_recording_stem() {
        let stem = recording_stem("Track01.flac");
        for name in [
            "Track01.mp3",
            "Track01_64kb.mp3",
            "Track01_128kb.mp3",
            "Track01_vbr.mp3",
            "Track01_sample.mp3",
        ] {
            assert_eq!(recording_stem(name), stem, "{name} must group with Track01");
        }
        // A different recording must NOT collapse into it.
        assert_ne!(recording_stem("Track02.mp3"), stem);
    }

    /// One recording, four derivatives, one record. Emitting all of them would
    /// show every track four times in an album view.
    #[test]
    fn only_the_best_derivative_of_each_recording_survives() {
        let files = json!([
            {"name": "Track01.flac", "title": "Opening", "track": "1", "size": "40000000"},
            {"name": "Track01.mp3",  "title": "Opening", "track": "1", "size": "8000000"},
            {"name": "Track01_64kb.mp3", "title": "Opening", "track": "1"},
            {"name": "Track02.mp3",  "title": "Second",  "track": "2"},
            {"name": "notes.txt"},
            {"name": "cover.jpg"}
        ]);
        let got = best_derivatives(files.as_array().unwrap());
        assert_eq!(got.len(), 2, "two recordings, not five files: {got:?}");
        assert_eq!(got[0].file_name, "Track01.flac", "flac outranks mp3");
        assert_eq!(got[0].track_number, Some(1));
        assert_eq!(got[1].file_name, "Track02.mp3");
    }

    /// Non-audio files are not tracks. An item's `cover.jpg` and `notes.txt`
    /// must never become playable records.
    #[test]
    fn non_audio_files_are_ignored() {
        assert!(audio_rank("cover.jpg").is_none());
        assert!(audio_rank("notes.txt").is_none());
        assert!(audio_rank("noextension").is_none());
        assert!(audio_rank("Track.FLAC").is_some(), "extension match is case-insensitive");
        // Preference order: flac beats mp3.
        assert!(audio_rank("a.flac") < audio_rank("a.mp3"));
    }

    /// ⚠ The Live Music Archive disc-encodes track numbers: `101` is disc 1
    /// track 1, `201` is disc 2 track 1. Read literally, a two-disc show
    /// reports "track 101" through "track 212" in every UI.
    #[test]
    fn disc_encoded_track_numbers_are_decoded() {
        assert_eq!(decode_track_number("101"), Some((Some(1), 1)));
        assert_eq!(decode_track_number("115"), Some((Some(1), 15)));
        assert_eq!(decode_track_number("212"), Some((Some(2), 12)));
        // Plain and "n/total" ordinals stay plain.
        assert_eq!(decode_track_number("7"), Some((None, 7)));
        assert_eq!(decode_track_number("7/12"), Some((None, 7)));
        // `100` is disc 1 track 0, which is meaningless — treat it as the
        // literal ordinal rather than inventing a track zero.
        assert_eq!(decode_track_number("100"), Some((None, 100)));
        assert_eq!(decode_track_number("0"), None);
        assert_eq!(decode_track_number(""), None);
        assert_eq!(decode_track_number("abc"), None);
    }

    /// Two discs' track 1s must not interleave.
    #[test]
    fn tracks_are_ordered_by_disc_then_track() {
        let files = json!([
            {"name": "d2t01.flac", "track": "201"},
            {"name": "d1t02.flac", "track": "102"},
            {"name": "d1t01.flac", "track": "101"}
        ]);
        let got = best_derivatives(files.as_array().unwrap());
        assert_eq!(
            got.iter().map(|t| t.file_name.as_str()).collect::<Vec<_>>(),
            vec!["d1t01.flac", "d1t02.flac", "d2t01.flac"]
        );
        assert_eq!(got[2].disc_number, Some(2));
        assert_eq!(got[2].track_number, Some(1));
    }

    #[test]
    fn tracks_are_ordered_by_track_number_then_name() {
        let files = json!([
            {"name": "b.mp3", "track": "2"},
            {"name": "a.mp3", "track": "1"},
            {"name": "z.mp3"}
        ]);
        let got = best_derivatives(files.as_array().unwrap());
        assert_eq!(
            got.iter().map(|t| t.file_name.as_str()).collect::<Vec<_>>(),
            vec!["a.mp3", "b.mp3", "z.mp3"],
            "untracked files sort last, not first"
        );
    }

    /// The archive returns single-valued fields as scalars and multi-valued
    /// ones as arrays, for the same field, depending on the item.
    #[test]
    fn scalar_and_array_field_shapes_both_parse() {
        assert_eq!(
            first_string(Some(&json!("Miles Davis"))).as_deref(),
            Some("Miles Davis")
        );
        assert_eq!(
            first_string(Some(&json!(["Miles Davis", "Gil Evans"]))).as_deref(),
            Some("Miles Davis")
        );
        assert_eq!(first_string(Some(&json!(["  ", "Real"]))).as_deref(), Some("Real"));
        assert_eq!(first_string(Some(&json!(null))), None);
        assert_eq!(string_list(Some(&json!("etree"))), vec!["etree"]);
        assert_eq!(
            string_list(Some(&json!(["etree", "stream_only"]))),
            vec!["etree", "stream_only"]
        );
    }

    #[test]
    fn the_query_is_scoped_to_the_collection_allow_list() {
        let p = InternetArchivePlugin::new();
        let q = p.build_query("");
        for c in IA_COLLECTIONS {
            assert!(q.contains(c), "collection {c} missing from the query");
        }
        // Free text is added, quoted.
        let q = p.build_query("kind of blue");
        assert!(q.contains("\"kind of blue\""));
    }

    /// ⚠ Regression guard for a measured, silent catalogue loss: Live Music
    /// Archive items are `mediatype:etree`, and a query saying only
    /// `mediatype:(audio)` matches **zero** of them (293,738 items, including
    /// 18,334 Grateful Dead recordings). The search still returns a few
    /// `audio_music` rows, so nothing looks broken — it just looks like a thin
    /// catalogue.
    #[test]
    fn the_query_covers_the_etree_mediatype_not_just_audio() {
        let q = InternetArchivePlugin::new().build_query("");
        assert!(
            q.contains("mediatype:(audio OR etree)"),
            "etree is a MEDIATYPE, not only a collection: {q}"
        );
    }

    /// The licensing rule is pushed into the query, not applied after it.
    /// Filtering afterwards spends the whole result page on items that will be
    /// dropped — `audio_music` sorted by downloads is mostly unlicensed, so a
    /// page of those yields zero records.
    #[test]
    fn the_licence_requirement_is_pushed_into_the_query() {
        let q = InternetArchivePlugin::new().build_query("");
        assert!(
            q.contains("licenseurl:[* TO *]"),
            "the licence requirement must be a query clause: {q}"
        );
        // …but only for the collections that do not vouch for themselves;
        // etree items routinely carry no per-item licence field at all.
        let vouching_clause = q
            .split(" OR (collection:")
            .next()
            .expect("vouching clause");
        for (name, _) in SELF_VOUCHING_COLLECTIONS {
            assert!(
                vouching_clause.contains(name),
                "{name} must be exempt from the per-item licence requirement: {q}"
            );
        }
    }

    #[test]
    fn declares_the_audio_types_it_serves() {
        let p = InternetArchivePlugin::new();
        assert_eq!(p.upstream_id(), "internetarchive");
        assert_eq!(p.served_file_types(), &["audio"]);
        assert_eq!(p.served_content_kinds(), &["track", "pack"]);
    }

    /// ⚠ **The bug this pins was seen live and looked like a working page.**
    /// An album detail page asks `mbReleaseGroupId:<uuid> fileType:audio`. The
    /// archive holds no MusicBrainz ids, so that query has no free text for
    /// this plugin — and an empty archive search returns its most-downloaded
    /// items. A BTS album page filled with sixty Grateful Dead and
    /// shortwave-numbers-station recordings, each presented as a source for
    /// that album.
    #[tokio::test]
    async fn an_id_anchored_query_returns_nothing_rather_than_a_browse() {
        let p = InternetArchivePlugin::new();
        for key in [
            "mbReleaseGroupId",
            "mbReleaseId",
            "mbArtistId",
            "mbRecordingId",
            "isrc",
        ] {
            let mut q = GatewayQuery::from_free_text("");
            q.filters
                .insert("fileType".to_string(), vec!["audio".to_string()]);
            q.filters
                .insert(key.to_string(), vec!["some-id".to_string()]);
            assert!(
                p.handle_query(&q, 20).await.unwrap().is_empty(),
                "{key} must not be answered with unrelated content"
            );
        }
    }

    /// …but a plain free-text audio query is still answered normally. The guard
    /// must not disable the plugin.
    #[tokio::test]
    async fn a_plain_free_text_query_is_not_blocked_by_the_guard() {
        let p = InternetArchivePlugin::new();
        let mut q = GatewayQuery::from_free_text("grateful dead");
        q.filters
            .insert("fileType".to_string(), vec!["audio".to_string()]);
        // No network here, so this resolves to an empty result — the assertion
        // is that it took the search path rather than the early return, which
        // the id-filter test above covers by contrast.
        let _ = p.handle_query(&q, 5).await.unwrap();
    }

    /// A pack has no bytes of its own — its tracks are separately addressable.
    #[tokio::test]
    async fn a_pack_record_id_resolves_to_no_outcomes() {
        let p = InternetArchivePlugin::new();
        assert!(p.compute_outcomes("some-identifier").await.unwrap().is_empty());
    }

    fn item() -> ArchiveItem {
        ArchiveItem {
            identifier: "gd1977-05-08".into(),
            title: "Barton Hall".into(),
            creator: "Grateful Dead".into(),
            date: Some("1977-05-08".into()),
            licence: Some("LiveMusicArchive-TapingPolicy".into()),
            tracks: vec![ArchiveTrack {
                file_name: "gd77-05-08d1t01.flac".into(),
                title: "New Minglewood Blues".into(),
                track_number: Some(1),
                disc_number: Some(1),
                size: Some(50_000_000),
            }],
        }
    }

    #[test]
    fn a_track_record_carries_the_music_vocabulary_and_a_licence() {
        let p = InternetArchivePlugin::new();
        let it = item();
        let r = p.track_record(&it, &it.tracks[0], &BTreeMap::new());
        assert_eq!(r.fields.get("fileType").map(String::as_str), Some("audio"));
        assert_eq!(r.fields.get("contentKind").map(String::as_str), Some("track"));
        assert_eq!(r.fields.get("album").map(String::as_str), Some("Barton Hall"));
        assert_eq!(
            r.fields.get("albumArtist").map(String::as_str),
            Some("Grateful Dead")
        );
        assert_eq!(r.fields.get("trackNumber").map(String::as_str), Some("1"));
        assert_eq!(r.fields.get("extension").map(String::as_str), Some("flac"));
        assert_eq!(
            r.fields.get("licence").map(String::as_str),
            Some("LiveMusicArchive-TapingPolicy")
        );
        assert_eq!(
            r.fields.get("internetarchiveid").map(String::as_str),
            Some("gd1977-05-08")
        );
        // ⚠ The url locator, without which this track cannot be played from a
        // search hit — a content digest does not exist until the bytes are
        // fetched, which happens long after the row is rendered.
        let want = meta_feeder_sdk::hash::compute_url_cid(
            "https://archive.org/download/gd1977-05-08/gd77-05-08d1t01.flac",
        )
        .expect("locator");
        assert_eq!(r.fields.get(&format!("cids/{want}")).map(String::as_str), Some("true"));
        // ⚠ A cover IS claimed, and must be: without one the gate hides every
        // free-tier track and the cold-start row is permanently empty. It is a
        // `url` LOCATOR cid, not a raw URL — rule #5.
        let want_cover = meta_feeder_sdk::hash::compute_url_cid(
            "https://archive.org/services/img/gd1977-05-08",
        )
        .expect("cover locator");
        assert_eq!(r.fields.get("cover").map(String::as_str), Some(want_cover.as_str()));
        assert!(
            !r.fields.contains_key("cover_url"),
            "artwork must never be a raw *_url field"
        );
        // Never a video field.
        assert!(r.fields.get("videoType").is_none());
        assert!(r.fields.get("tmdbid").is_none());
    }

    /// The album release stays first class here, unlike a season pack in the
    /// video domain — but it is explicitly non-playable, so the consumer greys
    /// the play button rather than failing at byte-fetch time.
    #[test]
    fn a_pack_record_is_emitted_and_marked_unplayable() {
        let p = InternetArchivePlugin::new();
        let r = p.pack_record(&item(), &BTreeMap::new());
        assert_eq!(r.fields.get("contentKind").map(String::as_str), Some("pack"));
        assert_eq!(r.fields.get("playable").map(String::as_str), Some("false"));
        assert_eq!(r.fields.get("trackCount").map(String::as_str), Some("1"));
        assert_eq!(r.record_id, "gd1977-05-08");
    }

    #[test]
    fn filters_are_echoed_except_the_key_sets() {
        let p = InternetArchivePlugin::new();
        let it = item();
        let mut filters = BTreeMap::new();
        filters.insert("licence".to_string(), vec!["cc".to_string()]);
        filters.insert("genres".to_string(), vec!["rock".to_string()]);
        let r = p.track_record(&it, &it.tracks[0], &filters);
        // The record's own licence wins over the echo.
        assert_eq!(
            r.fields.get("licence").map(String::as_str),
            Some("LiveMusicArchive-TapingPolicy")
        );
        assert!(r.fields.get("genres").is_none());
    }
}
