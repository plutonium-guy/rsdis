//! Geohash encode/decode for the GEO command family.
//!
//! Owned by W2c; do not edit if you are not that agent.
//!
//! Redis stores a geo point as a 52-bit interleaved geohash used directly as
//! the sorted-set score, so `GEOADD` is a `ZADD` and `GEOSEARCH` is a set of
//! `ZRANGEBYSCORE` calls over neighbouring boxes. The constants below are the
//! ones `geohash.c` uses; matching them is what makes `GEOPOS` round-trip to
//! the same coordinates real Redis returns.

/// Number of bits per coordinate in the interleaved score.
pub const GEO_STEP: u32 = 26;

/// Latitude bounds Redis clamps to (Mercator, not the full +/-90).
pub const GEO_LAT_MIN: f64 = -85.051_128_779_806_59;
pub const GEO_LAT_MAX: f64 = 85.051_128_779_806_59;
pub const GEO_LONG_MIN: f64 = -180.0;
pub const GEO_LONG_MAX: f64 = 180.0;

/// Earth radius in metres, as used by `geohashGetDistance`.
pub const EARTH_RADIUS_M: f64 = 6_372_797.560_856;
