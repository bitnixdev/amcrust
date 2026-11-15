//! Media output building blocks. Not yet connected to HomeKit transports.
pub mod audio;
pub mod fmp4;
pub mod rtp;

use crate::{ingest::TimeBase, source::Terminal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Source(Terminal),
    Configuration,
    Clock,
    Order,
    Limit,
    Allocation,
    Cancelled,
    Deadline,
    Incomplete,
    Closed,
    Native { operation: &'static str, code: i32 },
}
impl From<Terminal> for Error {
    fn from(value: Terminal) -> Self {
        Self::Source(value)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "media output: {self:?}")
    }
}
impl std::error::Error for Error {}

/// Exact rational rescaling only. Refuse clocks the target cannot represent;
/// never round away an unexplained source timing difference.
fn rescale(value: i128, from: TimeBase, to: TimeBase) -> Result<i64, Error> {
    if from.numerator <= 0 || from.denominator <= 0 || to.numerator <= 0 || to.denominator <= 0 {
        return Err(Error::Clock);
    }
    let numerator = value
        .checked_mul(from.numerator as i128)
        .and_then(|v| v.checked_mul(to.denominator as i128))
        .ok_or(Error::Clock)?;
    let denominator = from.denominator as i128 * to.numerator as i128;
    if numerator % denominator != 0 {
        return Err(Error::Clock);
    }
    let result = i64::try_from(numerator / denominator).map_err(|_| Error::Clock)?;
    // FFmpeg reserves i64::MIN for AV_NOPTS_VALUE, not a real timestamp.
    if result == i64::MIN {
        return Err(Error::Clock);
    }
    Ok(result)
}
