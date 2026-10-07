#[derive(Debug, PartialEq, Eq)]
pub enum SizeError {
    InvalidNumber,
    UnknownUnit,
    Overflow,
    FractionalByte,
}

impl std::fmt::Display for SizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidNumber => "expected a nonnegative size, such as 1048576, 1MiB, or 1.5GB",
            Self::UnknownUnit => {
                "unknown size unit; use B, KB/MB/GB/TB/PB/EB, or KiB/MiB/GiB/TiB/PiB/EiB"
            }
            Self::Overflow => "size exceeds the supported byte range",
            Self::FractionalByte => "size must resolve to a whole number of bytes",
        })
    }
}

impl std::error::Error for SizeError {}

pub fn parse_bytes(input: &str) -> Result<u64, SizeError> {
    let input = input.trim();
    let end = input
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(input.len());
    let (number, unit) = input.split_at(end);
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty()
        || number.ends_with('.')
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(SizeError::InvalidNumber);
    }
    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1000u128,
        "mb" => 1000u128.pow(2),
        "gb" => 1000u128.pow(3),
        "tb" => 1000u128.pow(4),
        "pb" => 1000u128.pow(5),
        "eb" => 1000u128.pow(6),
        "kib" => 1024u128,
        "mib" => 1024u128.pow(2),
        "gib" => 1024u128.pow(3),
        "tib" => 1024u128.pow(4),
        "pib" => 1024u128.pow(5),
        "eib" => 1024u128.pow(6),
        _ => return Err(SizeError::UnknownUnit),
    };
    // Floating-point rounding can change a retention limit or hide an overflow.
    let digits = format!("{whole}{fraction}")
        .parse::<u128>()
        .map_err(|_| SizeError::Overflow)?;
    let places = u32::try_from(fraction.len()).map_err(|_| SizeError::Overflow)?;
    let divisor = 10u128.checked_pow(places).ok_or(SizeError::Overflow)?;
    let scaled = digits.checked_mul(multiplier).ok_or(SizeError::Overflow)?;
    if scaled % divisor != 0 {
        return Err(SizeError::FractionalByte);
    }
    u64::try_from(scaled / divisor).map_err(|_| SizeError::Overflow)
}

pub fn parse_memory(input: &str) -> Result<usize, SizeError> {
    usize::try_from(parse_bytes(input)?).map_err(|_| SizeError::Overflow)
}
