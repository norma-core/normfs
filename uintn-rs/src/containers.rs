use super::UintN;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UintNRange {
    pub start: UintN,
    pub end: UintN,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionRange {
    pub start: Option<UintN>,
    pub end: Option<UintN>,
}
