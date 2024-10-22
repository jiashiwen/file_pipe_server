use serde::Deserialize;
use strum_macros::{Display, EnumString};

#[derive(EnumString, Display, Debug, PartialEq, Deserialize)]
pub enum Option {
    Put,
    Del,
    Get,
}
