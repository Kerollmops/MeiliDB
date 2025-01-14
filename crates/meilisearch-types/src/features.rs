use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct RuntimeTogglableFeatures {
    pub metrics: bool,
    pub logs_route: bool,
    pub edit_documents_by_function: bool,
    pub contains_filter: bool,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct InstanceTogglableFeatures {
    pub metrics: bool,
    pub logs_route: bool,
    pub contains_filter: bool,
}
