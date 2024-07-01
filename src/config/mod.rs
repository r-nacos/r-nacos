pub mod config_db;
pub mod config_index;
pub mod config_sled;
pub mod config_subscribe;
pub mod config_type;
pub mod core;
pub mod dal;
pub mod metrics;
pub mod model;
pub mod utils;

pub struct ConfigUtils;

pub const DEFAULT_TENANT: &str = "public";
pub const __INNER_SYSTEM__TENANT: &str = "__INNER_SYSTEM__";
pub const MANIFEST: &str = "manifest";

impl ConfigUtils {
    pub fn default_tenant(val: String) -> String {
        if val == DEFAULT_TENANT {
            "".to_owned()
        } else {
            val
        }
    }
}
