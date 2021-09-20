use std::collections::HashMap;
use chrono::{DateTime, Utc};

pub struct Message {
    pub timestamp: DateTime<Utc>,
    pub hostname: String,
    pub message: String,
    pub container: Option<BeatContainerMetadata>
}

pub struct BeatContainerMetadata {
    pub id: String,
    pub name: Option<String>,
    pub image: Option<String>
    // labels: HashMap<String, String>
}
