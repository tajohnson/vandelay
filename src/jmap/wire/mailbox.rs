/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use serde::{Deserialize, Serialize};

use super::JmapId;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mailbox {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<JmapId>,

    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<JmapId>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    // Stalwart 0.12 sends `sortOrder: null` for user-created mailboxes.
    #[serde(default, deserialize_with = "null_as_zero")]
    pub sort_order: u32,

    #[serde(default = "default_true", deserialize_with = "null_as_true")]
    pub is_subscribed: bool,
}

fn default_true() -> bool {
    true
}

fn null_as_zero<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u32, D::Error> {
    Ok(Option::<u32>::deserialize(d)?.unwrap_or(0))
}

fn null_as_true<'de, D: serde::Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
    Ok(Option::<bool>::deserialize(d)?.unwrap_or(true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_sort_order_and_subscription_take_defaults() {
        let m: Mailbox =
            serde_json::from_str(r#"{"name":"x","sortOrder":null,"isSubscribed":null}"#).unwrap();
        assert_eq!(m.sort_order, 0);
        assert!(m.is_subscribed);
    }
}
