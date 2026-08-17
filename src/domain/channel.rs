use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::{EffectId, RouteId};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelKind {
    Telegram,
    Signal,
    Slack,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChannelBinding {
    Telegram { topic_id: i64 },
    Signal { group_id: String },
    Slack { channel_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChannelSelection {
    pub kind: ChannelKind,
    pub destination: String,
    pub route_title: String,
}

#[derive(Clone, Debug, Default)]
pub struct ChannelAvailability<'a> {
    pub telegram_topic: Option<i64>,
    pub signal_enabled: bool,
    pub signal_group: Option<&'a str>,
    pub slack_enabled: bool,
    pub slack_channel: Option<&'a str>,
}

pub fn select_channel(
    source: Option<ChannelKind>,
    route_title: &str,
    availability: &ChannelAvailability<'_>,
) -> Option<ChannelSelection> {
    let selected = source.unwrap_or_else(|| {
        if availability.signal_enabled && availability.signal_group.is_some() {
            ChannelKind::Signal
        } else {
            ChannelKind::Telegram
        }
    });
    match selected {
        ChannelKind::Telegram => availability
            .telegram_topic
            .map(|topic_id| ChannelSelection {
                kind: selected,
                destination: topic_id.to_string(),
                route_title: route_title.into(),
            }),
        ChannelKind::Signal if availability.signal_enabled => {
            availability.signal_group.map(|group_id| ChannelSelection {
                kind: selected,
                destination: group_id.into(),
                route_title: String::new(),
            })
        }
        ChannelKind::Slack if availability.slack_enabled => {
            availability
                .slack_channel
                .map(|channel_id| ChannelSelection {
                    kind: selected,
                    destination: channel_id.into(),
                    route_title: String::new(),
                })
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxState {
    Pending,
    Delivering,
    Delivered,
    Failed,
    Indeterminate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutboxItem {
    pub id: EffectId,
    #[serde(default)]
    pub route_id: Option<RouteId>,
    #[serde(default)]
    pub sender_harness: Option<String>,
    pub kind: ChannelKind,
    pub destination: String,
    pub body: String,
    pub state: OutboxState,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub external_receipt: Option<String>,
}

pub fn fair_retry_indices(items: &[OutboxItem]) -> Vec<usize> {
    let mut seen = HashSet::new();
    items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            (item.state == OutboxState::Pending && seen.insert(item.kind)).then_some(index)
        })
        .collect()
}

pub fn chunk_lines(text: &str, limit: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        let separator = usize::from(!current.is_empty());
        if !current.is_empty() && current.len() + separator + line.len() > limit {
            chunks.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push('\n');
        }
        current.push_str(line);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

pub fn chunk_utf16(text: &str, limit: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut units = 0;
    for character in text.chars() {
        let character_units = character.len_utf16();
        if !current.is_empty() && units + character_units > limit {
            chunks.push(std::mem::take(&mut current));
            units = 0;
        }
        current.push(character);
        units += character_units;
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

pub fn normalize_signal_group_id(group_id: Option<&str>) -> String {
    group_id
        .unwrap_or_default()
        .trim()
        .trim_end_matches('=')
        .into()
}

pub fn format_slack_tables(text: &str) -> String {
    fn flush(table: &mut Vec<Vec<String>>, output: &mut Vec<String>, trailing_blank: bool) {
        if table.is_empty() {
            return;
        }
        let columns = table[0].len();
        let widths = (0..columns)
            .map(|column| {
                table
                    .iter()
                    .filter_map(|row| row.get(column))
                    .map(String::len)
                    .max()
                    .unwrap_or(0)
            })
            .collect::<Vec<_>>();
        for row in table.drain(..) {
            let rendered = row
                .iter()
                .enumerate()
                .map(|(column, cell)| format!("{cell:<width$}", width = widths[column]))
                .collect::<Vec<_>>()
                .join("  ");
            output.push(rendered);
        }
        if trailing_blank {
            output.push(String::new());
        }
    }

    let mut output = Vec::new();
    let mut table = Vec::new();
    for line in text.split('\n') {
        let trimmed = line.trim();
        let is_table = trimmed.starts_with('|') && trimmed.ends_with('|');
        if is_table {
            let inner = trimmed.trim_matches('|');
            let separator = inner
                .chars()
                .all(|character| matches!(character, '-' | ' ' | '|' | ':'));
            if !separator {
                table.push(
                    inner
                        .split('|')
                        .map(|cell| cell.trim().to_owned())
                        .collect(),
                );
            }
        } else {
            flush(&mut table, &mut output, true);
            output.push(line.to_owned());
        }
    }
    flush(&mut table, &mut output, false);
    output.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_at_line_and_utf16_boundaries() {
        assert_eq!(chunk_lines("one\ntwo\nthree", 8), ["one\ntwo", "three"]);
        assert_eq!(chunk_utf16("A😀BC", 3), ["A😀", "BC"]);
    }

    #[test]
    fn formats_tables_and_signal_ids() {
        assert_eq!(
            format_slack_tables("Before\n| A | B |\n| - | -: |\n| x | 2 |\nAfter"),
            "Before\nA  B\nx  2\n\nAfter"
        );
        assert_eq!(normalize_signal_group_id(Some(" group== ")), "group");
        assert_eq!(normalize_signal_group_id(None), "");
    }
}
