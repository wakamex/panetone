use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

use super::{EffectId, RouteId};

const TELEGRAM_TEXT_UNITS: usize = 3900;
const SIGNAL_TEXT_CHARS: usize = 4000;
// Telegram allows 1,024 caption characters; room is left for RESEND_PREFIX.
const ATTACHMENT_CAPTION_CHARS: usize = 1024 - RESEND_PREFIX.len();
pub const MAX_CHANNEL_ATTACHMENT_BYTES: u64 = 10 * 1024 * 1024;
pub const MAX_CHANNEL_ATTACHMENTS: usize = 10;
pub const MAX_CHANNEL_ATTACHMENT_TOTAL_BYTES: u64 = 50 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelKind {
    Telegram,
    Signal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChannelBinding {
    Telegram {
        topic_id: i64,
    },
    Signal {
        group_id: String,
        #[serde(default)]
        allow_members: bool,
        /// The Signal account that serves this group. Without one, the
        /// daemon's default account does.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        account: Option<String>,
    },
}

/// Removes characters that draw nothing, such as zero-width spaces, a byte
/// order mark, direction controls and control codes, and turns lone line
/// terminators into newlines. Claude Code does the same to a submitted
/// prompt and, having changed anything, waits for a second Enter instead of
/// sending it, so text bound for an agent must arrive already clean.
///
/// Joiners, variation selectors and tag characters stay only where Claude
/// Code 2.1.296 keeps them: inside emoji, keycap and flag sequences and next
/// to letters of scripts that need them. Characters it keeps only in a few
/// non-Latin contexts (a zero-width space in Khmer, Thai, Lao or Myanmar, and
/// direction marks in right-to-left text) are removed everywhere.
pub fn strip_invisible(text: &str) -> String {
    let chars = text.chars().collect::<Vec<_>>();
    let flag_tags = flag_tag_positions(&chars);
    let mut clean = String::with_capacity(text.len());
    for (index, &character) in chars.iter().enumerate() {
        let previous = index.checked_sub(1).map(|at| chars[at]);
        let next = chars.get(index + 1).copied();
        match character {
            '\r' if next == Some('\n') => {}
            '\r' | '\u{b}' | '\u{c}' | '\u{85}' | '\u{2028}' | '\u{2029}' => clean.push('\n'),
            '\t' | '\n' => clean.push(character),
            '\u{200d}' if keeps_joiner(&chars, index) => clean.push(character),
            '\u{200c}' if keeps_non_joiner(&chars, index) => clean.push(character),
            '\u{fe0e}' | '\u{fe0f}' if follows_emoji(previous, next) => clean.push(character),
            '\u{fe00}'..='\u{fe02}' if keeps_text_selector(character, previous) => {
                clean.push(character)
            }
            '\u{e0000}'..='\u{e0fff}' if flag_tags.contains(&index) => clean.push(character),
            '\u{0}'..='\u{1f}'
            | '\u{7f}'..='\u{9f}'
            | '\u{ad}'
            | '\u{34f}'
            | '\u{61c}'
            | '\u{115f}'
            | '\u{1160}'
            | '\u{180b}'..='\u{180f}'
            | '\u{200b}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{206f}'
            | '\u{3164}'
            | '\u{fe00}'..='\u{fe0f}'
            | '\u{feff}'
            | '\u{ffa0}'
            | '\u{fff0}'..='\u{fffb}'
            | '\u{1d173}'..='\u{1d17a}'
            | '\u{e0000}'..='\u{e0fff}' => {}
            _ => clean.push(character),
        }
    }
    clean
}

fn class(pattern: &str) -> regex::Regex {
    regex::Regex::new(&format!("^[{pattern}]$")).expect("the character class is valid")
}

fn in_class(cell: &std::sync::LazyLock<regex::Regex>, character: char) -> bool {
    cell.is_match(character.encode_utf8(&mut [0; 4]))
}

static PICTOGRAPH: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| class(r"\p{Extended_Pictographic}"));
static EMOJI: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| class(r"\p{Emoji}\p{Extended_Pictographic}"));
static MARK: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| class(r"\p{M}"));
static WORD: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| class(r"\p{L}\p{N}\p{M}"));
const INDIC: &str = r"\p{Devanagari}\p{Bengali}\p{Gurmukhi}\p{Gujarati}\p{Oriya}\p{Tamil}\p{Telugu}\p{Kannada}\p{Malayalam}\p{Sinhala}";
static JOINER_AFTER: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    class(&format!(
        r"[{INDIC}\p{{Myanmar}}\p{{Khmer}}\p{{Tibetan}}\p{{Arabic}}\p{{Syriac}}\p{{Tifinagh}}]&&\p{{L}}"
    ))
});
static JOINER_BEFORE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    class(r"[\p{Arabic}\p{Syriac}\p{Mongolian}\p{Nko}]&&\p{L}")
});
static NON_JOINER_LETTER: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    class(&format!(
        r"[{INDIC}\p{{Arabic}}\p{{Syriac}}\p{{Nko}}\p{{Mongolian}}\p{{Tibetan}}]&&\p{{L}}"
    ))
});
static SELECTOR_ONE_BASE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    class(r"\p{Egyptian_Hieroglyphs}\p{Myanmar}\p{Phags_Pa}\p{Manichaean}\x{2200}-\x{2aff}")
});
static HIEROGLYPH: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| class(r"\p{Egyptian_Hieroglyphs}"));

/// The character before `index`, skipping the combining marks attached to it.
fn base_before(chars: &[char], index: usize) -> Option<char> {
    chars[..index]
        .iter()
        .rev()
        .copied()
        .find(|&character| !in_class(&MARK, character))
}

fn keeps_joiner(chars: &[char], index: usize) -> bool {
    let previous = index.checked_sub(1).map(|at| chars[at]);
    let next = chars.get(index + 1).copied();
    // Between two pictographs, allowing presentation selectors and skin
    // tones after the first.
    let pictograph_before = chars[..index]
        .iter()
        .rev()
        .copied()
        .enumerate()
        .find(|&(skipped, character)| {
            skipped >= 2 || !matches!(character, '\u{fe0f}' | '\u{1f3fb}'..='\u{1f3ff}')
        })
        .is_some_and(|(_, character)| in_class(&PICTOGRAPH, character));
    if previous != Some('\u{200d}')
        && pictograph_before
        && next.is_some_and(|character| in_class(&PICTOGRAPH, character))
    {
        return true;
    }
    if previous.is_some()
        && base_before(chars, index).is_some_and(|character| in_class(&JOINER_AFTER, character))
    {
        return true;
    }
    next.is_some_and(|character| in_class(&JOINER_BEFORE, character))
        && !previous.is_some_and(|character| in_class(&WORD, character))
}

fn keeps_non_joiner(chars: &[char], index: usize) -> bool {
    let previous = index.checked_sub(1).map(|at| chars[at]);
    let next = chars.get(index + 1).copied();
    if base_before(chars, index).is_some_and(|character| in_class(&NON_JOINER_LETTER, character)) {
        return true;
    }
    next.is_some_and(|character| in_class(&NON_JOINER_LETTER, character))
        && !previous
            .is_some_and(|character| character.is_whitespace() || in_class(&MARK, character))
}

fn follows_emoji(previous: Option<char>, next: Option<char>) -> bool {
    match previous {
        Some('0'..='9' | '#' | '*') => next == Some('\u{20e3}'),
        Some(character) => character >= '\u{a9}' && in_class(&EMOJI, character),
        None => false,
    }
}

fn keeps_text_selector(selector: char, previous: Option<char>) -> bool {
    previous.is_some_and(|character| {
        in_class(&HIEROGLYPH, character)
            || (selector == '\u{fe00}' && in_class(&SELECTOR_ONE_BASE, character))
    })
}

/// The positions of tag characters inside the England, Scotland and Wales
/// flag sequences, the only places tag characters are kept.
fn flag_tag_positions(chars: &[char]) -> std::collections::BTreeSet<usize> {
    let mut positions = std::collections::BTreeSet::new();
    for (start, _) in chars
        .iter()
        .enumerate()
        .filter(|&(_, &character)| character == '\u{1f3f4}')
    {
        for region in ["gbeng", "gbsct", "gbwls"] {
            let tags = region
                .chars()
                .map(|letter| char::from_u32(0xe0000 + letter as u32).expect("a tag letter"))
                .chain(['\u{e007f}']);
            if chars[start + 1..].iter().copied().take(6).eq(tags) {
                positions.extend(start + 1..start + 7);
            }
        }
    }
    positions
}

/// The account bound to the route that serves a Signal group, when that
/// binding names one.
pub fn signal_group_account<'a>(routes: &'a [super::Route], group_id: &str) -> Option<&'a str> {
    routes
        .iter()
        .flat_map(|route| &route.channels)
        .find_map(|binding| match binding {
            ChannelBinding::Signal {
                group_id: bound,
                account,
                ..
            } if bound.trim_end_matches('=') == group_id.trim_end_matches('=') => {
                Some(account.as_deref())
            }
            _ => None,
        })
        .flatten()
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
pub struct ChannelAttachment {
    pub file_name: String,
    pub media_type: String,
    pub size: u64,
    pub sha256: String,
    pub data_base64: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutboxAction {
    pub id: String,
    pub label: String,
}

impl ChannelAttachment {
    pub fn bytes(&self) -> Result<Vec<u8>, base64::DecodeError> {
        BASE64.decode(&self.data_base64)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutboxItem {
    pub id: EffectId,
    #[serde(default)]
    pub route_id: Option<RouteId>,
    #[serde(default)]
    pub sender_harness: Option<String>,
    #[serde(default)]
    pub source_agent: Option<super::AgentBinding>,
    pub kind: ChannelKind,
    pub destination: String,
    pub body: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<ChannelAttachment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<OutboxAction>,
    pub state: OutboxState,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub external_receipt: Option<String>,
    /// Attempts that failed after the channel may already have posted the
    /// message. A later attempt is labeled as a resend.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub uncertain_attempts: u32,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

pub const RESEND_PREFIX: &str = "[resent] ";

impl OutboxItem {
    /// The body to post on this attempt: labeled when an earlier attempt may
    /// already have appeared, so a duplicate is recognizable.
    pub fn attempt_body(&self) -> String {
        if self.uncertain_attempts == 0 {
            self.body.clone()
        } else {
            format!("{RESEND_PREFIX}{}", self.body)
        }
    }
}

pub fn chunk_outbox(mut item: OutboxItem) -> Vec<OutboxItem> {
    if !item.attachments.is_empty() {
        let chunks = split_by_chars(&item.body, ATTACHMENT_CAPTION_CHARS);
        if chunks.len() == 1 {
            return vec![item];
        }
        let parent = item.id;
        return chunks
            .into_iter()
            .enumerate()
            .map(|(index, body)| {
                item.id = EffectId::chunk(parent, index);
                item.body = body;
                if index > 0 {
                    item.attachments.clear();
                }
                item.clone()
            })
            .collect();
    }
    if !item.actions.is_empty() {
        let chunks = split_text(item.kind, &item.body);
        if chunks.len() == 1 {
            return vec![item];
        }
        let parent = item.id;
        return chunks
            .into_iter()
            .enumerate()
            .map(|(index, body)| {
                item.id = EffectId::chunk(parent, index);
                item.body = body;
                if index > 0 {
                    item.actions.clear();
                }
                item.clone()
            })
            .collect();
    }
    let chunks = split_text(item.kind, &item.body);
    if chunks.len() == 1 {
        return vec![item];
    }
    let parent = item.id;
    chunks
        .into_iter()
        .enumerate()
        .map(|(index, body)| {
            item.id = EffectId::chunk(parent, index);
            item.body = body;
            item.clone()
        })
        .collect()
}

fn split_by_chars(text: &str, limit: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut chunk = String::new();
    let mut count = 0;
    for character in text.chars() {
        if !chunk.is_empty() && count == limit {
            chunks.push(std::mem::take(&mut chunk));
            count = 0;
        }
        chunk.push(character);
        count += 1;
    }
    if !chunk.is_empty() || chunks.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

fn split_text(kind: ChannelKind, text: &str) -> Vec<String> {
    let limit = match kind {
        ChannelKind::Telegram => TELEGRAM_TEXT_UNITS,
        ChannelKind::Signal => SIGNAL_TEXT_CHARS,
    };
    let mut chunks = Vec::new();
    let mut chunk = String::new();
    let mut units = 0;
    for character in text.chars() {
        let character_units = match kind {
            ChannelKind::Telegram => character.len_utf16(),
            ChannelKind::Signal => 1,
        };
        if !chunk.is_empty() && units + character_units > limit {
            chunks.push(std::mem::take(&mut chunk));
            units = 0;
        }
        chunk.push(character);
        units += character_units;
    }
    if !chunk.is_empty() || chunks.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn item(kind: ChannelKind, body: String) -> OutboxItem {
        OutboxItem {
            id: EffectId::new(Uuid::from_u128(1)),
            route_id: None,
            sender_harness: None,
            source_agent: None,
            kind,
            destination: "destination".into(),
            body,
            attachments: Vec::new(),
            actions: Vec::new(),
            state: OutboxState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
            uncertain_attempts: 0,
        }
    }

    #[test]
    fn telegram_chunks_count_utf16_units_and_keep_stable_ids() {
        let body = "x".repeat(3899) + "😀y";
        let chunks = chunk_outbox(item(ChannelKind::Telegram, body.clone()));
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].body.encode_utf16().count(), 3899);
        assert_eq!(chunks[1].body, "😀y");
        assert_eq!(
            chunks.iter().map(|chunk| chunk.id).collect::<Vec<_>>(),
            chunk_outbox(item(ChannelKind::Telegram, body))
                .iter()
                .map(|chunk| chunk.id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn short_output_keeps_its_original_effect_id() {
        let original = item(ChannelKind::Signal, "hello".into());
        assert_eq!(chunk_outbox(original.clone()), vec![original]);
    }

    #[test]
    fn long_attachment_caption_sends_the_file_once() {
        let mut original = item(ChannelKind::Signal, "x".repeat(1025));
        original.attachments.push(ChannelAttachment {
            file_name: "scene.png".into(),
            media_type: "image/png".into(),
            size: 1,
            sha256: "hash".into(),
            data_base64: "eA==".into(),
        });
        let mut chunks = chunk_outbox(original);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].attachments.len(), 1);
        assert_eq!(chunks[1].body, "x".repeat(10));
        assert!(chunks[1].attachments.is_empty());
        // A resent caption still fits Telegram's 1,024-character limit.
        chunks[0].uncertain_attempts = 1;
        assert_eq!(chunks[0].attempt_body().chars().count(), 1024);
    }
}

#[cfg(test)]
mod invisible_tests {
    use super::strip_invisible;

    #[test]
    fn characters_that_draw_nothing_are_removed() {
        assert_eq!(
            strip_invisible("\u{feff}Hi\u{200b} there\u{200e}\u{ad}\r\nnext\u{2028}line\u{7}"),
            "Hi there\nnext\nline"
        );
        assert_eq!(strip_invisible("a\rb\u{b}c\u{c}d\u{85}e"), "a\nb\nc\nd\ne");
        assert_eq!(strip_invisible("x\u{1d173}y\u{e0041}z\u{fe05}"), "xyz");
    }

    #[test]
    fn joiners_selectors_and_tags_stay_only_where_text_needs_them() {
        for kept in [
            "tab\there",
            // A warning sign with its emoji presentation, a family, a keycap.
            "\u{26a0}\u{fe0f}",
            "\u{1f468}\u{200d}\u{1f469}\u{200d}\u{1f467}",
            "\u{1f469}\u{1f3fd}\u{200d}\u{1f4bb}",
            "\u{2764}\u{fe0f}\u{200d}\u{1f525}",
            "1\u{fe0f}\u{20e3}",
            // The flag of Scotland, Persian with a non-joiner, Hindi with a joiner.
            "\u{1f3f4}\u{e0067}\u{e0062}\u{e0073}\u{e0063}\u{e0074}\u{e007f}",
            "\u{645}\u{6cc}\u{200c}\u{62e}",
            "\u{915}\u{94d}\u{200d}\u{937}",
        ] {
            assert_eq!(strip_invisible(kept), kept, "{kept:?}");
        }
        // The same characters where nothing needs them.
        assert_eq!(strip_invisible("a\u{200d}b\u{200c}c"), "abc");
        assert_eq!(strip_invisible("a\u{fe0f}"), "a");
        assert_eq!(strip_invisible("1\u{fe0f}"), "1");
        assert_eq!(strip_invisible("\u{26a0}\u{fe0f}\u{fe0f}"), "\u{26a0}\u{fe0f}");
        assert_eq!(strip_invisible("\u{1f468}\u{200d}\u{200d}\u{1f469}"), "\u{1f468}\u{1f469}");
        assert_eq!(
            strip_invisible("\u{1f3f4}\u{e0075}\u{e0073}\u{e007f}"),
            "\u{1f3f4}"
        );
    }
}
