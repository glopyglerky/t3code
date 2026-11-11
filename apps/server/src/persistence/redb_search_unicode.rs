//! SQLite's public-domain FTS5 trigram case and diacritic folding, in safe Rust.
//! Translated from sqlite/sqlite ext/fts5/fts5_unicode2.c, blob 2133d5d5b894c35fbbe642a32ee027ca15ddab33.
//! Scalar decoding follows SQLite 3.54's src/utf.c READ_UTF8 macro, blob
//! e8bb63979703241b038cac37a686de2fbe9616b2, used by the pinned Node 26 amalgamation.
//! Trigram remove_diacritics=1 uses UnicodeFold(..., 2), including complex
//! Latin diacritics. This is deliberately not Rust's evolving Unicode lowercase
//! or general NFD normalization. The source blessing applies:
//! May you do good and not evil.
//! May you find forgiveness for yourself and forgive others.
//! May you share freely, never taking more than you give.

const DIACRITIC_RANGES: &[u16] = &[
    0, 1797, 1848, 1859, 1891, 1928, 1940, 1995, 2024, 2040, 2060, 2110, 2168, 2206, 2264, 2286,
    2344, 2383, 2472, 2488, 2516, 2596, 2668, 2732, 2782, 2842, 2894, 2954, 2984, 3000, 3028, 3336,
    3456, 3696, 3712, 3728, 3744, 3766, 3832, 3896, 3912, 3928, 3944, 3968, 4008, 4040, 4056, 4106,
    4138, 4170, 4202, 4234, 4266, 4296, 4312, 4344, 4408, 4424, 4442, 4472, 4488, 4504, 6148, 6198,
    6264, 6280, 6360, 6429, 6505, 6529, 61448, 61468, 61512, 61534, 61592, 61610, 61642, 61672,
    61688, 61704, 61726, 61784, 61800, 61816, 61836, 61880, 61896, 61914, 61948, 61998, 62062,
    62122, 62154, 62184, 62200, 62218, 62252, 62302, 62364, 62410, 62442, 62478, 62536, 62554,
    62584, 62604, 62640, 62648, 62656, 62664, 62730, 62766, 62830, 62890, 62924, 62974, 63032,
    63050, 63082, 63118, 63182, 63242, 63274, 63310, 63368, 63390,
];
const DIACRITIC_LETTERS: &[u8] = &[
    0, 97, 99, 101, 105, 110, 111, 117, 121, 121, 97, 99, 100, 101, 101, 103, 104, 105, 106, 107,
    108, 110, 111, 114, 115, 116, 117, 117, 119, 121, 122, 111, 117, 97, 105, 111, 117, 245, 225,
    103, 107, 111, 239, 106, 103, 110, 225, 97, 101, 105, 111, 114, 117, 115, 116, 104, 97, 101,
    239, 111, 239, 121, 0, 0, 0, 0, 0, 0, 0, 0, 97, 98, 227, 100, 100, 229, 101, 229, 102, 103,
    104, 104, 105, 233, 107, 108, 236, 108, 109, 110, 239, 112, 114, 242, 114, 115, 243, 116, 117,
    245, 118, 119, 119, 120, 121, 122, 104, 116, 119, 121, 97, 225, 225, 225, 101, 229, 229, 105,
    111, 239, 239, 239, 117, 245, 245, 121,
];
const FOLD_RULES: &[(u16, u8, u8)] = &[
    (65, 14, 26),
    (181, 64, 1),
    (192, 14, 23),
    (216, 14, 7),
    (256, 1, 48),
    (306, 1, 6),
    (313, 1, 16),
    (330, 1, 46),
    (376, 116, 1),
    (377, 1, 6),
    (383, 104, 1),
    (385, 50, 1),
    (386, 1, 4),
    (390, 44, 1),
    (391, 0, 1),
    (393, 42, 2),
    (395, 0, 1),
    (398, 32, 1),
    (399, 38, 1),
    (400, 40, 1),
    (401, 0, 1),
    (403, 42, 1),
    (404, 46, 1),
    (406, 52, 1),
    (407, 48, 1),
    (408, 0, 1),
    (412, 52, 1),
    (413, 54, 1),
    (415, 56, 1),
    (416, 1, 6),
    (422, 60, 1),
    (423, 0, 1),
    (425, 60, 1),
    (428, 0, 1),
    (430, 60, 1),
    (431, 0, 1),
    (433, 58, 2),
    (435, 1, 4),
    (439, 62, 1),
    (440, 0, 1),
    (444, 0, 1),
    (452, 2, 1),
    (453, 0, 1),
    (455, 2, 1),
    (456, 0, 1),
    (458, 2, 1),
    (459, 1, 18),
    (478, 1, 18),
    (497, 2, 1),
    (498, 1, 4),
    (502, 122, 1),
    (503, 134, 1),
    (504, 1, 40),
    (544, 110, 1),
    (546, 1, 18),
    (570, 70, 1),
    (571, 0, 1),
    (573, 108, 1),
    (574, 68, 1),
    (577, 0, 1),
    (579, 106, 1),
    (580, 28, 1),
    (581, 30, 1),
    (582, 1, 10),
    (837, 36, 1),
    (880, 1, 4),
    (886, 0, 1),
    (902, 18, 1),
    (904, 16, 3),
    (908, 26, 1),
    (910, 24, 2),
    (913, 14, 17),
    (931, 14, 9),
    (962, 0, 1),
    (975, 4, 1),
    (976, 140, 1),
    (977, 142, 1),
    (981, 146, 1),
    (982, 144, 1),
    (984, 1, 24),
    (1008, 136, 1),
    (1009, 138, 1),
    (1012, 130, 1),
    (1013, 128, 1),
    (1015, 0, 1),
    (1017, 152, 1),
    (1018, 0, 1),
    (1021, 110, 3),
    (1024, 34, 16),
    (1040, 14, 32),
    (1120, 1, 34),
    (1162, 1, 54),
    (1216, 6, 1),
    (1217, 1, 14),
    (1232, 1, 88),
    (1329, 22, 38),
    (4256, 66, 38),
    (4295, 66, 1),
    (4301, 66, 1),
    (7680, 1, 150),
    (7835, 132, 1),
    (7838, 96, 1),
    (7840, 1, 96),
    (7944, 150, 8),
    (7960, 150, 6),
    (7976, 150, 8),
    (7992, 150, 8),
    (8008, 150, 6),
    (8025, 151, 8),
    (8040, 150, 8),
    (8072, 150, 8),
    (8088, 150, 8),
    (8104, 150, 8),
    (8120, 150, 2),
    (8122, 126, 2),
    (8124, 148, 1),
    (8126, 100, 1),
    (8136, 124, 4),
    (8140, 148, 1),
    (8152, 150, 2),
    (8154, 120, 2),
    (8168, 150, 2),
    (8170, 118, 2),
    (8172, 152, 1),
    (8184, 112, 2),
    (8186, 114, 2),
    (8188, 148, 1),
    (8486, 98, 1),
    (8490, 92, 1),
    (8491, 94, 1),
    (8498, 12, 1),
    (8544, 8, 16),
    (8579, 0, 1),
    (9398, 10, 26),
    (11264, 22, 47),
    (11360, 0, 1),
    (11362, 88, 1),
    (11363, 102, 1),
    (11364, 90, 1),
    (11367, 1, 6),
    (11373, 84, 1),
    (11374, 86, 1),
    (11375, 80, 1),
    (11376, 82, 1),
    (11378, 0, 1),
    (11381, 0, 1),
    (11390, 78, 2),
    (11392, 1, 100),
    (11499, 1, 4),
    (11506, 0, 1),
    (42560, 1, 46),
    (42624, 1, 24),
    (42786, 1, 14),
    (42802, 1, 62),
    (42873, 1, 4),
    (42877, 76, 1),
    (42878, 1, 10),
    (42891, 0, 1),
    (42893, 74, 1),
    (42896, 1, 4),
    (42912, 1, 10),
    (42922, 72, 1),
    (65313, 14, 26),
];
const FOLD_OFFSETS: &[u16] = &[
    1, 2, 8, 15, 16, 26, 28, 32, 37, 38, 40, 48, 63, 64, 69, 71, 79, 80, 116, 202, 203, 205, 206,
    207, 209, 210, 211, 213, 214, 217, 218, 219, 775, 7264, 10792, 10795, 23228, 23256, 30204,
    54721, 54753, 54754, 54756, 54787, 54793, 54809, 57153, 57274, 57921, 58019, 58363, 61722,
    65268, 65341, 65373, 65406, 65408, 65410, 65415, 65424, 65436, 65439, 65450, 65462, 65472,
    65476, 65478, 65480, 65482, 65488, 65506, 65511, 65514, 65521, 65527, 65528, 65529,
];

/// Return zero for combining marks dropped by SQLite's exact legacy table.
pub(super) fn fold(mut code: u32) -> u32 {
    if code < 128 {
        if (65..=90).contains(&code) {
            code += 32;
        }
        return code;
    }
    if code < 65536 {
        let index = FOLD_RULES.partition_point(|&(start, _, _)| u32::from(start) <= code) - 1;
        let (start, flags, count) = FOLD_RULES[index];
        if code < u32::from(start) + u32::from(count)
            && (1 & u32::from(flags) & (u32::from(start) ^ code)) == 0
        {
            code = (code + u32::from(FOLD_OFFSETS[usize::from(flags >> 1)])) & 0xffff;
        }
        let key = (code << 3) | 7;
        let index = DIACRITIC_RANGES.partition_point(|&range| u32::from(range) <= key) - 1;
        let range = u32::from(DIACRITIC_RANGES[index]);
        if code <= (range >> 3) + (range & 7) {
            return u32::from(DIACRITIC_LETTERS[index] & 0x7f);
        }
        return code;
    }
    if (66560..66600).contains(&code) {
        code + 40
    } else {
        code
    }
}

/// Trigram positions count emitted tokens, after zero folded codepoints are skipped.
pub(super) fn trigram_iter(text: &str) -> impl Iterator<Item = String> + '_ {
    text.chars()
        .filter_map(|point| {
            // Rust strings exclude malformed UTF-8 and surrogates. SQLite 3.54's
            // amalgamation preserves other scalars, including noncharacters.
            char::from_u32(fold(point as u32)).filter(|&point| point != '\0')
        })
        .scan(
            std::collections::VecDeque::with_capacity(3),
            |points, point| {
                if points.len() == 3 {
                    points.pop_front();
                }
                points.push_back(point);
                Some((points.len() == 3).then(|| points.iter().collect()))
            },
        )
        .flatten()
}

pub(super) fn trigrams(text: &str) -> Vec<String> {
    trigram_iter(text).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uses_sqlites_legacy_fold_including_complex_diacritics() {
        assert_eq!(trigrams("ÀḈộ"), vec!["aco"]);
        assert_eq!(trigrams("A\u{301}BC"), vec!["abc"]);
        assert_eq!(trigrams("AB\0CD"), vec!["abc", "bcd"]);
        assert_eq!(trigrams("A\u{ffff}B"), vec!["a\u{ffff}b"]);
        assert_eq!(trigrams("A\u{fffe}B"), vec!["a\u{fffe}b"]);
        assert_eq!(trigrams("A\u{10ffff}B"), vec!["a\u{10ffff}b"]);
        assert_eq!(fold('ẞ' as u32), 'ß' as u32);
        assert_eq!(fold('İ' as u32), 'i' as u32);
        assert_eq!(fold('Σ' as u32), 'σ' as u32);
        assert_eq!(fold('ς' as u32), 'σ' as u32);
        assert_eq!(fold('ﬀ' as u32), 'ﬀ' as u32);
        assert_eq!(fold('𐐀' as u32), '𐐨' as u32);
    }
}
