const std = @import("std");

/// Adjectives for name generation (64 entries)
const adjectives = [_][]const u8{
    "Agile",    "Bold",     "Bright",   "Calm",
    "Clever",   "Cosmic",   "Crisp",    "Curious",
    "Daring",   "Devoted",  "Eager",    "Earnest",
    "Fearless", "Fierce",   "Fleet",    "Focused",
    "Friendly", "Gentle",   "Gleaming", "Graceful",
    "Happy",    "Hardy",    "Helpful",  "Honest",
    "Keen",     "Kind",     "Lively",   "Loyal",
    "Lucky",    "Merry",    "Mighty",   "Nimble",
    "Noble",    "Patient",  "Peaceful", "Plucky",
    "Polite",   "Proud",    "Quick",    "Quiet",
    "Rapid",    "Ready",    "Robust",   "Sharp",
    "Silent",   "Sleek",    "Smart",    "Snappy",
    "Solid",    "Speedy",   "Stable",   "Steady",
    "Still",    "Strong",   "Swift",    "Tender",
    "Tidy",     "Tough",    "Trusty",   "Valiant",
    "Vivid",    "Warm",     "Wise",     "Zesty",
};

/// Colors/modifiers for name generation (64 entries)
const colors = [_][]const u8{
    "Amber",    "Azure",    "Bronze",   "Coral",
    "Cobalt",   "Copper",   "Crimson",  "Cyan",
    "Diamond",  "Dusk",     "Ebony",    "Ember",
    "Emerald",  "Flint",    "Frost",    "Garnet",
    "Gold",     "Granite",  "Hazel",    "Indigo",
    "Iron",     "Ivory",    "Jade",     "Jasper",
    "Jet",      "Lapis",    "Lava",     "Lime",
    "Magenta",  "Marble",   "Mint",     "Moss",
    "Navy",     "Night",    "Oak",      "Obsidian",
    "Ocean",    "Olive",    "Onyx",     "Opal",
    "Pearl",    "Pine",     "Plum",     "Quartz",
    "Rose",     "Ruby",     "Rust",     "Sable",
    "Sage",     "Sand",     "Sapphire", "Scarlet",
    "Silver",   "Slate",    "Steel",    "Storm",
    "Sunset",   "Teal",     "Thunder",  "Topaz",
    "Umber",    "Velvet",   "Violet",   "Zinc",
};

/// Animals for name generation (64 entries)
const animals = [_][]const u8{
    "Ant",      "Badger",   "Bear",     "Bee",
    "Bison",    "Bobcat",   "Camel",    "Cat",
    "Cheetah",  "Cobra",    "Condor",   "Cougar",
    "Coyote",   "Crane",    "Crow",     "Deer",
    "Dolphin",  "Dove",     "Eagle",    "Elk",
    "Falcon",   "Finch",    "Fox",      "Frog",
    "Gecko",    "Goat",     "Goose",    "Gopher",
    "Hawk",     "Heron",    "Horse",    "Hound",
    "Jackal",   "Jay",      "Koala",    "Lark",
    "Lemur",    "Lion",     "Lizard",   "Lynx",
    "Mole",     "Moose",    "Mouse",    "Newt",
    "Otter",    "Owl",      "Panda",    "Parrot",
    "Pelican",  "Penguin",  "Pigeon",   "Puma",
    "Rabbit",   "Raven",    "Robin",    "Salmon",
    "Seal",     "Shark",    "Sloth",    "Snail",
    "Snake",    "Sparrow",  "Squid",    "Stork",
    "Swan",     "Tiger",    "Toad",     "Toucan",
    "Turtle",   "Viper",    "Whale",    "Wolf",
};

/// Buffer for runtime name generation (max ~25 chars: "Fearless Obsidian Penguin")
var name_buffer: [32]u8 = undefined;

/// Decode Crockford base32 character to 5-bit value (0-31).
/// Returns 0 for invalid characters.
fn decodeBase32(c: u8) u5 {
    return switch (c) {
        '0', 'O', 'o' => 0,
        '1', 'I', 'i', 'L', 'l' => 1,
        '2' => 2,
        '3' => 3,
        '4' => 4,
        '5' => 5,
        '6' => 6,
        '7' => 7,
        '8' => 8,
        '9' => 9,
        'A', 'a' => 10,
        'B', 'b' => 11,
        'C', 'c' => 12,
        'D', 'd' => 13,
        'E', 'e' => 14,
        'F', 'f' => 15,
        'G', 'g' => 16,
        'H', 'h' => 17,
        'J', 'j' => 18,
        'K', 'k' => 19,
        'M', 'm' => 20,
        'N', 'n' => 21,
        'P', 'p' => 22,
        'Q', 'q' => 23,
        'R', 'r' => 24,
        'S', 's' => 25,
        'T', 't' => 26,
        'V', 'v' => 27,
        'W', 'w' => 28,
        'X', 'x' => 29,
        'Y', 'y' => 30,
        'Z', 'z' => 31,
        else => 0,
    };
}

/// Generate a memorable name from a ULID.
/// Uses randomness portion (positions 10-13) decoded from base32.
/// Combines 4 base32 chars (18 bits) to uniformly index 262,144 combinations.
/// Returns a slice into a static buffer (valid until next call).
pub fn fromUlid(ulid: []const u8) []const u8 {
    if (ulid.len < 14) return "Unknown Agent";

    // Decode 4 base32 chars from random portion to get 18 bits.
    // ULID format: 10 chars timestamp + 16 chars random (Crockford base32).
    const b0 = @as(u18, decodeBase32(ulid[10]));
    const b1 = @as(u18, decodeBase32(ulid[11]));
    const b2 = @as(u18, decodeBase32(ulid[12]));
    const b3 = @as(u18, decodeBase32(ulid[13]));

    // Combine to 18-bit value: 5 + 5 + 5 + 3 bits = 18 bits (0-262143)
    const combined = (b0 << 13) | (b1 << 8) | (b2 << 3) | (b3 >> 2);
    const total = adjectives.len * colors.len * animals.len;
    const idx = combined % total;

    // Decompose index into adjective/color/animal indices
    const adj_idx = idx / (colors.len * animals.len);
    const remainder = idx % (colors.len * animals.len);
    const col_idx = remainder / animals.len;
    const ani_idx = remainder % animals.len;

    // Build name at runtime: "Adjective Color Animal"
    const adj = adjectives[adj_idx];
    const col = colors[col_idx];
    const ani = animals[ani_idx];

    var pos: usize = 0;
    @memcpy(name_buffer[pos..][0..adj.len], adj);
    pos += adj.len;
    name_buffer[pos] = ' ';
    pos += 1;
    @memcpy(name_buffer[pos..][0..col.len], col);
    pos += col.len;
    name_buffer[pos] = ' ';
    pos += 1;
    @memcpy(name_buffer[pos..][0..ani.len], ani);
    pos += ani.len;

    return name_buffer[0..pos];
}

test "name generation is deterministic" {
    // Use 26-char ULIDs (standard format)
    const name1 = fromUlid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    const name2 = fromUlid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    try std.testing.expectEqualStrings(name1, name2);
}

test "different ULIDs produce different names" {
    // Different random portions (chars 10+) produce different names
    const name1 = fromUlid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    const name2 = fromUlid("01ARZ3NDEKABCDEFGHIJKLMNOP");
    try std.testing.expect(!std.mem.eql(u8, name1, name2));
}

test "short input returns fallback" {
    const name = fromUlid("01ABCDEF");
    try std.testing.expectEqualStrings("Unknown Agent", name);
}

test "name has three words" {
    const name = fromUlid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    var spaces: usize = 0;
    for (name) |c| {
        if (c == ' ') spaces += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), spaces);
}
