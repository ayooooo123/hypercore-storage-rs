//! Flat-tree index arithmetic (the parts `hypercore-storage` needs).

/// `flat-tree` `rightSpan`: the highest index of the subtree rooted at `index`.
pub fn right_span(index: u64) -> u64 {
    if index & 1 == 0 {
        return index;
    }
    // `depth(index)` is the number of trailing one bits; the subtree covers
    // `1 << depth` leaves.
    index + (1u64 << index.trailing_ones()) - 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spans_match_flat_tree() {
        // Values from `require('flat-tree').rightSpan`.
        for (index, span) in [
            (0, 0),
            (1, 2),
            (2, 2),
            (3, 6),
            (5, 6),
            (7, 14),
            (9, 10),
            (11, 14),
        ] {
            assert_eq!(right_span(index), span, "index {index}");
        }
    }
}
