//! A fair share of something a worker has only so much of, among those that use it
//! ([03 §9](../../../docs/03-data-plane.md)): the places for exchanges among upstreams, and
//! the connections among listeners.
//!
//! Short of the last one, what is shared would go to whoever asks first, and one user that
//! holds on — an upstream that stops answering, a listener being flooded — would take it
//! all. So once seven eighths are held, a user that already holds its fair share is
//! refused. The fair share is what there is, split equally among the users holding any,
//! the one asking counted, and never among fewer than two unless it is the only user
//! there can be. A user that holds something only for a moment holds nothing most of the
//! time, and counted by what it holds it would leave the one that holds on alone, free to
//! take everything: with the floor, the last eighth stays for whoever else comes. A user
//! under its share is never refused while any is free, the only user there can be may take
//! everything, and nothing is sized to what is behind a user.
//!
//! Pure: what is held is counted by the caller.

/// Whether a user that holds `holds` of the `held` of `limit` is refused for holding its
/// fair share, `holding` being how many users hold any and `alone` whether it is the only
/// user there can be. A `held` at `limit` is the caller's to refuse whoever asks.
#[must_use]
pub fn over_share(limit: usize, held: usize, holds: usize, holding: usize, alone: bool) -> bool {
    if held < limit - limit / 8 {
        return false;
    }
    let sharing = (holding + usize::from(holds == 0)).max(2 - usize::from(alone));
    // Never divides by zero: `sharing` is at least one.
    holds >= limit / sharing
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nobody_is_refused_before_seven_eighths_are_held() {
        assert!(!over_share(16, 13, 13, 1, false));
        assert!(!over_share(1024, 895, 895, 1, false));
        assert!(over_share(1024, 896, 896, 1, false));
    }

    #[test]
    fn once_short_a_user_at_its_share_is_refused_and_one_under_it_is_not() {
        // Two holding: 8 each of 16.
        assert!(over_share(16, 14, 8, 2, false));
        assert!(!over_share(16, 14, 7, 2, false));
        // One holding none asks: three sharing, 5 each.
        assert!(!over_share(16, 14, 0, 2, false));
        assert!(over_share(16, 15, 5, 3, false));
    }

    #[test]
    fn never_fewer_than_two_share_unless_the_user_is_alone() {
        // The only one holding, with others that could come: at most half once short.
        assert!(over_share(16, 14, 14, 1, false));
        // The only one there can be: everything.
        assert!(!over_share(16, 15, 15, 1, true));
    }

    #[test]
    fn small_limits_are_never_short_before_they_are_full() {
        for limit in 1..8 {
            assert!(!over_share(limit, limit.saturating_sub(1), 0, 1, false));
        }
    }
}
