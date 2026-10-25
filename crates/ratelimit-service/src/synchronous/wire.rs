//! The synchronous mode's messages on the wire, and their conversions to the owner's own
//! (`edgerush_ratelimit::synchronous`). As for the asynchronous mode's ([`crate::wire`]):
//! a field a reader does not know is skipped, and a message the owner cannot use — a key
//! without a contract, a request under no key or one key twice — is refused whole.

use crate::wire::{Contract, Invalid, View, contract};
use edgerush_ratelimit::local::Decision;
use edgerush_ratelimit::synchronous as owner;

/// A key with its contract.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Key {
    /// The key.
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    /// Its contract.
    #[prost(message, optional, tag = "2")]
    pub contract: Option<Contract>,
}

/// A request to decide, under its keys this owner holds.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Decide {
    /// Its number on the stream, for the answer to name.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// Its keys: at least one, each once.
    #[prost(message, repeated, tag = "2")]
    pub keys: Vec<Key>,
}

/// Charges to give back: a request admitted here and refused by another owner, or past its
/// data plane's time.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct GiveBack {
    /// The keys it was admitted under here.
    #[prost(message, repeated, tag = "1")]
    pub keys: Vec<Key>,
}

/// What a data plane sends an owner on its stream.
#[derive(Clone, PartialEq, Eq, prost::Oneof)]
pub enum ToOwner {
    /// A request to decide.
    #[prost(message, tag = "1")]
    Decide(Decide),
    /// Charges to give back.
    #[prost(message, tag = "2")]
    GiveBack(GiveBack),
}

/// One message on a data plane's stream to an owner.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Upstream {
    /// What it is.
    #[prost(oneof = "ToOwner", tags = "1, 2")]
    pub message: Option<ToOwner>,
}

/// An owner's answer to a request.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Decided {
    /// The request's number.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// Whether it was admitted.
    #[prost(bool, tag = "2")]
    pub admitted: bool,
    /// Refused, the key that waits longest, by its place in the request.
    #[prost(uint32, tag = "3")]
    pub position: u32,
    /// Refused, how long it waits, in nanoseconds.
    #[prost(uint64, tag = "4")]
    pub wait_ns: u64,
}

/// What an owner sends a data plane on its stream.
#[derive(Clone, PartialEq, Eq, prost::Oneof)]
pub enum FromOwner {
    /// An answer.
    #[prost(message, tag = "1")]
    Decided(Decided),
    /// The pods serving, as the pods agree: sent when the stream opens and when it
    /// changes, for the data plane to know each key's owner.
    #[prost(message, tag = "2")]
    View(View),
}

/// One message on an owner's stream to a data plane.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Downstream {
    /// What it is.
    #[prost(oneof = "FromOwner", tags = "1, 2")]
    pub message: Option<FromOwner>,
}

/// A key's state, copied from its owner.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Copy {
    /// The key.
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    /// Its contract.
    #[prost(message, optional, tag = "2")]
    pub contract: Option<Contract>,
    /// Nanoseconds from the copy until its bucket is full.
    #[prost(uint64, tag = "3")]
    pub full_in_ns: u64,
}

/// An owner's copies of the keys that changed in a cycle.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Copies {
    /// The pod they come from.
    #[prost(uint32, tag = "1")]
    pub from: u32,
    /// The keys.
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<Copy>,
}

/// Keys with their contracts, as the owner takes them.
type Keys = Vec<(Box<[u8]>, edgerush_ratelimit::service::Contract)>;

fn keys_of(keys: Vec<Key>) -> Result<Keys, Invalid> {
    keys.into_iter()
        .map(|key| Ok((key.key.into_boxed_slice(), contract(key.contract)?)))
        .collect()
}

fn keys_to(keys: Keys) -> Vec<Key> {
    keys.into_iter()
        .map(|(key, c)| Key {
            key: key.into_vec(),
            contract: Some(c.into()),
        })
        .collect()
}

impl TryFrom<Decide> for owner::Decide {
    type Error = Invalid;

    /// A request under no key would be admitted under nothing, and one under a key twice
    /// charged twice: both refused.
    fn try_from(decide: Decide) -> Result<Self, Invalid> {
        let keys = keys_of(decide.keys)?;
        if keys.is_empty() {
            return Err(Invalid::AskWithoutKeys);
        }
        for (at, (key, _)) in keys.iter().enumerate() {
            if keys[..at].iter().any(|(earlier, _)| earlier == key) {
                return Err(Invalid::KeyTwice);
            }
        }
        Ok(Self {
            id: decide.id,
            keys,
        })
    }
}

impl From<owner::Decide> for Decide {
    fn from(decide: owner::Decide) -> Self {
        Self {
            id: decide.id,
            keys: keys_to(decide.keys),
        }
    }
}

impl TryFrom<GiveBack> for owner::GiveBack {
    type Error = Invalid;

    fn try_from(back: GiveBack) -> Result<Self, Invalid> {
        Ok(Self {
            keys: keys_of(back.keys)?,
        })
    }
}

impl From<owner::GiveBack> for GiveBack {
    fn from(back: owner::GiveBack) -> Self {
        Self {
            keys: keys_to(back.keys),
        }
    }
}

impl From<owner::Decided> for Decided {
    fn from(decided: owner::Decided) -> Self {
        match decided.decision {
            Decision::Admit => Self {
                id: decided.id,
                admitted: true,
                position: 0,
                wait_ns: 0,
            },
            Decision::Refuse { position, wait } => Self {
                id: decided.id,
                admitted: false,
                position: u32::try_from(position).unwrap_or(u32::MAX),
                wait_ns: wait,
            },
        }
    }
}

impl From<Decided> for owner::Decided {
    fn from(decided: Decided) -> Self {
        Self {
            id: decided.id,
            decision: if decided.admitted {
                Decision::Admit
            } else {
                Decision::Refuse {
                    position: usize::try_from(decided.position).unwrap_or(usize::MAX),
                    wait: decided.wait_ns,
                }
            },
        }
    }
}

impl TryFrom<Copy> for owner::Copy {
    type Error = Invalid;

    fn try_from(copy: Copy) -> Result<Self, Invalid> {
        Ok(Self {
            key: copy.key.into_boxed_slice(),
            contract: contract(copy.contract)?,
            full_in: copy.full_in_ns,
        })
    }
}

impl From<owner::Copy> for Copy {
    fn from(copy: owner::Copy) -> Self {
        Self {
            key: copy.key.into_vec(),
            contract: Some(copy.contract.into()),
            full_in_ns: copy.full_in,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::num::NonZeroU64;

    fn contract() -> impl Strategy<Value = edgerush_ratelimit::service::Contract> {
        (1..u64::MAX, 1..u64::MAX, 1..u64::MAX).prop_map(|(limit, window, burst)| {
            let n = |n| NonZeroU64::new(n).unwrap();
            edgerush_ratelimit::service::Contract {
                limit: n(limit),
                window: n(window),
                burst: n(burst),
            }
        })
    }

    proptest! {
        #[test]
        fn a_request_goes_and_comes_back_as_it_was(
            id in any::<u64>(),
            keys in proptest::collection::btree_map(proptest::collection::vec(any::<u8>(), 0..40), contract(), 1..6),
        ) {
            let decide = owner::Decide { id, keys: keys.into_iter().map(|(k, c)| (k.into_boxed_slice(), c)).collect() };
            prop_assert_eq!(owner::Decide::try_from(Decide::from(decide.clone())).unwrap(), decide);
        }

        #[test]
        fn an_answer_and_a_copy_go_and_come_back_as_they_were(
            id in any::<u64>(),
            admitted in any::<bool>(),
            position in 0_usize..64,
            wait in any::<u64>(),
            full_in in any::<u64>(),
            c in contract(),
        ) {
            let decision = if admitted { Decision::Admit } else { Decision::Refuse { position, wait } };
            let decided = owner::Decided { id, decision };
            prop_assert_eq!(owner::Decided::from(Decided::from(decided)), decided);
            let copy = owner::Copy { key: b"k"[..].into(), contract: c, full_in };
            prop_assert_eq!(owner::Copy::try_from(Copy::from(copy.clone())).unwrap(), copy);
        }
    }

    #[test]
    fn a_request_under_no_key_or_one_twice_is_refused() {
        let key = |name: &[u8]| Key {
            key: name.to_vec(),
            contract: Some(Contract {
                limit: 1,
                window_ns: 1,
                burst: 1,
            }),
        };
        let none = Decide {
            id: 1,
            keys: vec![],
        };
        assert_eq!(owner::Decide::try_from(none), Err(Invalid::AskWithoutKeys));
        let twice = Decide {
            id: 1,
            keys: vec![key(b"a"), key(b"b"), key(b"a")],
        };
        assert_eq!(owner::Decide::try_from(twice), Err(Invalid::KeyTwice));
        let no_contract = Decide {
            id: 1,
            keys: vec![Key {
                key: b"a".to_vec(),
                contract: None,
            }],
        };
        assert_eq!(
            owner::Decide::try_from(no_contract),
            Err(Invalid::NoContract)
        );
    }
}
