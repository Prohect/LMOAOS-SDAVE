//! # SDAVE
//!
//! **S**treamable **D**elimiter-**A**daptive **V**erbatim **E**nvelope protocol.
//!
//! SDAVE is a fundamental, flat (nesting-agnostic, payload-agnostic)
//! serialization protocol. It reserves a serialize-time-defined delimiter
//! slice from the payload and decodes the payload verbatim — byte-identical
//! to what was written. From one slice (channel) mixing serialized envelopes,
//! potentially streaming envelopes and non-envelope slices, it tells them
//! apart, and those non-envelope slices are not considered unexpected results.
//!
//! An envelope is:
//!
//! ```text
//! <limiter slice><payload><delimiter slice>
//! ```
//!
//! where the limiter slice is `n` repeats (`n >= least_repeat`) of a
//! [`LimiterPair::limiter`], and the delimiter slice is exactly `n` repeats of
//! the matching [`LimiterPair::delimiter`]. Because the repeat count `n` is
//! chosen at serialize time, the writer can always pick an `n` that does not
//! collide with the payload — no escaping, verbatim payload.
//!
//! This crate never owns the buffer. The application controls the buffer
//! lifetime and tells SDAVE about (monotonically growing) slices; SDAVE
//! answers with offsets into the passed slice. See [`get_envelop`] and
//! [`get_envelop_incremental`].
//!
//! As a fundamental lib, no safe fn of SDAVE ever panics. Only well
//! documented `unsafe fn`s may panic (on contract violation).

use std::num::NonZeroUsize;

/// One of a configurable, SDAVE-parse-time-fixed set of limiter pairs.
///
/// The limiter slice is `least_repeat` or more repeats of `limiter`;
/// the matching delimiter slice is `delimiter` repeated the same number
/// of times.
///
/// For `u8` buffers in agentic harnesses, ASCII only uses `0x00..0x7F` and
/// an LLM can output UTF-8, so it is best practice to reserve UTF-8-only
/// chars; see [`is_recommended_set`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimiterPair<T> {
    pub limiter: T,
    pub delimiter: T,
    /// Minimum repeat count of `limiter` for a run to open an envelope.
    /// Meant to be `>= 1`; `0` behaves exactly like `1` (a run always has
    /// length `>= 1`).
    pub least_repeat: u32,
}

impl<T> LimiterPair<T> {
    pub const fn new(limiter: T, delimiter: T, least_repeat: u32) -> Self {
        Self {
            limiter,
            delimiter,
            least_repeat,
        }
    }
}

/// Delimiter match algorithm. There is not a full winner; choose based on
/// the scene.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    /// The every first full matched delimiter slice is just the delimiter
    /// slice. No delimiter slice confirmation latency, but the tail of the
    /// payload MUST NOT collide with the delimiter. This limiter pair mental
    /// burden addon is NOT solvable on application level.
    ///
    /// Head and tail of payload, plus the tail of existing contents (if NOT
    /// ended by a complete variant1 envelope) in the mixed buffer before this
    /// envelope, can at most each disable one limiter pair: at most 3 limiter
    /// pairs disabled per envelope. Two contiguous envelopes can share the
    /// same limiter pair.
    V1,
    /// The every first full matched delimiter slice FOLLOWED by a
    /// non-delimiter is the delimiter slice. The tail of the payload won't
    /// disable a limiter pair, but this introduces delimiter slice
    /// confirmation latency. The latency is solvable on application level, by
    /// automatically pushing a non-delimiter to the mixed channel.
    ///
    /// Head of payload and tail of existing contents in the mixed buffer
    /// before this envelope can at most each disable one limiter pair: at
    /// most 2 limiter pairs disabled per envelope. Two contiguous envelopes
    /// can share a limiter pair whose limiter != delimiter — e.g.
    /// `0^^1~~^^2~~3` is well defined. (Sharing a pair whose limiter ==
    /// delimiter merges the two envelopes into one, since the delimiter run
    /// and the next limiter run fuse.)
    V2,
}

/// A non-envelope slice of the mixed buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonEnvelop {
    pub head_offset: usize,
    /// Exclusive end. Can monotonic increase on next update while this slice
    /// is still the front slice of the buffer.
    pub tail_offset: usize,
}

impl NonEnvelop {
    /// Shift all offsets down by `offset`, after the application drained
    /// `offset` elements from the front of the buffer.
    ///
    /// # Safety
    /// `offset` must not exceed `head_offset`; every offset of this slice
    /// must stay valid after the shift. Panics on contract violation.
    pub unsafe fn offset(&mut self, offset: usize) {
        self.head_offset = self
            .head_offset
            .checked_sub(offset)
            .expect("offset must not exceed head_offset");
        self.tail_offset = self
            .tail_offset
            .checked_sub(offset)
            .expect("offset must not exceed tail_offset");
    }
}

/// A serialized envelope (or a potentially streaming one, see
/// [`State::Pending`]).
///
/// All offsets are relative to the slice the [`State`] was computed from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelop {
    /// Offset of the first element of the limiter slice.
    pub head_offset: usize,
    /// Offset of the first payload element. `Some` when and only when well
    /// defined (at least one non-limiter `T` exists following the limiter
    /// slice).
    pub payload_head_offset: Option<NonZeroUsize>,
    /// Exclusive end offset of the payload.
    ///
    /// For [`Variant::V1`], this is always `None` while
    /// [`State::Pending`]. For [`Variant::V2`], it can be `Some` when and only
    /// when the delimiter slice MUST exist but its exact offset is not
    /// confirmed yet, and thus its value can monotonic increase on next
    /// update.
    pub payload_tail_offset: Option<NonZeroUsize>,
    /// Exclusive end offset of the whole envelope (one past the delimiter
    /// slice). `Some` when and only when [`State::Ok`].
    pub tail_offset: Option<NonZeroUsize>,
}

impl Envelop {
    /// Shift all offsets down by `offset`, after the application drained
    /// `offset` elements from the front of the buffer.
    ///
    /// # Safety
    /// `offset` must not exceed `head_offset`; every present offset of this
    /// envelope must stay valid (`payload_*_offset` and `tail_offset` must
    /// stay non-zero) after the shift. Panics on contract violation.
    pub unsafe fn offset(&mut self, offset: usize) {
        self.head_offset = self
            .head_offset
            .checked_sub(offset)
            .expect("offset must not exceed head_offset");
        self.payload_head_offset = self.payload_head_offset.map(|v| {
            NonZeroUsize::new(v.get() - offset).expect("offset must keep payload_head_offset valid")
        });
        self.payload_tail_offset = self.payload_tail_offset.map(|v| {
            NonZeroUsize::new(v.get() - offset).expect("offset must keep payload_tail_offset valid")
        });
        self.tail_offset = self.tail_offset.map(|v| {
            NonZeroUsize::new(v.get() - offset).expect("offset must keep tail_offset valid")
        });
    }
}

/// What the front of the passed slice currently is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// A non-envelope slice.
    No(NonEnvelop),
    /// `s[usize..]` are potentially still defining a limiter slice; `usize`
    /// is the every first offset of the potential limiter slice.
    ///
    /// `s[usize..]` can still turn out to be a [`State::No`] if later
    /// followed by a not-same `T` AND its length does not reach
    /// `least_repeat`.
    OnLimiter(usize),
    /// A potentially streaming envelope: a valid limiter slice exists, but
    /// the matched delimiter slice has not (fully) arrived yet.
    ///
    /// WARNING: PENDING contents can later become PENDING or a valid envelope,
    /// or the stream may end before the envelope completes; in that case the
    /// application should handle the malformed tail data.
    Pending(Envelop),
    /// A complete envelope.
    ///
    /// When and only when an empty payload was serialized with a
    /// [`LimiterPair`] whose limiter != delimiter, an Ok envelope's
    /// `payload_head_offset` and `payload_tail_offset` are `None`.
    /// (An empty payload MUST NOT be serialized with a [`LimiterPair`] whose
    /// limiter == delimiter.)
    Ok(Envelop),
}

impl State {
    /// Offset of the first element of this item, whatever variant the state
    /// is. Comparing it across an [`get_envelop_incremental`] call tells
    /// whether parsing advanced to the next item.
    pub fn head_offset(&self) -> usize {
        match self {
            State::No(non) => non.head_offset,
            State::OnLimiter(head) => *head,
            State::Pending(envelop) | State::Ok(envelop) => envelop.head_offset,
        }
    }

    /// Shift all offsets down by `offset`, after the application drained
    /// `offset` elements from the front of the buffer.
    ///
    /// # Safety
    /// Same contract as [`Envelop::offset`] / [`NonEnvelop::offset`]. Panics
    /// on contract violation.
    pub unsafe fn offset(&mut self, offset: usize) {
        match self {
            State::No(non) => unsafe { non.offset(offset) },
            State::OnLimiter(head) => {
                *head = head
                    .checked_sub(offset)
                    .expect("offset must not exceed limiter slice head")
            }
            State::Pending(envelop) | State::Ok(envelop) => unsafe { envelop.offset(offset) },
        }
    }
}

/// Best-practice check for a limiter pair set (recommended for agentic
/// harness usage):
///
/// - every pair has limiter != delimiter;
/// - no limiter of the set equals any delimiter of the set;
/// - no two distinct pairs share a limiter or a delimiter.
///
/// Sharing one limiter between pairs is not recommended, but the safe
/// parse fns still support it (see [`get_envelop`]). The `unsafe`
/// `_unchecked` variants may assume this check holds.
pub fn is_recommended_set<T: PartialEq>(limiter_pairs: &[LimiterPair<T>]) -> bool {
    limiter_pairs.iter().all(|p| p.limiter != p.delimiter)
        && !limiter_pairs
            .iter()
            .any(|p| limiter_pairs.iter().any(|q| p.limiter == q.delimiter))
        && !limiter_pairs.iter().enumerate().any(|(i, p)| {
            limiter_pairs
                .iter()
                .skip(i + 1)
                .any(|q| p.limiter == q.limiter || p.delimiter == q.delimiter)
        })
}

/// Length of the run of `s[from]` starting at `from`. `from < s.len()`.
fn run_len<T: PartialEq>(s: &[T], from: usize) -> usize {
    let mut n = 1;
    while from + n < s.len() && s[from + n] == s[from] {
        n += 1;
    }
    n
}

fn is_limiter<T: PartialEq>(limiter_pairs: &[LimiterPair<T>], c: &T) -> bool {
    limiter_pairs.iter().any(|p| &p.limiter == c)
}

/// The pair opening an envelope for a run of `c` of length `r`.
///
/// Not recommended (see [`is_recommended_set`]) but supported: several pairs
/// may share the limiter `c`. The safe resolution (`recommended == false`)
/// picks the first pair in set order whose limiter is `c` and whose
/// `least_repeat <= r`. The `recommended == true` resolution relies on the
/// application asserting unique limiters: the first pair whose limiter is
/// `c` is THE pair, and the run must reach its `least_repeat`.
fn pair_for_run<'a, T: PartialEq>(
    limiter_pairs: &'a [LimiterPair<T>],
    c: &T,
    r: usize,
    recommended: bool,
) -> Option<&'a LimiterPair<T>> {
    if recommended {
        limiter_pairs
            .iter()
            .find(|p| &p.limiter == c)
            .filter(|p| (p.least_repeat as usize) <= r)
    } else {
        limiter_pairs
            .iter()
            .find(|p| &p.limiter == c && (p.least_repeat as usize) <= r)
    }
}

/// First index `>= from` where a potential limiter slice starts: a limiter
/// run that is either confirmed (terminated by a non-limiter and long
/// enough) or still growing at the end of the buffer. Returns `s.len()` when
/// there is none.
fn scan_non_envelop<T: PartialEq>(
    s: &[T],
    limiter_pairs: &[LimiterPair<T>],
    from: usize,
    recommended: bool,
) -> usize {
    let mut i = from;
    while i < s.len() {
        if is_limiter(limiter_pairs, &s[i]) {
            let end = i + run_len(s, i);
            if end == s.len() || pair_for_run(limiter_pairs, &s[i], end - i, recommended).is_some()
            {
                return i;
            }
            // Terminated run shorter than every matching least_repeat:
            // plain non-envelope contents.
            i = end;
        } else {
            i += 1;
        }
    }
    s.len()
}

fn ok_envelop(head: usize, payload_start: usize, payload_end: usize, end: usize) -> State {
    if payload_end == payload_start {
        // Empty payload; only possible when limiter != delimiter.
        State::Ok(Envelop {
            head_offset: head,
            payload_head_offset: None,
            payload_tail_offset: None,
            tail_offset: NonZeroUsize::new(end),
        })
    } else {
        State::Ok(Envelop {
            head_offset: head,
            payload_head_offset: NonZeroUsize::new(payload_start),
            payload_tail_offset: NonZeroUsize::new(payload_end),
            tail_offset: NonZeroUsize::new(end),
        })
    }
}

/// Parse an envelope whose limiter slice starts at `head`, has length `n`,
/// and is already terminated (a non-limiter `T` follows the run).
/// Returns [`State::Pending`] or [`State::Ok`].
///
/// `head < s.len()` and `head + n < s.len()`.
fn parse_envelop<T: PartialEq>(
    s: &[T],
    pair: &LimiterPair<T>,
    variant: Variant,
    head: usize,
    n: usize,
) -> State {
    let payload_start = head + n;
    let mut q = payload_start;
    while q < s.len() {
        if s[q] == pair.delimiter {
            let run_end = q + run_len(s, q);
            match variant {
                Variant::V1 => {
                    if run_end - q >= n {
                        // The every first full matched delimiter slice: the
                        // first `n` repeats of the run.
                        return ok_envelop(head, payload_start, q, q + n);
                    }
                    q = run_end;
                }
                Variant::V2 => {
                    if run_end == s.len() {
                        // Trailing delimiter run, confirmation latency. When
                        // the run already holds a full delimiter slice, the
                        // delimiter MUST exist but its exact offset is not
                        // confirmed yet.
                        return State::Pending(Envelop {
                            head_offset: head,
                            payload_head_offset: NonZeroUsize::new(payload_start),
                            payload_tail_offset: if run_end - q >= n {
                                NonZeroUsize::new(run_end - n)
                            } else {
                                None
                            },
                            tail_offset: None,
                        });
                    }
                    if run_end - q >= n {
                        // The delimiter slice followed by a non-delimiter:
                        // the last `n` repeats of the run.
                        return ok_envelop(head, payload_start, run_end - n, run_end);
                    }
                    q = run_end;
                }
            }
        } else {
            q += 1;
        }
    }
    State::Pending(Envelop {
        head_offset: head,
        payload_head_offset: NonZeroUsize::new(payload_start),
        payload_tail_offset: None,
        tail_offset: None,
    })
}

/// Parse the item starting at `head` (`head <= s.len()`).
fn parse_front<T: PartialEq>(
    s: &[T],
    limiter_pairs: &[LimiterPair<T>],
    variant: Variant,
    head: usize,
    recommended: bool,
) -> State {
    let i = scan_non_envelop(s, limiter_pairs, head, recommended);
    if i > head || head == s.len() {
        return State::No(NonEnvelop {
            head_offset: head,
            tail_offset: i,
        });
    }
    // `s[head..]` starts with a limiter run.
    let n = run_len(s, head);
    match pair_for_run(limiter_pairs, &s[head], n, recommended) {
        Some(pair) if head + n < s.len() => parse_envelop(s, pair, variant, head, n),
        // Run still growing at the end of the buffer: its length (and thus
        // the delimiter slice) is not defined yet.
        _ => State::OnLimiter(head),
    }
}

fn get_envelop_impl<T: PartialEq>(
    s: &[T],
    limiter_pairs: &[LimiterPair<T>],
    variant: Variant,
    recommended: bool,
) -> State {
    parse_front(s, limiter_pairs, variant, 0, recommended)
}

fn get_envelop_incremental_impl<T: PartialEq>(
    s: &[T],
    limiter_pairs: &[LimiterPair<T>],
    variant: Variant,
    state: &State,
    recommended: bool,
) -> State {
    let len = s.len();
    match state {
        // Final item followed by unread buffer: parse the next item.
        State::Ok(envelop) => match envelop.tail_offset {
            Some(tail) if tail.get() < len => {
                parse_front(s, limiter_pairs, variant, tail.get(), recommended)
            }
            // Nothing after it (or corrupt state): unchanged.
            _ => state.clone(),
        },
        State::No(non) => {
            let head = non.head_offset.min(len);
            let tail = non.tail_offset.min(len);
            let next = scan_non_envelop(s, limiter_pairs, tail, recommended);
            if next == tail && tail < len {
                // The non-envelope slice did not change and is followed by
                // another (potential) item: parse it.
                parse_front(s, limiter_pairs, variant, tail, recommended)
            } else {
                State::No(NonEnvelop {
                    head_offset: head,
                    tail_offset: next,
                })
            }
        }
        State::OnLimiter(start) => {
            let start = *start;
            if start >= len {
                // Corrupt state: recover by re-parsing from the front.
                return parse_front(s, limiter_pairs, variant, 0, recommended);
            }
            let n = run_len(s, start);
            if start + n == len {
                return State::OnLimiter(start);
            }
            match pair_for_run(limiter_pairs, &s[start], n, recommended) {
                Some(pair) => parse_envelop(s, pair, variant, start, n),
                // Run terminated below every matching least_repeat: the whole
                // run is plain non-envelope content.
                None => State::No(NonEnvelop {
                    head_offset: start,
                    tail_offset: scan_non_envelop(s, limiter_pairs, start + n, recommended),
                }),
            }
        }
        State::Pending(envelop) => {
            let head = envelop.head_offset;
            if head >= len {
                // Corrupt state: recover by re-parsing from the front.
                return parse_front(s, limiter_pairs, variant, 0, recommended);
            }
            let n = run_len(s, head);
            match pair_for_run(limiter_pairs, &s[head], n, recommended) {
                Some(pair) if head + n < len => parse_envelop(s, pair, variant, head, n),
                // Corrupt state (a once-terminated limiter slice cannot
                // un-terminate / re-grow): recover from the front.
                _ => parse_front(s, limiter_pairs, variant, 0, recommended),
            }
        }
    }
}

/// From the every start of `s`, tell what the front slice is: once a valid
/// limiter slice exists, this fn only cares about its matched delimiter
/// slice — if there is a matched delimiter slice, then [`State::Ok`],
/// otherwise [`State::Pending`]; if no valid limiter slice, [`State::No`] (or
/// [`State::OnLimiter`] while a limiter run is still growing at the end of
/// the buffer).
///
/// All offsets of the returned [`State`] are relative to the start of `s`.
///
/// Any limiter pair set is supported, including ones breaking the
/// [`is_recommended_set`] best practice (e.g. several pairs sharing one
/// limiter; the first matching pair in set order wins). Never panics.
pub fn get_envelop<T: PartialEq>(
    s: &[T],
    limiter_pairs: &[LimiterPair<T>],
    variant: Variant,
) -> State {
    get_envelop_impl(s, limiter_pairs, variant, false)
}

/// Incremental version of [`get_envelop`]: `s` is the same buffer the
/// previous `state` was computed from, with zero or more elements appended,
/// and the update of `state` is returned.
///
/// When the previous state is confirmed to not need an update — it is final
/// and followed by another item, i.e. the buffer is not all read — the next
/// item is parsed instead. Detect this by comparing head offsets:
///
/// ```rust
/// # use sdave::*;
/// # let pairs = [LimiterPair::new(b'%', b'%', 2u32)];
/// # let variant = Variant::V1;
/// let buf = b"%%A%%rest".as_slice();
/// let state = get_envelop(&buf[..5], &pairs, variant); // Ok envelope `%%A%%`
/// let next = get_envelop_incremental(buf, &pairs, variant, &state);
/// if next.head_offset() != state.head_offset() {
///     // `state` is finalized; `next` describes the following item.
/// }
/// # assert_eq!(next.head_offset(), 5);
/// ```
///
/// Per-variant update rules (while the item is still the front item):
///
/// - [`State::No`]: `tail_offset` can monotonic increase.
/// - [`State::OnLimiter`]: stays OnLimiter while the run grows; becomes
///   [`State::Pending`]/[`State::Ok`] once the limiter slice is terminated
///   and long enough, or [`State::No`] when the run turns out too short.
/// - [`State::Pending`]: continues the delimiter slice search; for
///   [`Variant::V2`], `payload_tail_offset` can monotonic increase.
/// - [`State::Ok`]: final; never updated in place.
///
/// The buffer is expected to be the very same one `state` was computed from
/// (contents at existing offsets unchanged, only appended to), or `state`
/// shifted with [`State::offset`] to match. A state inconsistent with `s`
/// is recovered by re-parsing from the front; never panics.
pub fn get_envelop_incremental<T: PartialEq>(
    s: &[T],
    limiter_pairs: &[LimiterPair<T>],
    variant: Variant,
    state: &State,
) -> State {
    get_envelop_incremental_impl(s, limiter_pairs, variant, state, false)
}

/// [`get_envelop`], with the application asserting the limiter pair set
/// obeys the [`is_recommended_set`] best practice, allowing faster limiter
/// pair lookup.
///
/// # Safety
/// `is_recommended_set(limiter_pairs)` must hold. With a set breaking the
/// recommendation (e.g. several pairs sharing one limiter), the returned
/// state is unspecified. Never panics.
pub unsafe fn get_envelop_unchecked<T: PartialEq>(
    s: &[T],
    limiter_pairs: &[LimiterPair<T>],
    variant: Variant,
) -> State {
    get_envelop_impl(s, limiter_pairs, variant, true)
}

/// [`get_envelop_incremental`], with the application asserting the limiter
/// pair set obeys the [`is_recommended_set`] best practice, allowing faster
/// limiter pair lookup.
///
/// # Safety
/// `is_recommended_set(limiter_pairs)` must hold. With a set breaking the
/// recommendation (e.g. several pairs sharing one limiter), the returned
/// state is unspecified. Never panics.
pub unsafe fn get_envelop_incremental_unchecked<T: PartialEq>(
    s: &[T],
    limiter_pairs: &[LimiterPair<T>],
    variant: Variant,
    state: &State,
) -> State {
    get_envelop_incremental_impl(s, limiter_pairs, variant, state, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `[('^', '~', 2), ('?', '!', 2), ('%', '%', 2), ('{', '}', 2)]`
    fn pairs() -> Vec<LimiterPair<u8>> {
        vec![
            LimiterPair::new(b'^', b'~', 2),
            LimiterPair::new(b'?', b'!', 2),
            LimiterPair::new(b'%', b'%', 2),
            LimiterPair::new(b'{', b'}', 2),
        ]
    }

    /// Bad example: `A.delimiter == B.limiter`.
    fn bad_pairs() -> Vec<LimiterPair<u8>> {
        vec![LimiterPair::new(b'^', b'~', 2), LimiterPair::new(b'~', b'%', 2)]
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Seg {
        Non(Vec<u8>),
        Payload(Vec<u8>),
        Pending(Vec<u8>),
        OnLimiter,
    }

    fn payload_slice<'a>(s: &'a [u8], e: &Envelop) -> &'a [u8] {
        match (e.payload_head_offset, e.payload_tail_offset) {
            (None, None) => &[],
            (Some(h), Some(t)) => &s[h.get()..t.get()],
            (Some(h), None) => &s[h.get()..],
            (None, Some(_)) => unreachable!(),
        }
    }

    fn seg_of(s: &[u8], st: &State) -> Seg {
        match st {
            State::No(n) => Seg::Non(s[n.head_offset..n.tail_offset].to_vec()),
            State::Ok(e) => Seg::Payload(payload_slice(s, e).to_vec()),
            State::Pending(e) => Seg::Pending(payload_slice(s, e).to_vec()),
            State::OnLimiter(_) => Seg::OnLimiter,
        }
    }

    fn get(s: &[u8], pairs: &[LimiterPair<u8>], variant: Variant, trusted: bool) -> State {
        if trusted {
            unsafe { get_envelop_unchecked(s, pairs, variant) }
        } else {
            get_envelop(s, pairs, variant)
        }
    }

    fn get_inc(
        s: &[u8],
        pairs: &[LimiterPair<u8>],
        variant: Variant,
        state: &State,
        trusted: bool,
    ) -> State {
        if trusted {
            unsafe { get_envelop_incremental_unchecked(s, pairs, variant, state) }
        } else {
            get_envelop_incremental(s, pairs, variant, state)
        }
    }

    /// One-shot driver: repeatedly parse the front slice and consume it.
    fn parse_all(s: &[u8], pairs: &[LimiterPair<u8>], variant: Variant, trusted: bool) -> Vec<Seg> {
        let mut segs = Vec::new();
        let mut rest = s;
        loop {
            match get(rest, pairs, variant, trusted) {
                State::No(non) => {
                    if non.tail_offset == 0 {
                        break;
                    }
                    segs.push(Seg::Non(rest[..non.tail_offset].to_vec()));
                    rest = &rest[non.tail_offset..];
                }
                State::OnLimiter(_) => {
                    segs.push(Seg::OnLimiter);
                    break;
                }
                State::Pending(e) => {
                    segs.push(Seg::Pending(payload_slice(rest, &e).to_vec()));
                    break;
                }
                State::Ok(e) => {
                    segs.push(Seg::Payload(payload_slice(rest, &e).to_vec()));
                    rest = &rest[e.tail_offset.unwrap().get()..];
                    if rest.is_empty() {
                        break;
                    }
                }
            }
        }
        segs
    }

    /// Incremental driver: feed one element at a time, updating the state
    /// with the incremental fn; finalized items are emitted when the head
    /// offset advances.
    fn parse_all_incremental(
        s: &[u8],
        pairs: &[LimiterPair<u8>],
        variant: Variant,
        trusted: bool,
    ) -> Vec<Seg> {
        let mut segs = Vec::new();
        let mut state: Option<State> = None;
        for end in 1..=s.len() {
            let cur = &s[..end];
            let st = match state.take() {
                None => get(cur, pairs, variant, trusted),
                Some(prev) => {
                    let mut prev = prev;
                    loop {
                        let next = get_inc(cur, pairs, variant, &prev, trusted);
                        if next.head_offset() != prev.head_offset() {
                            // Head advanced: `prev` is finalized.
                            segs.push(seg_of(cur, &prev));
                            prev = next;
                            continue;
                        }
                        prev = next;
                        // Final but followed by unread buffer: the next call
                        // advances to the following item; do it now.
                        let followed = match &prev {
                            State::Ok(e) => e.tail_offset.is_some_and(|t| t.get() < end),
                            State::No(n) => {
                                n.tail_offset < end && n.tail_offset > n.head_offset
                            }
                            _ => false,
                        };
                        if !followed {
                            break;
                        }
                    }
                    prev
                }
            };
            state = Some(st);
        }
        if let Some(st) = state {
            segs.push(seg_of(s, &st));
        }
        segs
    }

    /// Adjacent non-envelope slices may be reported separately by the
    /// incremental driver; merge them for comparison.
    fn normalize(segs: Vec<Seg>) -> Vec<Seg> {
        let mut out: Vec<Seg> = Vec::new();
        for seg in segs {
            match (out.last_mut(), &seg) {
                (Some(Seg::Non(prev)), Seg::Non(_)) => {
                    if let Seg::Non(v) = seg {
                        prev.extend(v);
                    }
                }
                _ => out.push(seg),
            }
        }
        out
    }

    fn assert_scene(s: &str, pairs: &[LimiterPair<u8>], variant: Variant, expected: Vec<Seg>) {
        for trusted in [false, true] {
            let one_shot = parse_all(s.as_bytes(), pairs, variant, trusted);
            assert_eq!(one_shot, expected, "one-shot (trusted={trusted}) mismatch for {s:?}");
            let incremental = parse_all_incremental(s.as_bytes(), pairs, variant, trusted);
            assert_eq!(
                normalize(incremental),
                expected,
                "incremental (trusted={trusted}) mismatch for {s:?}"
            );
        }
    }

    fn non(s: &str) -> Seg {
        Seg::Non(s.as_bytes().to_vec())
    }
    fn payload(s: &str) -> Seg {
        Seg::Payload(s.as_bytes().to_vec())
    }
    fn pending(s: &str) -> Seg {
        Seg::Pending(s.as_bytes().to_vec())
    }

    #[test]
    fn v1_example_basic() {
        // `0%%A%%%%B%%%%C%%1` -> [0, payload A, payload B, payload C, 1]
        assert_scene(
            "0%%A%%%%B%%%%C%%1",
            &pairs(),
            Variant::V1,
            vec![
                non("0"),
                payload("A"),
                payload("B"),
                payload("C"),
                non("1"),
            ],
        );
    }

    #[test]
    fn v1_example_trailing_limiter_char() {
        // `0%%A%%%%B%%%%C%%1%` -> [0, payload A, payload B, payload C, 1, ...]
        // The trailing single `%` is a potential limiter slice.
        assert_scene(
            "0%%A%%%%B%%%%C%%1%",
            &pairs(),
            Variant::V1,
            vec![
                non("0"),
                payload("A"),
                payload("B"),
                payload("C"),
                non("1"),
                Seg::OnLimiter,
            ],
        );
    }

    #[test]
    fn v1_example_long_limiter_run() {
        // `0%%%%%%B%%%%C%%1` -> [0, pending B%%%%C%%1]
        assert_scene(
            "0%%%%%%B%%%%C%%1",
            &pairs(),
            Variant::V1,
            vec![non("0"), pending("B%%%%C%%1")],
        );
    }

    #[test]
    fn v1_example_empty_payload() {
        // `0^^~~^^B~~{{C}}1` -> [0, payload (offset range None), payload B, payload C, 1]
        assert_scene(
            "0^^~~^^B~~{{C}}1",
            &pairs(),
            Variant::V1,
            vec![
                non("0"),
                payload(""),
                payload("B"),
                payload("C"),
                non("1"),
            ],
        );
        // Empty payload: both payload offsets are None.
        let st = get_envelop(b"^^~~", &pairs(), Variant::V1);
        assert_eq!(
            st,
            State::Ok(Envelop {
                head_offset: 0,
                payload_head_offset: None,
                payload_tail_offset: None,
                tail_offset: NonZeroUsize::new(4),
            })
        );
    }

    #[test]
    fn v1_example_bad_pair_set() {
        // pairs [('^', '~', 2), ('~', '%', 2)]:
        // `0^^A~~~~B%%~~C%%1` -> [0, payload A, payload B, payload C, 1]
        assert_scene(
            "0^^A~~~~B%%~~C%%1",
            &bad_pairs(),
            Variant::V1,
            vec![
                non("0"),
                payload("A"),
                payload("B"),
                payload("C"),
                non("1"),
            ],
        );
    }

    #[test]
    fn v2_example_basic() {
        // `0%%A%%%%B%%%%C%%1` -> [0, payload A%%, B, pending C%%1]
        assert_scene(
            "0%%A%%%%B%%%%C%%1",
            &pairs(),
            Variant::V2,
            vec![non("0"), payload("A%%"), non("B"), pending("C%%1")],
        );
    }

    #[test]
    fn v2_example_multi_pairs() {
        // `0{{A}}{{B}}??C!!1` -> [0, payload A, payload B, payload C, 1]
        assert_scene(
            "0{{A}}{{B}}??C!!1",
            &pairs(),
            Variant::V2,
            vec![
                non("0"),
                payload("A"),
                payload("B"),
                payload("C"),
                non("1"),
            ],
        );
    }

    #[test]
    fn v2_example_confirmation_latency() {
        // `0{{A}}{{B}}??C!!!` -> [0, payload A, payload B, pending C!]
        assert_scene(
            "0{{A}}{{B}}??C!!!",
            &pairs(),
            Variant::V2,
            vec![non("0"), payload("A"), payload("B"), pending("C!")],
        );
    }

    #[test]
    fn v2_example_bad_pair_set() {
        // `0^^A~~~~B%%~~C%%1` -> [0, payload A~~, B%%, payload C, 1]
        assert_scene(
            "0^^A~~~~B%%~~C%%1",
            &bad_pairs(),
            Variant::V2,
            vec![
                non("0"),
                payload("A~~"),
                non("B%%"),
                payload("C"),
                non("1"),
            ],
        );
    }

    #[test]
    fn v2_contiguous_envelopes_sharing_pair() {
        // `0^^1~~^^2~~3` is well defined: two contiguous envelopes can share
        // a limiter pair whose limiter != delimiter.
        assert_scene(
            "0^^1~~^^2~~3",
            &pairs(),
            Variant::V2,
            vec![non("0"), payload("1"), payload("2"), non("3")],
        );
    }

    #[test]
    fn shared_limiter_supported() {
        // Not recommended, but supported: two pairs sharing the limiter `^`.
        let pairs = [
            LimiterPair::new(b'^', b'~', 2),
            LimiterPair::new(b'^', b'!', 3),
        ];
        // First matching pair in set order wins.
        assert_scene(
            "0^^a~~1",
            &pairs,
            Variant::V1,
            vec![non("0"), payload("a"), non("1")],
        );
        assert_scene(
            "0^^^a~~~1",
            &pairs,
            Variant::V1,
            vec![non("0"), payload("a"), non("1")],
        );
    }

    #[test]
    fn incremental_advances_to_next_item() {
        let state = get_envelop(b"%%A%%", &pairs(), Variant::V1);
        assert_eq!(
            state,
            State::Ok(Envelop {
                head_offset: 0,
                payload_head_offset: NonZeroUsize::new(2),
                payload_tail_offset: NonZeroUsize::new(3),
                tail_offset: NonZeroUsize::new(5),
            })
        );
        // Old state is final and the buffer is not all read: parse new item.
        let next = get_envelop_incremental(b"%%A%%rest", &pairs(), Variant::V1, &state);
        assert_eq!(
            next,
            State::No(NonEnvelop {
                head_offset: 5,
                tail_offset: 9
            })
        );
        // Nothing new: unchanged.
        let same = get_envelop_incremental(b"%%A%%", &pairs(), Variant::V1, &state);
        assert_eq!(same, state);
    }

    #[test]
    fn v1_pending_tail_offsets_are_none() {
        let st = get_envelop(b"%%A", &pairs(), Variant::V1);
        assert_eq!(
            st,
            State::Pending(Envelop {
                head_offset: 0,
                payload_head_offset: NonZeroUsize::new(2),
                payload_tail_offset: None,
                tail_offset: None,
            })
        );
    }

    #[test]
    fn v2_pending_payload_tail_monotonic_increase() {
        let st = get_envelop(b"??C!!", &pairs(), Variant::V2);
        assert_eq!(
            st,
            State::Pending(Envelop {
                head_offset: 0,
                payload_head_offset: NonZeroUsize::new(2),
                payload_tail_offset: NonZeroUsize::new(3),
                tail_offset: None,
            })
        );
        let st = get_envelop_incremental(b"??C!!!", &pairs(), Variant::V2, &st);
        assert_eq!(
            st,
            State::Pending(Envelop {
                head_offset: 0,
                payload_head_offset: NonZeroUsize::new(2),
                payload_tail_offset: NonZeroUsize::new(4),
                tail_offset: None,
            })
        );
    }

    #[test]
    fn onlimiter_falls_back_to_non_envelop() {
        // Single `%` can still grow into a limiter slice...
        let st = get_envelop(b"abc%", &pairs(), Variant::V1);
        assert_eq!(
            st,
            State::No(NonEnvelop {
                head_offset: 0,
                tail_offset: 3
            })
        );
        // ...but a terminated too-short run is plain non-envelope content.
        let st = get_envelop_incremental(b"%x", &pairs(), Variant::V1, &State::OnLimiter(0));
        assert_eq!(
            st,
            State::No(NonEnvelop {
                head_offset: 0,
                tail_offset: 2
            })
        );
    }

    #[test]
    fn state_offset_shift() {
        let mut e = Envelop {
            head_offset: 3,
            payload_head_offset: NonZeroUsize::new(5),
            payload_tail_offset: NonZeroUsize::new(7),
            tail_offset: NonZeroUsize::new(9),
        };
        unsafe { e.offset(3) };
        assert_eq!(
            e,
            Envelop {
                head_offset: 0,
                payload_head_offset: NonZeroUsize::new(2),
                payload_tail_offset: NonZeroUsize::new(4),
                tail_offset: NonZeroUsize::new(6),
            }
        );
        let mut st = State::OnLimiter(2);
        unsafe { st.offset(2) };
        assert_eq!(st, State::OnLimiter(0));
    }

    #[test]
    fn recommended_set_check() {
        // `%` is both limiter and delimiter of one pair: not recommended.
        assert!(!is_recommended_set(&pairs()));
        assert!(is_recommended_set(&[
            LimiterPair::new(b'^', b'~', 2),
            LimiterPair::new(b'?', b'!', 2),
        ]));
        assert!(!is_recommended_set(&bad_pairs()));
    }
}
