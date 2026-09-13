# SDAVE

**S**treamable **D**elimiter-**A**daptive **V**erbatim **E**nvelope protocol.

SDAVE is a **fundamental**, flat (nesting-agnostic, payload-agnostic)
serialization protocol optimized for LLM agentic harness usage. It reserves
a serialize-time-defined delimiter slice from the payload and decodes the
payload verbatim — byte-identical to what was written. It tells serialized
envelopes and potentially streaming envelopes and non-envelope slices from
one slice(channel) mixing them together, and those non-envelope slices won't
be considered as unexpected results.

## Motivation

Existing serialization protocols include JSON, XML, TOML, YAML, Protobuf,
Rust raw strings, traditional C strings, Java strings, etc.

Can be summarized to four fundamental payload-protocols:

1. **Fixed reserved slices + payload escape mechanism** — fixed reserved
   slices is reserved from the payload, plus payload escape mechanisms.
   - The payload may **NOT** be **byte-identical** to what was written.
   - `O(m * 2^n)` escapers are needed for `n` levels of nested serialized
     payload with `m` escaper-requiring slices: mental burden for payload
     writers, and wasted IO when heavily nested content is serialized in
     the payload.
2. **Length-prefix** — the payload length MUST be defined ahead of the
   payload.
   - The full payload must be known before serialization; incremental
     payload streaming byte-identical to what was read is impossible.
3. **Strict static structure** (e.g. Protobuf) — markers and build-time
   defined static paths.
   - Even worse for incremental payload streaming.
4. **Configurable delimiter slice** — a configurable delimiter slice is
   reserved from the payload.
   - Undefined behaviour when the payload collides with the configured
     delimiter slice; the writer had better know what can appear in an
     incremental streaming payload.

For LLM agentic harness usages, an LLM roughly thinks about what it is
going to call **before** making the real call, and can consider delimiter
slice definition within thinking — so serialize agent tool call into
SDAVE could work.

## Design

```rust
struct LimiterPair<T> where T: Sized{
    limiter: T,
    delimiter: T,
    least_repeat: u32,
}
enum State {
    NO(NonEnvelop),
    /// `s`[usize..] are potentially still defining a limiter slice
    /// usize is the every first offset of potential limiter slice
    /// `s`[usize..] can be NonEnvelop if later follwed by not same T AND length not exceed `least_repeat`
    ONLIMITER(usize),
    /// `payload_head_offset` could be Some when and only when well defined(at least one non-limiter T exsits following limiter slice).
    /// for variant1, two tail offsets for PENDING are None;
    /// for variant2, `payload_tail_offset` can be Some when and only when the delimiter slice MUST exists
    /// but exact offset not confirmed yet, and thus `payload_tail_offset`'s value can monotonic increase on next update;
    /// 
    /// WARNING: PENDING contents can later be PENDING or valid Envelop or otherwise the stream is ended before the envelope completes
    /// in case of the stream is ended before the envelope completes, the application should handle the malformed tail data.
    PENDING(Envelop),
    /// When and only when an empty payload serialized with a LimiterPair whose limiter != delimiter, a Ok's Envelop's payload_head_offset and payload_tail_offset can be None.
    /// An empty MUST NOT be serialized with a LimiterPair whose limiter == delimiter
    OK(Envelop),
}
struct NonEnvelop{
    head_offset: usize,
    /// can monotonic increase on next update
    tail_offset: usize,
}
struct Envelop {
    head_offset: usize,
    payload_head_offset: Option<NonZeroUsize>,
    payload_tail_offset: Option<NonZeroUsize>,
    tail_offset: Option<NonZeroUsize>,
}
// not own the buffer so that Application can full control buffer's life time.
// SAFETY: SDAVE never owns buffer in this demo, can not confirm the buffer is monotonic recieving or offset MUST be updated to existing State.
impl Envelop{
    unsafe fn offset(&mut self, offset: usize);
}
/// from the every start of `s`, once a valid limiter slice exsits, this fn only cares about its matched delimiter slice.
/// if there's matched delimiter slice, then Ok(), otherwise Pending()
/// if no valid limiter slice, NO()
unsafe fn get_envelop<T>(s: &[T], limiter_pairs: &[LimiterPair<T>]) -> State where T: Sized;
unsafe fn get_envelop_incremental<T>(s: &[T], limiter_pairs: &[LimiterPair<T>], state: State) -> State where T: Sized;
```

### Limiter slice / Delimiter slice

- The limiter pair is one of a configurable, SDAVE-parse-time-fixed set,
  e.g. `[('^', '~', 2), ('?', '!', 2), ('%', '%', 2), ('{', '}', 2)]`.

Note: In case of agentic harness, for u8 buffer, since ASCII only use 0x00..0x7F,
and LLM can output UTF-8, it's best practice to reserved UTF-8 only chars;
it's recommended to only reserve limiterPairs whose limiter != delimiter,
and the limiterPair set match eq(len(intersect(all limiter,all delimiter)),0)
and set.any(|p|set.any(|p1|p.as_ptr()!=p1.as_ptr&&(p.limiter==p1.limiter||
p.delimiter==p1.delimiter))).is_none();

The limiter slice is `<$limiter>[...]`: least_repeat or more repeats of a T.
The delimiter slice is then the other delimiter of same pair repeat same times.
Repeating the same delimiter T solves `payload–delimiter slice` corruption issue,
otherwise the payload is broken by SDAVE when there's conflict between payload
and delimiter slice.

#### Delimiter match algorithm

There's not a full winner, choose based on scene.

##### Variant1

The every first full matched delimiter slice is just the delimiter slice. No
delimiter slice confirmation latency, but tail of payload MUST NOT collide with
delimiter. This limiter pair mental burden addon is NOT solvable on application level.

For variant1, head and tail of payload and tail of existing contents(if NOT ended
by a complete variant1 envelope) in the mixed buffer before this envelope can at
most each disable one limiter pair. Thus at most 3 limiter pairs can be disabled
for this envelope. Two contiguous envelopes can have same delimiter pair.

assume limiter pair set same as example every above.
`0%%A%%%%B%%%%C%%1` -> [0, payload A, payload B, payload C, 1]
`0%%A%%%%B%%%%C%%1%` -> [0, payload A, payload B, payload C, 1]
`0%%%%%%B%%%%C%%1` -> [0, pending B%%%%C%%1]
`0^^~~^^B~~{{C}}1` -> [0, payload (offset range None), payload B, payload C, 1]

assume a bad example of a limiter pair set containing A and B with (A.delimiter == B.limiter)
[('^', '~', 2), ('~', '%', 2)].
`0^^A~~~~B%%~~C%%1` -> [0, payload A, payload B, payload C, 1]

##### Variant2

The every first full matched delimiter slice FOLLOWED by a NON-delimiter is the
delimiter slice. Tail of payload won't disable a limiter pair, but would introduce
delimiter slice confirmation latency. This latency is solvable on application
level, by automatically push a non-delimiter to mixed channel.

For variant2, head of payload and tail of existing contents in the mixed buffer
before this envelope can at most each disable one limiter pair. Thus at most 2
limiter pairs can be disabled for this envelope. Two contiguous envelopes MUST
NOT share same delimiter pare(won't contribut 2 to 3). And variant2 introduce
a dilimiter slice confirmation latency.

assume limiter pair set same as example every above.
`0%%A%%%%B%%%%C%%1` -> [0, payload A%%, B, pending C%%1]
`0{{A}}{{B}}??C!!1` -> [0, payload A, payload B, payload C, 1]
`0{{A}}{{B}}??C!!1` -> [0, payload A, payload B, payload C, 1]
`0{{A}}{{B}}??C!!!`  -> [0, payload A, payload B, pending C!]

assume a bad example of a limiter pair set containing A and B with (A.delimiter == B.limiter)
[('^', '~', 2), ('~', '%', 2)].
`0^^A~~~~B%%~~C%%1` -> [0, payload A~~, B%%, payload C, 1]

### Q&A

Q Why not force limiter != delimiter?
A SDAVE is a fundamental general purpose protocol, Sized T may not have that much reservable variants.

Q Why is NonEnvelope existing?
A Cases are that different info stream share same channel. LLM output is just a typical one.

## Scope

A buffer can be [content_slice0 /* State::EMPTY */, OK(SDAVE0),content_slice1, OK(SDAVE1), PENDING(SDAVE2)].

SDAVE differs NonEnvelope and Envelope and pending potential Envelope from
mixed buffer/channel.

It's application's responsibility to explain/make use of NonEnvelope
and Envelope and pending potential Envelope.

Application can make use of PENDING(payload_head_offset) for
**well defined streamable recoverable** payload usage. In case
of that, it's application's own responsibility to verify and
deal with potentially pending payload and prepare solutions for
false-positive envelope.
