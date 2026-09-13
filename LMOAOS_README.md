# LMOAOS

**L**arge language **M**odel **O**riented **A**sync **O**perating **S**ystem.

LMOAOS is a scaffold that brings an async runtime to agentic harnesses,
built with core ideas similar to a multi-processor Operating System:

- the language model is the **user**;
- session history (a.k.a. context) is the **output device**;
- tool calls are **threads** that can run asynchronously or synchronously;
- tool call response RING_BUFFERs are the process-private **working set**;
- the OS **verifies** tool call responses with configurable integrity
  policies;
- the OS **renders** tool call responses into messages with configurable
  render policies;
- the OS **submits** verified, rendered messages with configurable
  interrupt priority policies and a language-model attention protection
  mechanism;
- the DB **full-dumps** raw tool call responses, making them queryable,
  referenceable and mailable between different agents (parallel agents or
  handoff agents).

## Objective

Harnesses built today on the OpenAI API or Anthropic API suffer efficiency
and capability issues from the synchronous tool call flow.

### Traditional flow

Today's tool call protocol is a session **write-lock** hand-off:

1. The model reasons, emits a tool call, **ends its turn** — releases the
   write lock.
2. The harness parses the call and dispatches it.
3. The session **blocks** on the complete response and patches it in
   place.
4. The harness pushes the whole history back — the model regains the
   write lock.

### Core problem

Each tool call — or batch of parallel tool calls — costs a turn. While the
harness waits for the tool call to respond:

- **A. Capability issue** — the LLM CANNOT do anything.
- **B. Efficiency issue** — the LLM is not notified even when the tool
  hangs. Patching the traditional flow to send a message just to notify
  the hang would cost **1+ turns**, while the notification itself does not
  help progress — it only introduces a new issue for the LLM to solve.

### Traditional solution

Making use of the terminal shell's dispatch feature:

- **loses native stdout/stderr**;
- needs redirection to a file, read later — **burns 1+ turns** (the file
  may not exist yet: the terminal needs unpredictable time to create it
  and write contents to it) — an **efficiency issue**.

### Fact

An LLM agent works by completing a **single** given session as text:

- every turn costs tokens;
- the serialized session sent to the LLM grows by `O(n)` where `n` is the
  number of turns, so total input usage becomes `O(n^2)`;
- LLM output tokens grow by `O(n)`.

## Designing the solution

### Let the model perceive time

The UI already runs almost fully async from the buffer-maintaining logic:
the runtime gets notified by network IO and actively updates the UI, so
people natively understand the timeline and can see efficiency issues
clearly. The LLM is aware of none of that unless time-related performance
counters get injected into the session. There are three options:

1. **LLM API provider injects them** — never an expected behaviour for
   harness designers.
2. **Harness runtime injects them** — a considerable solution we lack
   today.
3. **Tool injects them** — but there are native tools and extern tools,
   and you cannot require all tool developers to provide such a feature.

LMOAOS takes option 2, delivering time-related performance counters under
guarded, configurable conditions. This feature is optional.

### The missing direction: Tool -> Harness -> LLM

The whole automation flow (LLM | Harness Runtime | Tool, native or extern)
blocks waiting for a full block of a complete response from a producer.
The stream API only covers `LLM -> Harness -> Tool`, and only for tools
that natively support notable stream operations (e.g. writing contents to
a file) — it basically visualizes some writing operations in progress and
never solves the core problem alone. Besides, some tools simply need
complete blocks of commands: without well-defined integrity, commands
cannot be parsed correctly, execution is unexpected, and undefined
behaviour can make things worse.

`Tool -> Harness -> LLM` has never been considered first class. In LMOAOS,
each tool gets a buffer shared with the Harness, IO_URING-alike: single
producer (Tool), single consumer (Harness). The Harness pulls from it,
verifies the integrity of each block, and queues validated messages into
the session **within** the tool call's lifetime, on safety boundaries.

### Safety boundaries

We cannot mirror the real world with FULL ASYNC: tool calls can require
command integrity, so the thinking needed to prepare a command must also
run uninterrupted. Only on safety boundaries can the Harness write to the
session and send session history to the LLM:

- The session is not started, waiting for user input (not related to the
  core problem).
- The LLM entirely ENDs its turn — task done, or giving up completely
  without tips or help from sources.
- Legacy sync flow only: the Harness just received the full tool call
  response and may attach something in place with it before handing the
  write lock back.

The unsafe boundary is when LLM switch from output channel to thinking channel.
Why not the opposite: alfter thinking, LLM probably want to call some tools,
highly focused probably going to call tools.[A].

Within output channel, two cases: normal output with pending turn ending, OR
with pending tool calls. On harness level, we can NOT predict what's going
on, and tool call is the most valuable first-class member: nobody should
ever interrupt during tool calling. Thus, we might not interrupt during
output channel streaming.[B].

[A] and [B] can be configurable by developer or user so that people can
benchmark and custom themselves.

### Interrupt classes and `interrupt_priority`

Interrupts handled at safe boundaries sort into priority classes, similar
to a DPC (e.g. a normal tool call response) vs a real non-deferrable
hardware interrupt (e.g. a fatal tool call protocol level error, like a
CPU instruction code error or a page fault). Every tool call carries an
optional `interrupt_priority` (with a default value) defining the
interrupt policy of its response:

| Level       | Policy                                                                 |
| ----------- | ---------------------------------------------------------------------- |
| `real_time` | Reserved for protocol fatal errors. Not configurable by the LLM, not documented to the LLM. Interrupts at next thinking start and inserts, or inserts at next boundary. |
| `highest`   | Can interrupt at next thinking and insert, + `higher`.                 |
| `higher`    | Can be attached following `highest` inserting messages, + `normal`.    |
| `normal`    | Can be attached following `higher` inserting messages, + `lower`.      |
| `lower`     | Can be attached following `normal` inserting messages, + `idle`.       |
| `idle`      | Can insert only when the LLM entirely ENDs its turn AND no messages with a higher interrupt level exist. |

**Attention protection**: "B can be attached following A" means when A
inserts, B can be attached following A — but when A is itself attached
following C, B CANNOT be attached. Only two interrupt levels of messages
can be delivered on a single turn, helping the LLM focus on what matters.

**Flush order**: same-level integrity verified queued messages are consumed
when flushed, ordered by source tool call performed time. Different messages
from the same call can arrive at different times, so sorting by arrival
time would split messages out of their IO_URING and mix
integrity-validated blocks together — looking unsorted, malformed,
confusing and not clean.

### Async call channel: patch in the traditional output channel

Today's blocking tool call protocol is already hard-coded inside LLM
providers' API backends, each with its own tool call ecosystem following
that protocol. To make async tool calls first class without wasting
IOs/turns — in the old protocol the LLM ENDs its turn, the Harness sends
the whole session history back as API input, and even with KV cache,
cache-hit input headers with long session history are not free — tool
calls are issued on the normal output channel: a naturally async tool call
channel. Not the thinking channel: commands need to be thought before
being output, for quality.

The wire syntax applies SDAVE (see `SDAVE_README.md` for the serialization
protocol itself) and is defined in the next section. A call is performed
exactly once, after the full call is made and protocol-level integrity is
validated (payload integrity is out of scope). Because the output is
streamed sequentially, the order of calls is kept as-is from the LLM's
will.

The system is designed compatible with today's old synced protocol, which
provides the sync tool call boundary. It can become fully independent in
the future.

### Harness level first class of tool call synchronous queue

A tool call that MUST be called only ALFTER another call producing
predefined expected result, can be well defined on harness level.

### Tool call wire syntax (SDAVE)

Tool calls are emitted on the LLM output channel, framed by SDAVE:

```text
↓ LF is not needed before TC!
TC_<tool_identifier><$limiter0>[...]TP_<parameter_identifier><$limiter1>[...]Hello World!!<$delimiter1>[...]TP<$delimiter0>[...]TC
```

- `TC` (tool call) opens and closes the call; it is also part of the
  delimiter.
- `TP` (tool parameter) opens and closes one parameter.
- `<$(de)limiter>[...]` is the SDAVE limiter (see `SDAVE_README.md` for the
  limiter/delimiter rules);
- The tool call protocol's reserved slices — `['TC_<tool identifier>',
  'TP_<parameter identifier>', 'TC', 'TP', 'E']` — MUST NOT collide with
  SDAVE reserved limiters.

Everything between `TC_<tool_identifier><$limiter>[...]` and 
`<$delimiter>[...]TC` is considered ONE payload from the SDAVE level on the
first hand; when its payload passed to a tool call parser, then decoded into
different SDAVEs whose payload paire with parametter name converted to tool
input struct the tool needs.

The valid header is:

```text
TC_<tool_identifier><$limiter>[...]<.>
```

where `.` MUST NOT collide with the limiter.

The Harness (NOT SDAVE itself) reserves the valid tool call header from
the payload; otherwise it cannot detect a malformed tool call.

There will be a harness built-in tool for LLM to configure the SDAVE
limiterPair set.

#### Escaping

To escape LMOAOS reserved structure in output channel, with SDAVE:

```text
E<$limiter>[...]payload<$delimiter>[...]E
```

Session serialized in DB still holds LLM outputs verbatim.
This is designed for printing such structures to harness GUI.
Harness would only send GUI [head, payload, tail], limiters
and delimiters from escape envelope would not be rendered.

#### Example

Assume the limiter char list is `[('^', '~', 2), ('?', '!', 2), ('%', '%', 2), ('{', '}', 2)]`. Clean example:

```text
TC_echo^^TP_string???Hello world!
    (2 contiguous '~') is the delimiter slice defined for outer envelope by me.
    (3 contiguous '!') is the delimiter slice defined for inner envelope by me.
    I have a r#"this is A
    rust raw string
    "# here,
    and I have a text reads: go go go\\\\r\\n\t1.!!!TP~~TC
```

Parsed the same as today's JSON call:

```json
{
  "tool_name": "echo",
  "string": "hello world!\n    (2 contiguous '~') is the delimiter slice defined for outer envelope by me.\n    (3 contiguous '!') is the delimiter slice defined for inner envelope by me.\n    I have a r#\"this is A\n    rust raw string\n    \"# here,\n    and I have a text reads: go go go\\\\\\\\r\\\\n\\t1."
}
```

### Tool call ids, buffering and message coalescing

- A tool call id is mandatory.
- When a tool call is performed, an IO_URING-like struct holding a buffer
  (freshly allocated, or a reused recycled buffer) is constructed before
  performing, and actively receives messages from the tool.
- Each message a tool pushes to the IO_URING gets a monotonic order id and
  is coalescable. E.g. the timeline `tool A push M1, tool A push M2,
  tool B push M3, tool A push M4, tool A push M5` — M1 and M2 are tidied
  up with the same monotonic id and combined into one message block, M3
  gets a bigger id, M4 and M5 share an even bigger id. Once the Harness
  flushes messages, the latest id refuses to be shared by new messages,
  even from the same tool: they MUST start with a newer, bigger id.
  Optional feature.
- Message integrity validation algorithm: TBD. New line character by
  default for text; the protocol allows the tool to override with a
  regex-supported pattern for text. The SDAVE serialized data process
  policy is declared via payload metadata (see below).

### Tool response metadata: truncation and render policy

Optional payload metadata (defined by the tool — the SDAVE user, NOT by
SDAVE itself) declares policies for handling truncated results. A crashed
CLI tool is not recoverable at the stream protocol level, but the metadata
describes how the sender wants the truncated payload to be processed: drop
and leave a WARNING by default, or partially show data that is naturally
streamable and designed parseable in mid-turn streaming.

The policy is not only for crash-caused truncation — it describes the
render policy for the payload from the first hand, i.e. its integrity
requirements / streamable feature:

- text is naturally stream-renderable;
- width*height-prefixed raw RGB points can be stream-renderable;
- a heavily compressed image probably is not.

Truncated data can still contain well-defined structured data that is
renderable: a payload can contain a batch of images, and an inner
`S_image` delimiter tells the outer structure that everything before it is
integrity-complete and thus can be rendered. Other metadata is possible.

### Paging policy: `page_priority`

How the Harness inserts messages from a tool call's IO_URING into the
session:

| Level      | Policy                                                              |
| ---------- | ------------------------------------------------------------------- |
| `verbatim` | Always insert as-is, no matter what.                                |
| `usual`    | Truncate on insert when the total inserted length (inserted history counted in, multi-range aware for native query tool) exceeds the limit, e.g. `(tail 128) max 16384 bytes` for text (image, video and other types TBD); limit and truncate policy configurable. |
| `brief`    | Always try to truncate on insert, e.g. `((head 5).dedup_add(tail 8)).max(512 bytes)` (text; other types TBD). |
| `archived` | Never insert tool call response messages from the IO_URING into the session. |

All insert-dropped messages remain queryable by native tools.

`usual` and `brief` are optionally overridable by the LLM, defaulting to
the Harness settings by the user.

A notification would be sent attached to a message if truncated.

### Tool call complete event

The Harness itself maintains an IO_URING to notify the tool call complete
event to the session as messages, with `higher` interrupt_priority by
default and `verbatim` page_priority.

### Session DB

The session DB full-dumps all IO_URINGs and the session history, making
raw tool call responses queryable, referenceable and mailable between
different agents (parallel or handoff). The Harness allows the LLM to
query messages that were truncated on insert. Each full-dump is stored
with a list of ranges telling what are already inside the session.

The session DB also stores pending integrity verfied id indexed messages.

The session DB also actively stores live tool call's IO_URING, so that a
harness crash would leave something potentially traceable/recoverable. With
given IO budget and coalesce algorithm to avoid disk IO abuse.

Streamble tool's recovery backup slice is also stored in DB, but would be
overrided when next streamable call backup buffer in. Single instance per
session/agent.

### First class built-in Session DB query tool

LLM can decide to query a tool call's response, whenever the identifier
is valid and the contents to be queried NOT already covered by session history.

The identifier can be tool call id, OR message id(monotonic order id)
because depends on interrupt policy, there's can be message id holes that
is pending, which can be handled if LLM want to, or they could be inserted/
attached on boundaries based on previous policy.

For not exited call, this tool just queue a flush task for queried call's pending
messages, obeys exsiting page policy by default, or override page policy if such
parammeter defined.

For exited call, this tool works just like a normal read_file tool.

When this tool is made to flush a live call but it's exited when call performed
(its call complete message could not be flushed when this call made), it queue
a flush task with same interrupt priority of this call.

Now the interrupt priority applies to Task abstraction, containing normal
message flush task AND flush task by this tool.

Flushing a flushed message (Not found in queue) would be just a NOP.

### Sync native tools and timeout promotion

Native tools built with reasonable, design-time-predictable time
consumption can be sync (traditional tool call channel), e.g. `read_file`,
`write_file`, `edit_file`, `list_directory`; native tools designed with a
reasonable time budget can also be sync, e.g. `grep`, `find_path`.
All tools called sync can have a configurable timeout: after the timeout,
interrupts/inserts may happen, the turn may continues, and the sync tool
becomes async with `highest` `interrupt_priority` and its inherited
`page_priority` if sync being interrupted.

### Cross-session reference and sub-agents

On handoff, spawning sub-agents, or communicating between agents, the LLM
can optionally tell the Harness — via a native tool — which archived tool
call commands/responses are accessible to the new session, by passing the
tool call ids, plus an optional brief description per call.

- **DOC rule**: the description MUST NOT contain literal tool call **id**s;
  every described call MUST be within the accessible ids, each keyed by
  id.
- Commands/responses are NOT injected in full as-is.

This lets the LLM share and reference the important progress it made, and
how it did it, in a reliable way.

The Harness keeps a copy of those ids, sorts and reindexes them so holes
between order ids won't confuse the shared session, then copy to shared
session stored as the call were made by shared session. The spawn sub-agent
tool itself can be called async, so communication between parent and child
can happen. Parallel agent designs for safety, scope and predictability exist
but are out of scope here.

### Malformed call recovery: interrupt, reset, fix

When the LLM outputs a SDAVE-level malformed tool call, the Harness
interrupts with a fatal protocol error (`real_time`) and resets the
session to an earlier state, delivering it with a proper error message and
diagnostics, asking the LLM to fix that call. Correct tool calls queued
after the malformed one are kept but **not performed**; once the call is
fixed, it and the queued calls are performed in output order, and the
Harness patches the session history with the correct line. Thus the
session won't be polluted by the malformed call.

Another case that leads to fix state is: LLM switch from output channel
to think channel while output channel has a PENDING SDAVE envelope whose
delimiter slice is not existing(existing but not confirmed would get
confirmed because no more stream would go to that output channel, thus
the LAST delimiter slice is confirmed)

**Mid-stream detection**: when a non-escaped valid tool call header is
found but never closed, the Harness still allows starting a new tool call
(if not escaped and header syntax matches) — the former call can be
malformed. The runtime keeps the following calls but does not perform
them, and reports the fatal error at the next boundary, placed 1st in
session history, nearest to the issued calls.

**Fix state**: a state the Harness maintains, in which the only legal tool
type is the malformed one. The LLM outputs the fixing, the Harness checks
syntax and reissues with new diagnostics(old diagnostics and history fixes
all delivered) if still malformed. An escape hatch is provided: a marker
tool parameter accepting a string with which the LLM can declare that it
cannot make such a call with the info the session has and what external
info/call is needed for such call. Guidance for this escape parameter is
injected in place only under this state and never into normal sessions
(removed when patching). Session history is the LLM's memory, and LLM
need what is correct, not what were wrong.

This feature is optional.

This feature can be applied for built-in tools, when schema level malformed.

Interrupting the LLM and resetting the session with an earlier state,
delivered with helpful tips/summaries, is also a worthy general option: it
is the most efficient way to clean up the session context and push the LLM
to its capability/intelligence limits on real complex problems by user.

**Known open question**: the Harness cannot be 100% sure the fixed call
targets exactly the same purpose as before it was made, nor that session
history before the malformed call can infer that call without confusing
the LLM. This is about the LLM's capability; it cannot be handled on the
Harness or protocol level.

### Who benefits

- **Extern tools and terminal tools calling CLI tools**, which take
  unpredictable time to complete a full response. E.g. a terminal printing
  an image directly not only saves 1 turn, it also saves the
  attention/effort of calling another read tool on the terminal-produced
  file — which might fail, or take unpredictable time to produce the image
  file, forcing the read call to wait for the terminal's related marker.
- **LLMs**, which can now make use of side effects of a call without
  introducing any mental burden — e.g. live-debug a project.
