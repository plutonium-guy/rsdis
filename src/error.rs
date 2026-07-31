//! Command errors.
//!
//! Owner: F0. **FROZEN** -- the enum shape is part of the cross-agent contract.
//! You may add `impl` helpers; you may not add, remove or reorder variants.
//!
//! Every `Display` output below is byte-for-byte what Redis 7.4 puts on the
//! wire after the leading `-`. Client libraries pattern-match on these strings
//! (`WRONGTYPE`, `NOAUTH`, `OOM`, `BUSYGROUP`, ...), so paraphrasing is a
//! compatibility bug, not a style choice. Each is annotated with the Redis
//! source it comes from.

use std::fmt;

/// The error type every command handler returns.
#[derive(Debug)]
pub enum CmdError {
    /// `server.c: shared.wrongtypeerr`
    WrongType,
    /// `server.c: rejectCommandFormat(c, "wrong number of arguments for '%s' command", ...)`
    /// The payload is the *command* name as registered (lowercase), matching
    /// what Redis reports for container commands as `"cmd|subcmd"`.
    WrongArity(&'static str),
    /// `object.c: getLongLongFromObjectOrReply()`
    NotAnInteger,
    /// `object.c: getDoubleFromObjectOrReply()`
    NotAFloat,
    /// `server.c: shared.syntaxerr`
    Syntax,
    /// `object.c: getRangeLongFromObject()` -- a *value* is out of range.
    OutOfRange,
    /// `server.c: shared.outofrangeerr` -- an *index* is out of range (`LSET`).
    IndexOutOfRange,
    /// `server.c: shared.nokeyerr`
    NoSuchKey,
    /// `t_string.c: incrDecrCommand()`
    ValueOverflow,
    /// `server.c: shared.noautherr`
    Unauthenticated,
    /// `server.c: helloCommand()`
    NoProto,
    /// `server.c: shared.oomerr`
    Oom,
    /// Anything with a bespoke code, e.g. `("BUSYGROUP", "Consumer Group name
    /// already exists")`. `Display` renders it as `"{code} {message}"`.
    Custom(&'static str, String),
    /// An I/O failure while serving the command. Rendered as an `ERR`.
    Io(std::io::Error),
}

impl CmdError {
    /// `("ERR", msg)`.
    #[inline]
    pub fn err(msg: impl Into<String>) -> Self {
        CmdError::Custom("ERR", msg.into())
    }

    /// A bespoke `(code, message)` error.
    #[inline]
    pub fn custom(code: &'static str, msg: impl Into<String>) -> Self {
        CmdError::Custom(code, msg.into())
    }

    /// `ERR value is out of range, must be positive` -- `SETRANGE`, `SETBIT`,
    /// `GETRANGE` offsets and `EXPIRE`-family negative-guard paths.
    #[inline]
    pub fn out_of_range_positive() -> Self {
        CmdError::Custom("ERR", "value is out of range, must be positive".into())
    }

    /// `ERR unknown command '<name>', with args beginning with: '<a>' '<b>' `
    ///
    /// Mirrors `server.c: commandCheckExistence()`:
    ///
    /// ```c
    /// sds args = sdsempty();
    /// for (int j = 1; j < c->argc && sdslen(args) < 128; j++)
    ///     args = sdscatprintf(args, "'%.*s' ", 128-(int)sdslen(args), c->argv[j]->ptr);
    /// rejectCommandFormat(c, "unknown command '%.128s', with args beginning with: %s", ...);
    /// ```
    ///
    /// Note the single-quote-space separator (no comma), the trailing space
    /// after the final argument, the 128-byte budget on the argument list and
    /// the 128-byte truncation of the command name itself.
    pub fn unknown_command(name: &[u8], rest: &[bytes::Bytes]) -> Self {
        let mut args = String::new();
        for a in rest {
            if args.len() >= 128 {
                break;
            }
            let budget = 128usize.saturating_sub(args.len());
            let text = String::from_utf8_lossy(a);
            let truncated: String = text.chars().take(budget).collect();
            args.push('\'');
            args.push_str(&truncated);
            args.push_str("' ");
        }
        let name_text = String::from_utf8_lossy(name);
        let name_trunc: String = name_text.chars().take(128).collect();
        CmdError::Custom(
            "ERR",
            format!("unknown command '{name_trunc}', with args beginning with: {args}"),
        )
    }

    /// The leading token of the error, e.g. `WRONGTYPE`. Used by `INFO
    /// errorstats` (W3c) and by tests.
    pub fn code(&self) -> &'static str {
        match self {
            CmdError::WrongType => "WRONGTYPE",
            CmdError::Unauthenticated => "NOAUTH",
            CmdError::NoProto => "NOPROTO",
            CmdError::Oom => "OOM",
            CmdError::Custom(code, _) => code,
            _ => "ERR",
        }
    }
}

impl fmt::Display for CmdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CmdError::WrongType => {
                f.write_str("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            CmdError::WrongArity(name) => {
                write!(f, "ERR wrong number of arguments for '{name}' command")
            }
            CmdError::NotAnInteger => f.write_str("ERR value is not an integer or out of range"),
            CmdError::NotAFloat => f.write_str("ERR value is not a valid float"),
            CmdError::Syntax => f.write_str("ERR syntax error"),
            CmdError::OutOfRange => f.write_str("ERR value is out of range"),
            CmdError::IndexOutOfRange => f.write_str("ERR index out of range"),
            CmdError::NoSuchKey => f.write_str("ERR no such key"),
            CmdError::ValueOverflow => f.write_str("ERR increment or decrement would overflow"),
            CmdError::Unauthenticated => f.write_str("NOAUTH Authentication required."),
            CmdError::NoProto => f.write_str("NOPROTO unsupported protocol version"),
            CmdError::Oom => f.write_str("OOM command not allowed when used memory > 'maxmemory'."),
            CmdError::Custom(code, msg) => write!(f, "{code} {msg}"),
            // Redis surfaces internal I/O failures as plain ERRs.
            CmdError::Io(e) => write!(f, "ERR {e}"),
        }
    }
}

impl std::error::Error for CmdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CmdError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CmdError {
    #[inline]
    fn from(e: std::io::Error) -> Self {
        CmdError::Io(e)
    }
}

/// Shorthand used pervasively by handlers.
pub type CmdResult = Result<(), CmdError>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Every string here was captured from a live Redis. Changing one of them
    /// is a wire-compatibility break.
    #[test]
    fn error_strings_are_byte_exact() {
        assert_eq!(
            CmdError::WrongType.to_string(),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        assert_eq!(
            CmdError::WrongArity("get").to_string(),
            "ERR wrong number of arguments for 'get' command"
        );
        assert_eq!(
            CmdError::NotAnInteger.to_string(),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(
            CmdError::NotAFloat.to_string(),
            "ERR value is not a valid float"
        );
        assert_eq!(CmdError::Syntax.to_string(), "ERR syntax error");
        assert_eq!(
            CmdError::OutOfRange.to_string(),
            "ERR value is out of range"
        );
        assert_eq!(
            CmdError::IndexOutOfRange.to_string(),
            "ERR index out of range"
        );
        assert_eq!(CmdError::NoSuchKey.to_string(), "ERR no such key");
        assert_eq!(
            CmdError::ValueOverflow.to_string(),
            "ERR increment or decrement would overflow"
        );
        assert_eq!(
            CmdError::Unauthenticated.to_string(),
            "NOAUTH Authentication required."
        );
        assert_eq!(
            CmdError::NoProto.to_string(),
            "NOPROTO unsupported protocol version"
        );
        assert_eq!(
            CmdError::Oom.to_string(),
            "OOM command not allowed when used memory > 'maxmemory'."
        );
        assert_eq!(
            CmdError::out_of_range_positive().to_string(),
            "ERR value is out of range, must be positive"
        );
        assert_eq!(
            CmdError::custom("BUSYGROUP", "Consumer Group name already exists").to_string(),
            "BUSYGROUP Consumer Group name already exists"
        );
    }

    #[test]
    fn unknown_command_format() {
        let rest = vec![
            bytes::Bytes::from_static(b"a"),
            bytes::Bytes::from_static(b"b"),
        ];
        assert_eq!(
            CmdError::unknown_command(b"FOO", &rest).to_string(),
            "ERR unknown command 'FOO', with args beginning with: 'a' 'b' "
        );
        assert_eq!(
            CmdError::unknown_command(b"FOO", &[]).to_string(),
            "ERR unknown command 'FOO', with args beginning with: "
        );
    }

    #[test]
    fn codes() {
        assert_eq!(CmdError::WrongType.code(), "WRONGTYPE");
        assert_eq!(CmdError::Syntax.code(), "ERR");
        assert_eq!(CmdError::custom("BUSYGROUP", "x").code(), "BUSYGROUP");
    }

    #[test]
    fn error_strings_contain_no_crlf() {
        // A CR or LF inside an error would desynchronise the protocol.
        for e in [
            CmdError::WrongType,
            CmdError::Syntax,
            CmdError::Oom,
            CmdError::Unauthenticated,
        ] {
            let s = e.to_string();
            assert!(!s.contains('\r') && !s.contains('\n'));
        }
    }
}
