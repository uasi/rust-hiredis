extern mod std;
use either::{Either, Left, Right};
use libc::{c_int, c_longlong, c_void};

extern mod hiredis {
    fn redisConnect(ip: *u8, port: c_int) -> *RedisContext;
    fn redisFree(c: *RedisContext);
    fn redisCommandArgv(c: *RedisContext, argc: c_int,
                        argv: **u8, argvlen: *uint) -> *RedisReply;
    fn freeReplyObject(reply: *c_void);
}

pub const DEFAULT_PORT: u16 = 6379;

pub enum RedisResponseValue<RegionedBytesT> {
    RedisResponseNil,
    RedisResponseMulti,
    RedisResponseStatus(~str),
    RedisResponseError(~str),
    RedisResponseI64(i64),
    RedisResponseBytesView(RegionedBytesT /* &r/[u8] */),
    RedisResponseNone,
}

pub struct Redis {
    priv context: *RedisContext,
    drop {
        if ptr::is_not_null(self.context) {
            hiredis::redisFree(self.context);
        }
    }
}

pub struct RedisResponse {
    priv reply_or_error: Either<*RedisReply, ~str>,
    drop {
        match self.reply_or_error {
            Left(reply) => {
                assert ptr::is_not_null(reply);
                hiredis::freeReplyObject(reply as *c_void);
            }
            _ => { }
        }
    }
}

struct RedisContext {
    err: c_int,
    errstr: [u8 * 128],
    fd: c_int,
    flags: c_int,
    obuf: *u8,
    reader: *c_void,
}

struct RedisReply {
    type_: c_int,
    integer: c_longlong,
    len: c_int,
    str_: *u8,
    elements: uint,
    element: **RedisReply,
}

pub trait RedisArg {
    unsafe fn to_buf_with_len() -> (*u8, uint);
}

macro_rules! send(
    ( $redis:expr, $($arg:expr),* ) => (
        unsafe { ($redis).send_([ $( ($arg).to_buf_with_len() ),* ]) }
    );
)

pub impl Redis {
    fn send<T: RedisArg>(args: &[T]) -> ~RedisResponse {
        self.send_(args.map(|a| a.to_buf_with_len()))
    }

    unsafe fn send_(args: &[(*u8, uint)]) -> ~RedisResponse {
        let (bufs, lens) = vec::unzip(vec::from_slice(args));
        let reply = hiredis::redisCommandArgv(
            self.context,
            args.len() as c_int,
            vec::raw::to_ptr(bufs),
            vec::raw::to_ptr(lens));
        if ptr::is_null(reply) {
            return ~RedisResponse {reply_or_error: Right(self.get_errstr()) };
        } else {
            return ~RedisResponse {reply_or_error: Left(reply)};
        }
    }

    priv unsafe fn get_errstr() -> ~str {
        let buf = cast::reinterpret_cast(& &(*self.context).errstr);
        str::raw::from_buf(buf)
    }
}

pub impl RedisResponse {
    fn value(&self) -> RedisResponseValue<&self/[u8]> {
        let reply = self.reply();
        if ptr::is_null(reply) { return RedisResponseNone; }
        unsafe {
            match (*reply).type_ as int {
                1 => RedisResponseBytesView(self.get_bytes_view()),
                2 => RedisResponseMulti,
                3 => RedisResponseI64((*reply).integer as i64),
                4 => RedisResponseNil,
                5 => RedisResponseStatus(self.get_str()),
                6 => RedisResponseError(self.get_str()),
                _ => fail,
            }
        }
    }

    fn nil_value() -> Option<()> {
        match self.value() {
            RedisResponseNil => Some(()),
            _ => None,
        }
    }

    fn status_value() -> Option<~str> {
        match self.value() {
            RedisResponseStatus(move st) => Some(st),
            _ => None,
        }
    }

    fn error_value() -> Option<~str> {
        match self.value() {
            RedisResponseError(move err) => Some(err),
            _ => None,
        }
    }

    fn i64_value() -> Option<i64> {
        match self.value() {
            RedisResponseI64(i) => Some(i),
            _ => None,
        }
    }

    fn str_value() -> Option<~str> {
        match self.value() {
            RedisResponseBytesView(bytes) if str::is_utf8(bytes) =>
                Some(str::from_bytes(bytes)),
            _ => None,
        }
    }

    fn bytes_value() -> Option<~[u8]> {
        match self.value() {
            RedisResponseBytesView(bytes) =>
                Some(vec::from_slice(bytes)),
            _ => None,
        }
    }

    fn bytes_value_view(&self) -> Option<&self/[u8]> {
        match self.value() {
            RedisResponseBytesView(bytes) => Some(bytes),
            _ => None,
        }
    }

    fn has_value() -> bool {
        match self.value() {
            RedisResponseNone => false,
            _ => true,
        }
    }

    fn is_success() -> bool {
        match self.value() {
            RedisResponseNone | RedisResponseError(*) => false,
            _ => true,
        }
    }

    fn error() -> Option<~str> {
        match self.reply_or_error {
            Right(err) => Some(copy err),
            _ => None,
        }
    }

    priv fn reply() -> *RedisReply {
        match self.reply_or_error {
            Left(reply) => reply,
            _ => ptr::null(),
        }
    }

    priv unsafe fn get_str() -> ~str {
        let reply = self.reply();
        assert ptr::is_not_null(reply);
        str::raw::from_buf_len((*reply).str_, (*reply).len as uint)
    }

    priv unsafe fn get_bytes_view(&self) -> &self/[u8] {
        let reply = self.reply();
        assert ptr::is_not_null(reply);
        cast::reinterpret_cast(&((*reply).str_, (*reply).len as uint))
    }
}

impl &str: RedisArg {
    unsafe fn to_buf_with_len() -> (*u8, uint) { str_to_buf_with_len(self) }
}

impl ~str: RedisArg {
    unsafe fn to_buf_with_len() -> (*u8, uint) { str_to_buf_with_len(self) }
}

impl @str: RedisArg {
    unsafe fn to_buf_with_len() -> (*u8, uint) { str_to_buf_with_len(self) }
}

impl &[u8]: RedisArg {
    unsafe fn to_buf_with_len() -> (*u8, uint) { vec_to_buf_with_len(self) }
}

impl ~[u8]: RedisArg {
    unsafe fn to_buf_with_len() -> (*u8, uint) { vec_to_buf_with_len(self) }
}

impl @[u8]: RedisArg {
    unsafe fn to_buf_with_len() -> (*u8, uint) { vec_to_buf_with_len(self) }
}

impl (*u8, uint): RedisArg {
    unsafe fn to_buf_with_len() -> (*u8, uint) { self }
}

impl int: RedisArg {
    unsafe fn to_buf_with_len() -> (*u8, uint) {
        str_to_buf_with_len(fmt!("%d", self))
    }
}

impl uint: RedisArg {
    unsafe fn to_buf_with_len() -> (*u8, uint) {
        str_to_buf_with_len(fmt!("%u", self))
    }
}

impl float: RedisArg {
    unsafe fn to_buf_with_len() -> (*u8, uint) {
        str_to_buf_with_len(fmt!("%f", self))
    }
}

#[inline(always)]
unsafe fn str_to_buf_with_len(s: &str) -> (*u8, uint) {
     do str::as_buf(s) |buf, n| { (buf, n - 1) }
}

#[inline(always)]
unsafe fn vec_to_buf_with_len(v: &[u8]) -> (*u8, uint) {
    do vec::as_imm_buf(v) |buf, n| { (buf, n) }
}

pub fn connect(ip: &str, port: u16) -> Result<~Redis, ~str> {
    let ctx = do str::as_buf(ip) |ip_buf, _n| {
        hiredis::redisConnect(ip_buf, port as c_int)
    };
    let err = unsafe { (*ctx).err };
    let errstr = unsafe {
        let buf = cast::reinterpret_cast(& &(*ctx).errstr);
        str::raw::from_buf(buf)
    };
    if err == 0 { Ok(~Redis {context: ctx}) } else { Err(errstr) }
}


/// Test usage: ./hiredis [[host] port]
#[cfg(test)]
mod test {
    fn setup() -> ~Redis {
        let (host, port) = match os::args().len() {
            2 => (copy os::args()[1], DEFAULT_PORT),
            3 => (copy os::args()[1], u16::from_str(os::args()[2]).get()),
            _ => (~"localhost", DEFAULT_PORT),
        };
        result::unwrap(connect(host, port))
    }

    #[test]
    fn set_and_get() {
        let redis = setup();
        {
            let key = "rust-hiredis:set_and_get";
            let res = redis.send(["SET", key, "val"]);
            assert res.is_success();
            let res = redis.send(["GET", key]);
            assert res.str_value() == Some(~"val");
        }
        {
            let key = ~"rust-hiredis:set_and_get";
            let res = redis.send([~"SET", copy key, ~"val"]);
            assert res.is_success();
            let res = redis.send([~"GET", copy key]);
            assert res.str_value() == Some(~"val");
        }
    }

    #[test]
    fn set_and_get_macro() {
        let redis = setup();
        let key = ~"rust-hiredis:set_and_get_macro";
        let val = vec::from_elem(5, 0u8);
        let res = send!(redis, "SET", key, val);
        assert res.is_success();
        let res = send!(redis, "GET", key);
        assert res.bytes_value() == Some(val);
    }
}
