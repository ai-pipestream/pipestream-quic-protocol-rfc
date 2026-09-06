use super::{Error, require};
use minicbor::{Decoder, Encoder};

pub(super) struct Writer(Encoder<Vec<u8>>);

impl Writer {
    pub fn new() -> Self {
        Self(Encoder::new(Vec::new()))
    }
    pub fn array(&mut self, n: usize) {
        self.0.array(n as u64).expect("Vec writer");
    }
    pub fn array_u64(&mut self, n: u64) {
        self.0.array(n).expect("Vec writer");
    }
    pub fn uint(&mut self, n: u64) {
        self.0.u64(n).expect("Vec writer");
    }
    pub fn bytes(&mut self, s: &[u8]) {
        self.0.bytes(s).expect("Vec writer");
    }
    pub fn text(&mut self, s: &str) {
        self.0.str(s).expect("Vec writer");
    }
    pub fn boolean(&mut self, b: bool) {
        self.0.bool(b).expect("Vec writer");
    }
    pub fn null(&mut self) {
        self.0.null().expect("Vec writer");
    }
    pub fn finish(self) -> Vec<u8> {
        self.0.into_writer()
    }
}

pub(super) fn malformed(_: minicbor::decode::Error) -> Error {
    Error::frame("invalid typed CBOR field")
}

pub(super) fn array(d: &mut Decoder<'_>, size: usize) -> Result<(), Error> {
    require(
        d.array().map_err(malformed)? == Some(size as u64),
        "wrong array cardinality",
    )
}

pub(super) trait Wire: Sized {
    fn read(d: &mut Decoder<'_>) -> Result<Self, Error>;
    fn write(&self, w: &mut Writer);
    fn check(&self) -> Result<(), Error>;
}

pub(super) fn decode<T: Wire>(bytes: &[u8], limit: usize) -> Result<T, Error> {
    require(
        !bytes.is_empty() && bytes.len() <= limit,
        "CBOR body exceeds bound",
    )?;
    // This allocation-free pass enforces shortest representations and bounds
    // nesting. Typed readers below further reject all V2-forbidden data types.
    crate::deterministic::validate(bytes).map_err(|_| Error::frame("noncanonical CBOR"))?;
    let mut d = Decoder::new(bytes);
    let value = T::read(&mut d)?;
    require(d.position() == bytes.len(), "trailing CBOR item")?;
    value.check()?;
    Ok(value)
}

pub(super) fn encode<T: Wire>(value: &T, limit: usize) -> Result<Vec<u8>, Error> {
    value.check()?;
    let mut w = Writer::new();
    value.write(&mut w);
    let bytes = w.finish();
    require(bytes.len() <= limit, "encoded body exceeds bound")?;
    Ok(bytes)
}

impl Wire for bool {
    fn read(d: &mut Decoder<'_>) -> Result<Self, Error> {
        d.bool().map_err(malformed)
    }
    fn write(&self, w: &mut Writer) {
        w.boolean(*self);
    }
    fn check(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl<T: Wire> Wire for Option<T> {
    fn read(d: &mut Decoder<'_>) -> Result<Self, Error> {
        if d.datatype().map_err(malformed)? == minicbor::data::Type::Null {
            d.null().map_err(malformed)?;
            Ok(None)
        } else {
            Ok(Some(T::read(d)?))
        }
    }
    fn write(&self, w: &mut Writer) {
        if let Some(value) = self {
            value.write(w);
        } else {
            w.null();
        }
    }
    fn check(&self) -> Result<(), Error> {
        self.as_ref().map_or(Ok(()), Wire::check)
    }
}

impl<T: Wire> Wire for Box<T> {
    fn read(d: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(Box::new(T::read(d)?))
    }
    fn write(&self, w: &mut Writer) {
        self.as_ref().write(w);
    }
    fn check(&self) -> Result<(), Error> {
        self.as_ref().check()
    }
}

impl<T: Wire> Wire for Vec<T> {
    fn read(d: &mut Decoder<'_>) -> Result<Self, Error> {
        let n = d
            .array()
            .map_err(malformed)?
            .ok_or_else(|| Error::frame("indefinite list"))?;
        // Every list in V2 is bounded by 256; capability lists have a further
        // bound of 32. Never allocate from the untrusted length before checking.
        require(
            n <= 256 && n <= (d.input().len() - d.position()) as u64,
            "oversize list",
        )?;
        let mut values = Vec::with_capacity(n as usize);
        for _ in 0..n {
            values.push(T::read(d)?);
        }
        Ok(values)
    }
    fn write(&self, w: &mut Writer) {
        w.array(self.len());
        for value in self {
            value.write(w);
        }
    }
    fn check(&self) -> Result<(), Error> {
        require(self.len() <= 256, "oversize list")?;
        self.iter().try_for_each(Wire::check)
    }
}

macro_rules! record {
    ($name:ident { $($field:ident: $ty:ty),* $(,)? } |$s:ident| $body:block) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name { $(pub $field: $ty),* }
        impl $crate::v2::codec::Wire for $name {
            fn read(d: &mut minicbor::Decoder<'_>) -> Result<Self, $crate::v2::Error> {
                $crate::v2::codec::array(d, [$(stringify!($field)),*].len())?;
                let value = Self { $($field: <$ty as $crate::v2::codec::Wire>::read(d)?),* };
                value.check()?;
                Ok(value)
            }
            fn write(&self, w: &mut $crate::v2::codec::Writer) {
                w.array([$(stringify!($field)),*].len());
                $(self.$field.write(w);)*
            }
            fn check(&self) -> Result<(), $crate::v2::Error> {
                $(self.$field.check()?;)*
                let $s = self;
                $body
            }
        }
    };
}
pub(super) use record;

macro_rules! message {
    ($name:ident { $($opcode:literal => $variant:ident { $($field:ident: $ty:ty),* $(,)? }),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum $name { $($variant { $($field: $ty),* }),* }
        impl $crate::v2::codec::Wire for $name {
            fn read(d: &mut minicbor::Decoder<'_>) -> Result<Self, $crate::v2::Error> {
                let count = d.array().map_err($crate::v2::codec::malformed)?;
                match d.u64().map_err($crate::v2::codec::malformed)? {
                    $($opcode => {
                        $crate::v2::require(count == Some(1 + [$(stringify!($field)),*].len() as u64), "wrong message cardinality")?;
                        let value = Self::$variant { $($field: <$ty as $crate::v2::codec::Wire>::read(d)?),* };
                        value.check()?;
                        Ok(value)
                    }),*
                    _ => Err($crate::v2::Error::frame("unknown message discriminant")),
                }
            }
            fn write(&self, w: &mut $crate::v2::codec::Writer) {
                match self { $(Self::$variant { $($field),* } => {
                    w.array(1 + [$(stringify!($field)),*].len());
                    w.uint($opcode);
                    $($field.write(w);)*
                }),* }
            }
            fn check(&self) -> Result<(), $crate::v2::Error> {
                match self { $(Self::$variant { $($field),* } => {
                    $($field.check()?;)*
                }),* }
                self.check_fields()
            }
        }
    };
}
pub(super) use message;
