use super::{Error, Id, Number, OutputIndex, Producer, WorkKey, codec::*, require};
use minicbor::Decoder;
use std::net::Ipv6Addr;

/// Exact locator text is retained for manifest byte commitments. Parsed endpoint
/// fields are for comparison only, never authorization or credential discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultLocator(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultTarget {
    pub host: String,
    pub port: u16,
    pub generation: Id,
    pub work: WorkKey,
    pub attempt: Id,
    pub index: OutputIndex,
}

fn decimal(text: &str) -> Result<u64, Error> {
    require(
        !text.is_empty()
            && (text.len() == 1 || !text.starts_with('0'))
            && text.bytes().all(|b| b.is_ascii_digit()),
        "invalid locator decimal",
    )?;
    text.parse()
        .map_err(|_| Error::frame("locator integer overflow"))
}

impl ResultLocator {
    pub fn target(&self) -> Result<ResultTarget, Error> {
        let value = &self.0;
        require(
            value.len() <= 1024 && value.is_ascii() && !value.contains(['@', '?', '#', '%']),
            "invalid locator encoding",
        )?;
        let (scheme, remainder) = value
            .split_once("://")
            .ok_or_else(|| Error::frame("locator scheme absent"))?;
        require(
            scheme.eq_ignore_ascii_case("pipestream"),
            "wrong locator scheme",
        )?;
        let (authority, path) = remainder
            .split_once('/')
            .ok_or_else(|| Error::frame("locator path absent"))?;
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| Error::frame("explicit port absent"))?;
        let host = if let Some(address) = host.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            address
                .parse::<Ipv6Addr>()
                .map_err(|_| Error::frame("invalid IPv6 locator"))?
                .to_string()
        } else {
            require(
                !host.is_empty()
                    && host.len() <= 253
                    && host.split('.').all(|s| {
                        !s.is_empty()
                            && s.len() <= 63
                            && !s.starts_with('-')
                            && !s.ends_with('-')
                            && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                    }),
                "invalid DNS locator",
            )?;
            host.to_ascii_lowercase()
        };
        let port =
            u16::try_from(decimal(port)?).map_err(|_| Error::frame("locator port overflow"))?;
        require(port != 0, "zero locator port")?;
        let parts: Vec<_> = path.split('/').collect();
        let [
            "v2",
            "sessions",
            generation,
            "scopes",
            scope,
            "producers",
            producer,
            "entities",
            entity,
            "attempts",
            attempt,
            "outputs",
            index,
        ] = parts.as_slice()
        else {
            return Err(Error::frame("invalid V2 result path"));
        };
        let target = ResultTarget {
            host,
            port,
            generation: Id(decimal(generation)?),
            work: WorkKey {
                scope: Number(decimal(scope)?),
                producer: Producer(decimal(producer)?),
                entity: Id(decimal(entity)?),
            },
            attempt: Id(decimal(attempt)?),
            index: OutputIndex(decimal(index)?),
        };
        target.generation.check()?;
        target.work.check()?;
        target.attempt.check()?;
        target.index.check()?;
        Ok(target)
    }
}

impl Wire for ResultLocator {
    fn read(d: &mut Decoder<'_>) -> Result<Self, Error> {
        let text = d.str().map_err(malformed)?;
        require(text.len() <= 1024, "locator too long")?;
        let value = Self(text.to_owned());
        value.check()?;
        Ok(value)
    }
    fn write(&self, w: &mut Writer) {
        w.text(&self.0);
    }
    fn check(&self) -> Result<(), Error> {
        self.target().map(|_| ())
    }
}
