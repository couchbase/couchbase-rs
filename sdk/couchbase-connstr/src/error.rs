/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use std::fmt::{Display, Formatter};
use std::{fmt, io};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Error {
    pub(crate) kind: ErrorKind,
}

impl Error {
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    Parse(String),
    InvalidArgument {
        msg: String,
        arg: String,
    },
    Io(io::Error),
    #[cfg(feature = "dns-srv")]
    Resolve(hickory_resolver::ResolveError),
}

impl Clone for ErrorKind {
    fn clone(&self) -> Self {
        match self {
            ErrorKind::Parse(s) => ErrorKind::Parse(s.clone()),
            ErrorKind::InvalidArgument { msg, arg } => ErrorKind::InvalidArgument {
                msg: msg.clone(),
                arg: arg.clone(),
            },
            ErrorKind::Io(e) => Self::Io(io::Error::from(e.kind())),
            #[cfg(feature = "dns-srv")]
            ErrorKind::Resolve(e) => Self::Resolve(e.clone()),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.kind, f)
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::Parse(s) => write!(f, "Parse error: {s}"),
            ErrorKind::InvalidArgument { msg, arg } => {
                write!(f, "Invalid argument: {msg} ({arg})")
            }
            ErrorKind::Io(e) => write!(f, "IO error: {e}"),
            #[cfg(feature = "dns-srv")]
            ErrorKind::Resolve(e) => write!(f, "Resolve error: {e}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self {
            kind: ErrorKind::Io(e),
        }
    }
}

#[cfg(feature = "dns-srv")]
impl From<hickory_resolver::ResolveError> for Error {
    fn from(e: hickory_resolver::ResolveError) -> Self {
        Self {
            kind: ErrorKind::Resolve(e),
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { kind }
    }
}
