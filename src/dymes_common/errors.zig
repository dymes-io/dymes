//! Common Errors.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

pub const UsageError = error{
    IllegalArgument,
    MissingArgument,
    IllegalConversion,
    InvalidRequest,
    OtherUsageFailure,
    UnsupportedOperation,
};

pub const AllocationError = error{
    OutOfMemory,
    LimitReached,
};

pub const CreationError = AllocationError || error{
    FileAlreadyExists,
    DuplicateEntry,
    OutOfSpace,
    GenerationFailure,
    OutOfOrderCreation,
    OtherCreationFailure,
};

pub const AccessError = error{
    AccessFailure,
    FileNotFound,
    EntryNotFound,
    OtherAccessFailure,
};

pub const IoError = error{
    WriteFailed,
    ReadFailed,
    WouldBlock,
};

pub const TimeoutError = error{
    TimedOut,
};

pub const StateError = error{
    IllegalState,
    InconsistentState,
    ChecksumFailure,
    DataTruncated,
    Interrupted,
    NotReady,
};

pub const SocketError = error{
    SocketFailure,
    BindFailure,
    ListenFailure,
    ConnectFailure,
};

pub const NetworkError = TimeoutError || SocketError;

pub const AddressError = error{
    AddressResolutionFailure,
    UnsupportedAddressFamily,
    OutOfMemory,
};

pub const ServerError = error{
    ServerFailure,
    ServerHealthFailure,
};

pub const ClientError = error{
    ClientFailure,
    ClientHealthFailure,
};

pub const PeerError = error{
    PeerFailure,
    PeerHealthFailure,
};

pub const URIParseError = error{
    InvalidFormat,
    UnexpectedCharacter,
    InvalidPort,
};
