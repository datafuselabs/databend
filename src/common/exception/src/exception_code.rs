// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(non_snake_case)]

use crate::exception_backtrace::capture;
use crate::ErrorCode;

macro_rules! build_exceptions {
    ($($(#[$meta:meta])* $body:ident($code:expr)),*$(,)*) => {
        impl ErrorCode {
            $(

                paste::item! {
                    $(
                        #[$meta]
                    )*
                    pub const [< $body:snake:upper >]: u16 = $code;
                }
                $(
                    #[$meta]
                )*
                pub fn $body(display_text: impl Into<String>) -> ErrorCode {
                    let bt = capture();
                    ErrorCode::create(
                        $code,
                        stringify!($body),
                        display_text.into(),
                        String::new(),
                        None,
                        bt,
                    )
                }
            )*
        }
    }
}

// Internal errors [0, 2000].
build_exceptions! {
    Ok(0),

    /// Internal means this is the internal error that no action
    /// can be taken by neither developers or users.
    /// In most of the time, they are code bugs.
    ///
    /// If there is an error that are unexpected and no other actions
    /// to taken, please use this error code.
    ///
    /// # Notes
    ///
    /// This error should never be used to for error checking. An error
    /// that returns as internal error could be assigned a separate error
    /// code at anytime.
    Internal(1001),

    /// Unimplemented means this is a not implemented feature.
    ///
    /// Developers could implement the feature to resolve this error at anytime,
    ///
    /// # Notes
    ///
    /// It's OK to use this error code for not implemented feature in
    /// our dependences. For example, in arrow.
    Unimplemented(1002),

    // Legacy error codes, we will refactor them one by one.

    UnknownDatabase(1003),
    UnknownDatabaseId(1004),
    SyntaxException(1005),
    BadArguments(1006),
    IllegalDataType(1007),
    UnknownFunction(1008),
    BadDataValueType(1010),
    EmptyData(1016),
    DataStructMissMatch(1017),
    BadDataArrayLength(1018),
    UnknownTableId(1020),
    UnknownTable(1025),
    UnknownView(1026),
    UnknownAggregateFunction(1027),
    NumberArgumentsNotMatch(1028),
    EmptyDataFromServer(1030),
    NotFoundClusterNode(1035),
    BadAddressFormat(1036),
    DnsParseError(1037),
    CannotConnectNode(1038),
    TooManyUserConnections(1041),
    AbortedSession(1042),
    AbortedQuery(1043),
    ClosedQuery(1044),
    CannotListenerPort(1045),
    BadBytes(1046),
    InitPrometheusFailure(1047),
    Overflow(1049),
    AuthenticateFailure(1051),
    TLSConfigurationFailure(1052),
    UnknownSession(1053),
    SHA1CheckFailed(1057),
    UnknownColumn(1058),
    StrParseError(1060),
    IllegalGrant(1061),
    ManagementModePermissionDenied(1062),
    PermissionDenied(1063),
    UnmarshalError(1064),
    SemanticError(1065),
    NeedChangePasswordDenied(1066),
    UnknownException(1067),
    TokioError(1068),
    HttpNotFound(1072),
    UnknownFormat(1074),
    UnknownCompressionType(1075),
    InvalidCompressionData(1076),
    InvalidAuthInfo(1077),
    InvalidTimezone(1078),
    InvalidDate(1079),
    InvalidTimestamp(1080),
    InvalidClusterKeys(1081),
    UnknownFragmentExchange(1082),
    TenantIsEmpty(1101),
    IndexOutOfBounds(1102),
    LayoutError(1103),
    PanicError(1104),
    TableInfoError(1106),
    ReadTableDataError(1107),
    AddColumnExistError(1108),
    DropColumnEmptyError(1109),
    // create table or alter table add column with internal column name
    TableWithInternalColumnName(1110),
    EmptyShareEndpointConfig(1111),
    LicenceDenied(1112),
    UnknownDatamask(1113),
    UnmatchColumnDataType(1114),
    VirtualColumnNotFound(1115),
    VirtualColumnAlreadyExists(1116),
    ColumnReferencedByComputedColumn(1117),
    ColumnReferencedByInvertedIndex(1118),
    // The table is not a clustered table.
    UnclusteredTable(1118),
    UnknownCatalog(1119),
    UnknownCatalogType(1120),
    UnmatchMaskPolicyReturnType(1121),
    Timeout(1122),
    Outdated(1123),
    // sequence
    OutofSequenceRange(1124),
    WrongSequenceCount(1125),
    UnknownSequence(1126),
    UnknownQuery(1127),

    // Data Related Errors

    /// ParquetFileInvalid is used when given parquet file is invalid.
    ParquetFileInvalid(1201),
    /// InvalidUtf8String is used when given string is not a valid utf8 string.
    InvalidUtf8String(1202),

    // Table related errors starts here.

    /// TableOptionInvalid is used when users input an invalid option.
    ///
    /// For example: try to set a reserved table option.
    TableOptionInvalid(1301),
    /// TableEngineMismatch is used when users try to do not supported
    /// operations on specified engine.
    ///
    /// For example: drop on `view` engine.
    TableEngineNotSupported(1302),
    /// TableSchemaMismatch is used when table's schema is not match with input
    ///
    /// For example: try to with 3 columns into a table with 4 columns.
    TableSchemaMismatch(1303),

    // License related errors starts here

    /// LicenseKeyParseError is used when license key cannot be pared by the jwt public key
    ///
    /// For example: license key is not valid
    LicenseKeyParseError(1401),

    /// LicenseKeyInvalid is used when license key verification error occurs
    ///
    /// For example: license key is expired
    LicenseKeyInvalid(1402),
    EnterpriseFeatureNotEnable(1403),

    BackgroundJobAlreadyExists(1501),
    UnknownBackgroundJob(1502),

    InvalidRowIdIndex(1503),
    // Index related errors.
    UnsupportedIndex(1601),
    RefreshIndexError(1602),
    IndexOptionInvalid(1603),

    // Cloud control error codes
    CloudControlConnectError(1701),
    CloudControlNotEnabled(1702),
    IllegalCloudControlMessageFormat(1703),

    // Geometry errors.
    GeometryError(1801),
    InvalidGeometryFormat(1802),
    // Tantivy errors.
    TantivyError(1901),
    TantivyOpenReadError(1902),
    TantivyQueryParserError(1903),

    ReqwestError(1910)
}

// Meta service errors [2001, 3000].
build_exceptions! {
    // Meta service does not work.
    MetaServiceError(2001),
    InvalidConfig(2002),
    MetaStorageError(2003),
    InvalidArgument(2004),
    // Meta service replied with invalid data
    InvalidReply(2005),

    TableVersionMismatched(2009),
    OCCRetryFailure(2011),
    TableNotWritable(2012),
    TableHistoricalDataNotFound(2013),
    DuplicatedUpsertFiles(2014),
    TableAlreadyLocked(2015),
    TableLockExpired(2016),

    // User api error codes.
    UnknownUser(2201),
    UserAlreadyExists(2202),
    IllegalUserInfoFormat(2203),
    UnknownRole(2204),
    InvalidRole(2206),
    UnknownNetworkPolicy(2207),
    NetworkPolicyAlreadyExists(2208),
    IllegalNetworkPolicy(2209),
    NetworkPolicyIsUsedByUser(2210),
    UnknownPasswordPolicy(2211),
    PasswordPolicyAlreadyExists(2212),
    IllegalPasswordPolicy(2213),
    PasswordPolicyIsUsedByUser(2214),
    InvalidPassword(2215),
    RoleAlreadyExists(2216),
    IllegalRole(2217),
    IllegalUser(2218),

    // Meta api error codes.
    DatabaseAlreadyExists(2301),
    TableAlreadyExists(2302),
    ViewAlreadyExists(2306),
    CreateTableWithDropTime(2307),
    UndropTableAlreadyExists(2308),
    UndropTableHasNoHistory(2309),
    CreateDatabaseWithDropTime(2310),
    UndropDbHasNoHistory(2312),
    UndropTableWithNoDropTime(2313),
    DropTableWithDropTime(2314),
    DropDbWithDropTime(2315),
    UndropDbWithNoDropTime(2316),
    TxnRetryMaxTimes(2317),
    /// `CatalogNotSupported` should be raised when defining a catalog, which is:
    /// - not supported by the application, like creating a `HIVE` catalog but `HIVE` feature not enabled;
    /// - forbidden to create, like creating a `DEFAULT` catalog
    CatalogNotSupported(2318),
    /// `CatalogAlreadyExists` should be raised when defining a catalog, which is:
    /// - having the same name as a already exist, like `default`
    /// - and without `IF NOT EXISTS`
    CatalogAlreadyExists(2319),
    /// `CatalogNotFound` should be raised when trying to drop a catalog that is:
    /// - not exists.
    /// - and without `IF EXISTS`
    CatalogNotFound(2320),
    /// data mask error codes
    DatamaskAlreadyExists(2321),

    CommitTableMetaError(2322),
    CreateAsDropTableWithoutDropTime(2323),


    // Cluster error codes.
    ClusterUnknownNode(2401),
    ClusterNodeAlreadyExists(2402),

    // Stage error codes.
    UnknownStage(2501),
    StageAlreadyExists(2502),
    IllegalUserStageFormat(2503),
    StageFileAlreadyExists(2504),
    IllegalStageFileFormat(2505),
    StagePermissionDenied(2506),

    // FileFormat error codes.
    UnknownFileFormat(2507),
    IllegalFileFormat(2508),
    FileFormatAlreadyExists(2509),

    // Connection error codes.
    UnknownConnection(2510),
    IllegalConnection(2511),
    ConnectionAlreadyExists(2512),

    // User defined function error codes.
    IllegalUDFFormat(2601),
    UnknownUDF(2602),
    UdfAlreadyExists(2603),
    UDFServerConnectError(2604),
    UDFSchemaMismatch(2605),
    UnsupportedDataType(2606),
    UDFDataError(2607),

    // Database error codes.
    UnknownDatabaseEngine(2701),
    UnknownTableEngine(2702),
    UnsupportedEngineParams(2703),

    // Share error codes.
    ShareAlreadyExists(2705),
    UnknownShare(2706),
    UnknownShareId(2707),
    ShareAccountsAlreadyExists(2708),
    UnknownShareAccounts(2709),
    WrongShareObject(2710),
    WrongShare(2711),
    ShareHasNoGrantedDatabase(2712),
    ShareHasNoGrantedPrivilege(2713),
    ShareEndpointAlreadyExists(2714),
    UnknownShareEndpoint(2715),
    UnknownShareEndpointId(2716),
    CannotAccessShareTable(2717),
    CannotShareDatabaseCreatedFromShare(2718),
    ShareStorageError(2719),

    // Index error codes.
    CreateIndexWithDropTime(2720),
    IndexAlreadyExists(2721),
    UnknownIndex(2722),
    DropIndexWithDropTime(2723),
    GetIndexWithDropTime(2724),
    DuplicatedIndexColumnId(2725),
    IndexColumnIdNotFound(2726),

    // Stream error codes.
    UnknownStream(2730),
    UnknownStreamId(2731),
    StreamAlreadyExists(2732),
    IllegalStream(2733),
    StreamVersionMismatched(2734),

    // dynamic error codes.
    IllegalDynamicTable(2740),

    // Variable error codes.
    UnknownVariable(2801),
    OnlySupportAsciiChars(2802),
    WrongValueForVariable(2803),

    // Tenant quota error codes.
    IllegalTenantQuotaFormat(2901),
    TenantQuotaUnknown(2902),
    TenantQuotaExceeded(2903),

    // Script error codes.
    ScriptSemanticError(3001),
    ScriptExecutionError(3002),

    // sequence
    SequenceError(3101),

    // Share error codes(continue).
    ErrorShareEndpointCredential(3111),
    WrongSharePrivileges(3112),
}

// Storage errors [3001, 4000].
build_exceptions! {
    StorageNotFound(3001),
    StoragePermissionDenied(3002),
    StorageUnavailable(3901),
    StorageUnsupported(3902),
    StorageInsecure(3903),
    DeprecatedIndexFormat(3904),
    InvalidOperation(3905),
    StorageOther(4000),
    UnresolvableConflict(4001),

    // transaction error codes
    CurrentTransactionIsAborted(4002),
    TransactionTimeout(4003),
    InvalidSessionState(4004),

    // recluster error codes
    NoNeedToRecluster(4011),
    NoNeedToCompact(4012),

    RefreshTableInfoFailure(4012),
}

// Service errors [5001,6000].
build_exceptions! {
    // A task that already stopped and can not stopped twice.
    AlreadyStopped(5002),

    SessionTokenExpired(5100),
    RefreshTokenExpired(5101),
    SessionTokenNotFound(5102),
    RefreshTokenNotFound(5103)
}
