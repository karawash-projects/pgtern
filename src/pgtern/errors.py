# SPDX-License-Identifier: Apache-2.0
"""DB-API 2.0 exceptions, plus one subclass per common SQLSTATE.

Server errors are raised as the class registered for their SQLSTATE
(``errors.UniqueViolation`` for ``23505``, ...); unknown codes fall back to the DB-API
class for their first two characters (``DatabaseError`` if even that is unknown).
``str(exc)`` starts with the server's primary message; DETAIL, HINT and CONTEXT follow
on later lines. The class layout follows the DB-API conventions.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable


class Warning(Exception):  # noqa: A001 - the name is required by DB-API 2.0
    pass


_DIAG_FIELDS = {
    "severity": "S", "severity_nonlocalized": "V", "sqlstate": "C", "message_primary": "M",
    "message_detail": "D", "message_hint": "H", "statement_position": "P",
    "internal_position": "p", "internal_query": "q", "context": "W", "schema_name": "s",
    "table_name": "t", "column_name": "c", "datatype_name": "d", "constraint_name": "n",
    "source_file": "F", "source_line": "L", "source_function": "R",
}


class Diagnostic:
    """The fields of a server ErrorResponse (``diag.message_primary``, ``diag.constraint_name``, ...)."""

    __slots__ = ("_fields",)

    if TYPE_CHECKING:  # the properties below are generated at import time; declared here for type checkers
        severity: str | None
        severity_nonlocalized: str | None
        sqlstate: str | None
        message_primary: str | None
        message_detail: str | None
        message_hint: str | None
        statement_position: str | None
        internal_position: str | None
        internal_query: str | None
        context: str | None
        schema_name: str | None
        table_name: str | None
        column_name: str | None
        datatype_name: str | None
        constraint_name: str | None
        source_file: str | None
        source_line: str | None
        source_function: str | None

    def __init__(self, fields: dict[str, str]) -> None:
        self._fields = dict(fields)

    def __repr__(self) -> str:
        return f"Diagnostic(sqlstate={self.sqlstate!r}, message_primary={self.message_primary!r})"


def _field_getter(code: str) -> Callable[[Diagnostic], str | None]:
    def get(self: Diagnostic) -> str | None:
        return self._fields.get(code)
    return get


for _name, _code in _DIAG_FIELDS.items():
    setattr(Diagnostic, _name, property(_field_getter(_code)))


class Error(Exception):
    sqlstate: str | None = None

    def __init__(self, *args: Any, diag: Diagnostic | None = None) -> None:
        super().__init__(*args)
        self.diag = diag if diag is not None else Diagnostic({})


class InterfaceError(Error): ...
class DatabaseError(Error): ...
class DataError(DatabaseError): ...
class OperationalError(DatabaseError): ...
class IntegrityError(DatabaseError): ...
class InternalError(DatabaseError): ...
class ProgrammingError(DatabaseError): ...
class NotSupportedError(DatabaseError): ...


class PipelineAborted(OperationalError):
    """A statement in a pipeline did not run because an earlier statement in the same batch failed."""


# SQLSTATE class (first two characters) -> DB-API base class. Any class not listed here
# (03, 09, 0B, 0F, 0L, 0P, 72, ...) maps to plain DatabaseError. The table is checked against
# unknown codes are covered by tests/test_unit.py (test_unknown_sqlstates_fall_back_to_their_dbapi_class).
_BASE_BY_CLASS: dict[str, type[Error]] = {
    "08": OperationalError, "0A": NotSupportedError, "20": ProgrammingError, "21": ProgrammingError,
    "22": DataError, "23": IntegrityError, "24": InternalError, "25": InternalError,
    "26": ProgrammingError, "27": OperationalError, "28": OperationalError, "2B": InternalError,
    "2D": InternalError, "2F": OperationalError, "34": ProgrammingError, "38": OperationalError,
    "39": OperationalError, "3B": OperationalError, "3D": ProgrammingError, "3F": ProgrammingError,
    "40": OperationalError, "42": ProgrammingError, "44": ProgrammingError, "53": OperationalError,
    "54": OperationalError, "55": OperationalError, "57": OperationalError, "58": OperationalError,
    "F0": OperationalError, "HV": OperationalError, "P0": ProgrammingError, "XX": InternalError,
}

# "<sqlstate> <ClassName>". Codes ending in 000 are class-level parents of the rest of their class.
_SPEC = """
08000 ConnectionException
08001 SqlclientUnableToEstablishSqlconnection
08003 ConnectionDoesNotExist
08004 SqlserverRejectedEstablishmentOfSqlconnection
08006 ConnectionFailure
08P01 ProtocolViolation
0A000 FeatureNotSupported
21000 CardinalityViolation
22000 DataException
22001 StringDataRightTruncation
22002 NullValueNoIndicatorParameter
22003 NumericValueOutOfRange
22007 InvalidDatetimeFormat
22008 DatetimeFieldOverflow
22012 DivisionByZero
22021 CharacterNotInRepertoire
22023 InvalidParameterValue
22026 StringDataLengthMismatch
22P02 InvalidTextRepresentation
22P03 InvalidBinaryRepresentation
22P05 UntranslatableCharacter
23000 IntegrityConstraintViolation
23001 RestrictViolation
23502 NotNullViolation
23503 ForeignKeyViolation
23505 UniqueViolation
23514 CheckViolation
23P01 ExclusionViolation
24000 InvalidCursorState
25000 InvalidTransactionState
25001 ActiveSqlTransaction
25006 ReadOnlySqlTransaction
25P01 NoActiveSqlTransaction
25P02 InFailedSqlTransaction
25P03 IdleInTransactionSessionTimeout
26000 InvalidSqlStatementName
28000 InvalidAuthorizationSpecification
28P01 InvalidPassword
2BP01 DependentObjectsStillExist
2D000 InvalidTransactionTermination
34000 InvalidCursorName
38000 ExternalRoutineException
3B000 SavepointException
3D000 InvalidCatalogName
3F000 InvalidSchemaName
40000 TransactionRollback
40001 SerializationFailure
40002 TransactionIntegrityConstraintViolation
40003 StatementCompletionUnknown
40P01 DeadlockDetected
42000 SyntaxErrorOrAccessRuleViolation
42501 InsufficientPrivilege
42601 SyntaxError
42602 InvalidName
42611 InvalidColumnDefinition
42622 NameTooLong
42701 DuplicateColumn
42702 AmbiguousColumn
42703 UndefinedColumn
42704 UndefinedObject
42710 DuplicateObject
42712 DuplicateAlias
42723 DuplicateFunction
42725 AmbiguousFunction
42803 GroupingError
42804 DatatypeMismatch
42809 WrongObjectType
42830 InvalidForeignKey
42846 CannotCoerce
42883 UndefinedFunction
42939 ReservedName
42P01 UndefinedTable
42P02 UndefinedParameter
42P03 DuplicateCursor
42P04 DuplicateDatabase
42P05 DuplicatePreparedStatement
42P06 DuplicateSchema
42P07 DuplicateTable
42P08 AmbiguousParameter
42P09 AmbiguousAlias
42P10 InvalidColumnReference
42P16 InvalidTableDefinition
42P17 InvalidObjectDefinition
42P18 IndeterminateDatatype
44000 WithCheckOptionViolation
53000 InsufficientResources
53100 DiskFull
53200 OutOfMemory
53300 TooManyConnections
54000 ProgramLimitExceeded
55000 ObjectNotInPrerequisiteState
55006 ObjectInUse
55P02 CantChangeRuntimeParam
55P03 LockNotAvailable
57000 OperatorIntervention
57014 QueryCanceled
57P01 AdminShutdown
57P02 CrashShutdown
57P03 CannotConnectNow
57P04 DatabaseDropped
58000 SystemError
58030 IoError
P0000 PlpgsqlError
P0001 RaiseException
P0002 NoDataFound
P0003 TooManyRows
P0004 AssertFailure
XX000 InternalError_
XX001 DataCorrupted
XX002 IndexCorrupted
"""

_BY_SQLSTATE: dict[str, type[Error]] = {}
_ENTRIES = [line.split() for line in _SPEC.strip().splitlines()]

# The hierarchy is flat: every specific class derives directly from its DB-API
# family (UniqueViolation -> IntegrityError).
for _code, _name in _ENTRIES:
    _cls = type(_name, (_BASE_BY_CLASS.get(_code[:2], DatabaseError),),
                {"sqlstate": _code, "__module__": __name__})
    globals()[_name] = _cls
    _BY_SQLSTATE[_code] = _cls

__all__ = [
    "Warning", "Error", "InterfaceError", "DatabaseError", "DataError", "OperationalError",
    "IntegrityError", "InternalError", "ProgrammingError", "NotSupportedError", "Diagnostic", "PipelineAborted",
    *(name for _, name in _ENTRIES),
]


def error_from_fields(fields: dict[str, str]) -> Error:
    """Build the right exception from the fields of a server ErrorResponse."""
    sqlstate = fields.get("C") or "XX000"
    cls = _BY_SQLSTATE.get(sqlstate) or _BASE_BY_CLASS.get(sqlstate[:2], DatabaseError)
    lines = [fields.get("M", "unknown error")]
    for key, label in (("D", "DETAIL"), ("H", "HINT"), ("W", "CONTEXT")):
        if key in fields:
            lines.append(f"{label}:  {fields[key]}")
    exc = cls("\n".join(lines), diag=Diagnostic(fields))
    exc.sqlstate = sqlstate
    return exc
