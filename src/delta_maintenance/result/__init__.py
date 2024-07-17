from enum import Enum
from typing import Generic, TypeVar, Union

# Define the enum for the result status
class ResultStatus(Enum):
    OK = "OK"
    ERR = "ERR"

# Define type variables
T = TypeVar('T')  # Success type
E = TypeVar('E')  # Error type

# Define the Result class
class Result(Generic[T, E]):
    def __init__(self, status: ResultStatus, value: Union[T, None] = None, error: Union[E, None] = None):
        self.status = status
        self.value = value
        self.error = error

    def is_ok(self) -> bool:
        return self.status == ResultStatus.OK

    def is_err(self) -> bool:
        return self.status == ResultStatus.ERR

    def unwrap(self) -> T:
        if self.is_ok():
            return self.value
        else:
            raise ValueError(f"Called unwrap on an error result: {self.error}")

    def unwrap_err(self) -> E:
        if self.is_err():
            return self.error
        else:
            raise ValueError("Called unwrap_err on an ok result")

    def unwrap_or(self, default: T) -> T:
        if self.is_ok():
            return self.value
        else:
            return default

# Helper functions to create success and error results
def Ok(value: T) -> Result[T, E]:
    return Result(ResultStatus.OK, value=value)

def Err(error: E) -> Result[T, E]:
    return Result(ResultStatus.ERR, error=error)

