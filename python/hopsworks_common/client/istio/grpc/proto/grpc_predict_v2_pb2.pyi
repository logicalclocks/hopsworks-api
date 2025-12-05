from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import (
    ClassVar as _ClassVar,
)

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class InferParameter(_message.Message):
    __slots__ = ["bool_param", "int64_param", "string_param"]
    BOOL_PARAM_FIELD_NUMBER: _ClassVar[int]
    INT64_PARAM_FIELD_NUMBER: _ClassVar[int]
    STRING_PARAM_FIELD_NUMBER: _ClassVar[int]
    bool_param: bool
    int64_param: int
    string_param: str
    def __init__(
        self,
        bool_param: bool = ...,
        int64_param: int | None = ...,
        string_param: str | None = ...,
    ) -> None: ...

class InferTensorContents(_message.Message):
    __slots__ = [
        "bool_contents",
        "bytes_contents",
        "fp32_contents",
        "fp64_contents",
        "int64_contents",
        "int_contents",
        "uint64_contents",
        "uint_contents",
    ]
    BOOL_CONTENTS_FIELD_NUMBER: _ClassVar[int]
    BYTES_CONTENTS_FIELD_NUMBER: _ClassVar[int]
    FP32_CONTENTS_FIELD_NUMBER: _ClassVar[int]
    FP64_CONTENTS_FIELD_NUMBER: _ClassVar[int]
    INT64_CONTENTS_FIELD_NUMBER: _ClassVar[int]
    INT_CONTENTS_FIELD_NUMBER: _ClassVar[int]
    UINT64_CONTENTS_FIELD_NUMBER: _ClassVar[int]
    UINT_CONTENTS_FIELD_NUMBER: _ClassVar[int]
    bool_contents: _containers.RepeatedScalarFieldContainer[bool]
    bytes_contents: _containers.RepeatedScalarFieldContainer[bytes]
    fp32_contents: _containers.RepeatedScalarFieldContainer[float]
    fp64_contents: _containers.RepeatedScalarFieldContainer[float]
    int64_contents: _containers.RepeatedScalarFieldContainer[int]
    int_contents: _containers.RepeatedScalarFieldContainer[int]
    uint64_contents: _containers.RepeatedScalarFieldContainer[int]
    uint_contents: _containers.RepeatedScalarFieldContainer[int]
    def __init__(
        self,
        bool_contents: _Iterable[bool] | None = ...,
        int_contents: _Iterable[int] | None = ...,
        int64_contents: _Iterable[int] | None = ...,
        uint_contents: _Iterable[int] | None = ...,
        uint64_contents: _Iterable[int] | None = ...,
        fp32_contents: _Iterable[float] | None = ...,
        fp64_contents: _Iterable[float] | None = ...,
        bytes_contents: _Iterable[bytes] | None = ...,
    ) -> None: ...

class ModelInferRequest(_message.Message):
    __slots__ = [
        "id",
        "inputs",
        "model_name",
        "model_version",
        "outputs",
        "parameters",
        "raw_input_contents",
    ]

    class InferInputTensor(_message.Message):
        __slots__ = ["contents", "datatype", "name", "parameters", "shape"]

        class ParametersEntry(_message.Message):
            __slots__ = ["key", "value"]
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: InferParameter
            def __init__(
                self,
                key: str | None = ...,
                value: InferParameter | _Mapping | None = ...,
            ) -> None: ...

        CONTENTS_FIELD_NUMBER: _ClassVar[int]
        DATATYPE_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        PARAMETERS_FIELD_NUMBER: _ClassVar[int]
        SHAPE_FIELD_NUMBER: _ClassVar[int]
        contents: InferTensorContents
        datatype: str
        name: str
        parameters: _containers.MessageMap[str, InferParameter]
        shape: _containers.RepeatedScalarFieldContainer[int]
        def __init__(
            self,
            name: str | None = ...,
            datatype: str | None = ...,
            shape: _Iterable[int] | None = ...,
            parameters: _Mapping[str, InferParameter] | None = ...,
            contents: InferTensorContents | _Mapping | None = ...,
        ) -> None: ...

    class InferRequestedOutputTensor(_message.Message):
        __slots__ = ["name", "parameters"]

        class ParametersEntry(_message.Message):
            __slots__ = ["key", "value"]
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: InferParameter
            def __init__(
                self,
                key: str | None = ...,
                value: InferParameter | _Mapping | None = ...,
            ) -> None: ...

        NAME_FIELD_NUMBER: _ClassVar[int]
        PARAMETERS_FIELD_NUMBER: _ClassVar[int]
        name: str
        parameters: _containers.MessageMap[str, InferParameter]
        def __init__(
            self,
            name: str | None = ...,
            parameters: _Mapping[str, InferParameter] | None = ...,
        ) -> None: ...

    class ParametersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: InferParameter
        def __init__(
            self,
            key: str | None = ...,
            value: InferParameter | _Mapping | None = ...,
        ) -> None: ...

    ID_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    RAW_INPUT_CONTENTS_FIELD_NUMBER: _ClassVar[int]
    id: str
    inputs: _containers.RepeatedCompositeFieldContainer[
        ModelInferRequest.InferInputTensor
    ]
    model_name: str
    model_version: str
    outputs: _containers.RepeatedCompositeFieldContainer[
        ModelInferRequest.InferRequestedOutputTensor
    ]
    parameters: _containers.MessageMap[str, InferParameter]
    raw_input_contents: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(
        self,
        model_name: str | None = ...,
        model_version: str | None = ...,
        id: str | None = ...,
        parameters: _Mapping[str, InferParameter] | None = ...,
        inputs: _Iterable[ModelInferRequest.InferInputTensor | _Mapping] | None = ...,
        outputs: _Iterable[ModelInferRequest.InferRequestedOutputTensor | _Mapping]
        | None = ...,
        raw_input_contents: _Iterable[bytes] | None = ...,
    ) -> None: ...

class ModelInferResponse(_message.Message):
    __slots__ = [
        "id",
        "model_name",
        "model_version",
        "outputs",
        "parameters",
        "raw_output_contents",
    ]

    class InferOutputTensor(_message.Message):
        __slots__ = ["contents", "datatype", "name", "parameters", "shape"]

        class ParametersEntry(_message.Message):
            __slots__ = ["key", "value"]
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: InferParameter
            def __init__(
                self,
                key: str | None = ...,
                value: InferParameter | _Mapping | None = ...,
            ) -> None: ...

        CONTENTS_FIELD_NUMBER: _ClassVar[int]
        DATATYPE_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        PARAMETERS_FIELD_NUMBER: _ClassVar[int]
        SHAPE_FIELD_NUMBER: _ClassVar[int]
        contents: InferTensorContents
        datatype: str
        name: str
        parameters: _containers.MessageMap[str, InferParameter]
        shape: _containers.RepeatedScalarFieldContainer[int]
        def __init__(
            self,
            name: str | None = ...,
            datatype: str | None = ...,
            shape: _Iterable[int] | None = ...,
            parameters: _Mapping[str, InferParameter] | None = ...,
            contents: InferTensorContents | _Mapping | None = ...,
        ) -> None: ...

    class ParametersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: InferParameter
        def __init__(
            self,
            key: str | None = ...,
            value: InferParameter | _Mapping | None = ...,
        ) -> None: ...

    ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    RAW_OUTPUT_CONTENTS_FIELD_NUMBER: _ClassVar[int]
    id: str
    model_name: str
    model_version: str
    outputs: _containers.RepeatedCompositeFieldContainer[
        ModelInferResponse.InferOutputTensor
    ]
    parameters: _containers.MessageMap[str, InferParameter]
    raw_output_contents: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(
        self,
        model_name: str | None = ...,
        model_version: str | None = ...,
        id: str | None = ...,
        parameters: _Mapping[str, InferParameter] | None = ...,
        outputs: _Iterable[ModelInferResponse.InferOutputTensor | _Mapping]
        | None = ...,
        raw_output_contents: _Iterable[bytes] | None = ...,
    ) -> None: ...

class ModelMetadataRequest(_message.Message):
    __slots__ = ["name", "version"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: str
    def __init__(self, name: str | None = ..., version: str | None = ...) -> None: ...

class ModelMetadataResponse(_message.Message):
    __slots__ = ["inputs", "name", "outputs", "platform", "versions"]

    class TensorMetadata(_message.Message):
        __slots__ = ["datatype", "name", "shape"]
        DATATYPE_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        SHAPE_FIELD_NUMBER: _ClassVar[int]
        datatype: str
        name: str
        shape: _containers.RepeatedScalarFieldContainer[int]
        def __init__(
            self,
            name: str | None = ...,
            datatype: str | None = ...,
            shape: _Iterable[int] | None = ...,
        ) -> None: ...

    INPUTS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    PLATFORM_FIELD_NUMBER: _ClassVar[int]
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    inputs: _containers.RepeatedCompositeFieldContainer[
        ModelMetadataResponse.TensorMetadata
    ]
    name: str
    outputs: _containers.RepeatedCompositeFieldContainer[
        ModelMetadataResponse.TensorMetadata
    ]
    platform: str
    versions: _containers.RepeatedScalarFieldContainer[str]
    def __init__(
        self,
        name: str | None = ...,
        versions: _Iterable[str] | None = ...,
        platform: str | None = ...,
        inputs: _Iterable[ModelMetadataResponse.TensorMetadata | _Mapping] | None = ...,
        outputs: _Iterable[ModelMetadataResponse.TensorMetadata | _Mapping]
        | None = ...,
    ) -> None: ...

class ModelReadyRequest(_message.Message):
    __slots__ = ["name", "version"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: str
    def __init__(self, name: str | None = ..., version: str | None = ...) -> None: ...

class ModelReadyResponse(_message.Message):
    __slots__ = ["ready"]
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: bool = ...) -> None: ...

class RepositoryModelLoadRequest(_message.Message):
    __slots__ = ["model_name"]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    def __init__(self, model_name: str | None = ...) -> None: ...

class RepositoryModelLoadResponse(_message.Message):
    __slots__ = ["isLoaded", "model_name"]
    ISLOADED_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    isLoaded: bool
    model_name: str
    def __init__(self, model_name: str | None = ..., isLoaded: bool = ...) -> None: ...

class RepositoryModelUnloadRequest(_message.Message):
    __slots__ = ["model_name"]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    def __init__(self, model_name: str | None = ...) -> None: ...

class RepositoryModelUnloadResponse(_message.Message):
    __slots__ = ["isUnloaded", "model_name"]
    ISUNLOADED_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    isUnloaded: bool
    model_name: str
    def __init__(
        self, model_name: str | None = ..., isUnloaded: bool = ...
    ) -> None: ...

class ServerLiveRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ServerLiveResponse(_message.Message):
    __slots__ = ["live"]
    LIVE_FIELD_NUMBER: _ClassVar[int]
    live: bool
    def __init__(self, live: bool = ...) -> None: ...

class ServerMetadataRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ServerMetadataResponse(_message.Message):
    __slots__ = ["extensions", "name", "version"]
    EXTENSIONS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    extensions: _containers.RepeatedScalarFieldContainer[str]
    name: str
    version: str
    def __init__(
        self,
        name: str | None = ...,
        version: str | None = ...,
        extensions: _Iterable[str] | None = ...,
    ) -> None: ...

class ServerReadyRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ServerReadyResponse(_message.Message):
    __slots__ = ["ready"]
    READY_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    def __init__(self, ready: bool = ...) -> None: ...
