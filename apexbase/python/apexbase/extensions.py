"""Extension column codec registry (V5 S4, design D2).

An ``extension`` column stores codec-encoded object payloads. The storage
layer is codec-agnostic: it keeps the encoded bytes in a binary column and
records the codec name/version in the logical-type schema section. Encoding
and decoding happen in Python through the functions registered here.

Only ``bytes`` and ``json`` are built in. ``pickle`` is **not** registered by
default: a pickle payload can execute arbitrary code on read, so enabling it
requires an explicit ``register_extension_codec("pickle", pickle.dumps,
pickle.loads)`` by the application, which then owns that security decision.
"""

from __future__ import annotations

import json
from typing import Callable, Dict, List, Tuple


class ExtensionCodecError(ValueError):
    """Raised for unregistered codecs, bad payloads and version mismatches."""


# name -> (version, encode, decode)
_CODECS: Dict[str, Tuple[int, Callable[[object], bytes], Callable[[bytes], object]]] = {}


def _normalize_payload(payload) -> bytes:
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, bytearray):
        return bytes(payload)
    if isinstance(payload, memoryview):
        return payload.tobytes()
    raise ExtensionCodecError(
        f"extension encoder must return bytes, got {type(payload).__name__}"
    )


def _bytes_encode(value) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    raise ExtensionCodecError("the 'bytes' codec only accepts bytes-like values")


def _bytes_decode(payload: bytes) -> bytes:
    return bytes(payload)


def _json_encode(value) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ExtensionCodecError(f"json codec cannot encode value: {error}") from error


def _json_decode(payload: bytes):
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise ExtensionCodecError(f"json codec cannot decode payload: {error}") from error


def register_extension_codec(
    name: str,
    encode: Callable[[object], bytes],
    decode: Callable[[bytes], object],
    version: int = 1,
) -> None:
    """Register (or replace) an extension codec.

    Args:
        name: Codec name used in the declared type ``extension<name>``.
        encode: Callable converting a Python object to bytes.
        decode: Callable converting the stored bytes back to a Python object.
        version: Codec version recorded in the column's logical type. Reading a
            column whose recorded version differs from the registered codec
            raises :class:`ExtensionCodecError`.

    Raises:
        ExtensionCodecError: If the name/version/arguments are invalid.
    """
    if not isinstance(name, str) or not name:
        raise ExtensionCodecError("codec name must be a non-empty string")
    if not all(ch.isalnum() or ch in "_-" for ch in name):
        raise ExtensionCodecError(
            "codec name may only contain letters, digits, '_' and '-'"
        )
    if not callable(encode) or not callable(decode):
        raise ExtensionCodecError("encode and decode must be callables")
    if not isinstance(version, int) or version < 1 or version > 255:
        raise ExtensionCodecError("codec version must be an integer in 1..255")
    if isinstance(name, str) and name.lower() in ("",):
        raise ExtensionCodecError("codec name must be non-empty")
    _CODECS[name.lower()] = (version, encode, decode)


def unregister_extension_codec(name: str) -> None:
    """Remove a codec registration (built-ins can be removed too)."""
    _CODECS.pop(str(name).lower(), None)


def list_extension_codecs() -> List[str]:
    """Return the registered codec names (sorted)."""
    return sorted(_CODECS)


def get_extension_codec(name: str):
    """Return ``(version, encode, decode)`` for *name*.

    Raises:
        ExtensionCodecError: If the codec is not registered.
    """
    entry = _CODECS.get(str(name).lower())
    if entry is None:
        raise ExtensionCodecError(
            f"extension codec '{name}' is not registered; call "
            f"register_extension_codec('{name}', encode, decode) first"
        )
    return entry


def encode_extension_value(name: str, value) -> bytes:
    """Encode *value* with the registered codec *name*."""
    _, encode, _ = get_extension_codec(name)
    try:
        payload = encode(value)
    except ExtensionCodecError:
        raise
    except Exception as error:
        raise ExtensionCodecError(
            f"extension codec '{name}' failed to encode value: {error}"
        ) from error
    return _normalize_payload(payload)


def decode_extension_value(name: str, payload: bytes, version: int = 1):
    """Decode *payload* with the registered codec *name*.

    Args:
        name: Codec name recorded in the column's logical type.
        payload: Stored bytes.
        version: Version recorded in the column's logical type. A mismatch with
            the registered codec version raises :class:`ExtensionCodecError`.

    Raises:
        ExtensionCodecError: Unknown codec, version mismatch or corrupt payload.
    """
    registered_version, _, decode = get_extension_codec(name)
    if version != registered_version:
        raise ExtensionCodecError(
            f"extension codec '{name}' version mismatch: column stores v{version}, "
            f"registered codec is v{registered_version}"
        )
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise ExtensionCodecError(
            f"extension payload must be bytes, got {type(payload).__name__}"
        )
    try:
        return decode(bytes(payload))
    except ExtensionCodecError:
        raise
    except Exception as error:
        raise ExtensionCodecError(
            f"extension codec '{name}' failed to decode payload: {error}"
        ) from error


def parse_extension_declared(declared: str) -> Tuple[str, int]:
    """Parse ``extension<codec>`` / ``extension<codec@version>`` declarations."""
    text = str(declared).strip().lower()
    if not text.startswith("extension<") or not text.endswith(">"):
        raise ExtensionCodecError(f"malformed extension type '{declared}'")
    inner = text[len("extension<"):-1].strip()
    name, _, version_text = inner.partition("@")
    name = name.strip()
    version = 1
    if version_text:
        try:
            version = int(version_text.strip())
        except ValueError as error:
            raise ExtensionCodecError(
                f"invalid extension codec version in '{declared}'"
            ) from error
    if not name:
        raise ExtensionCodecError(f"malformed extension type '{declared}'")
    return name, version


def _install_builtins() -> None:
    register_extension_codec("bytes", _bytes_encode, _bytes_decode)
    register_extension_codec("json", _json_encode, _json_decode)


_install_builtins()
