from __future__ import annotations

import ipaddress
import math
import os
import socket
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from urllib.parse import urlsplit


_DEFAULT_OPERATION_SKILLS = {
    "assess": "inventory-assessment",
    "inbound_lookup": "inbound-lookup",
}


def _validate_loopback_host(host: str, *, name: str) -> None:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if host.lower() != "localhost":
            raise ValueError(f"{name} must use a loopback address")
        try:
            resolved = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        except OSError as exc:
            raise ValueError(f"{name} must resolve exclusively to loopback addresses") from exc
        addresses = {
            ipaddress.ip_address(result[4][0])
            for result in resolved
        }
        if not addresses or any(not address.is_loopback for address in addresses):
            raise ValueError(f"{name} must resolve exclusively to loopback addresses")
    else:
        if not address.is_loopback:
            raise ValueError(f"{name} must use a loopback address")


def _origin(url: str, *, name: str) -> tuple[str, str, int]:
    if url != url.strip() or any(ord(character) < 32 for character in url):
        raise ValueError(f"{name} must be a valid absolute HTTP(S) URL")
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid absolute HTTP(S) URL") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{name} must be a valid absolute HTTP(S) URL")
    _validate_loopback_host(parsed.hostname, name=name)
    if port is not None and not 1 <= port <= 65535:
        raise ValueError(f"{name} must use a valid TCP port")
    if name == "base_url" and parsed.path not in {"", "/"}:
        raise ValueError("base_url must identify an origin without a path")
    return (
        parsed.scheme.lower(),
        parsed.hostname.lower(),
        port if port is not None else (443 if parsed.scheme.lower() == "https" else 80),
    )


@dataclass(frozen=True)
class SpecialistSpec:
    specialist_id: str
    base_url: str
    operation_skills: Mapping[str, str]

    def __post_init__(self) -> None:
        if not self.specialist_id.strip():
            raise ValueError("specialist_id must not be empty")
        _origin(self.base_url, name="base_url")
        skills = dict(self.operation_skills)
        if not skills or any(
            not operation or not skill for operation, skill in skills.items()
        ):
            raise ValueError("operation_skills must map operations to skill IDs")
        object.__setattr__(self, "operation_skills", MappingProxyType(skills))

    def skill_for(self, operation: str) -> str:
        try:
            return self.operation_skills[operation]
        except KeyError as exc:
            raise ValueError(
                f"Specialist {self.specialist_id!r} does not support operation "
                f"{operation!r}"
            ) from exc

    @property
    def origin(self) -> tuple[str, str, int]:
        return _origin(self.base_url, name="base_url")


class SpecialistRegistry:
    def __init__(self, specialists: tuple[SpecialistSpec, ...]) -> None:
        entries = {specialist.specialist_id: specialist for specialist in specialists}
        if len(entries) != len(specialists):
            raise ValueError("specialist IDs must be unique")
        self._specialists = MappingProxyType(entries)

    @classmethod
    def with_inventory(
        cls, base_url: str = "http://127.0.0.1:8001"
    ) -> SpecialistRegistry:
        return cls(
            (
                SpecialistSpec(
                    specialist_id="inventory",
                    base_url=base_url,
                    operation_skills=_DEFAULT_OPERATION_SKILLS,
                ),
            )
        )

    @classmethod
    def from_environment(cls) -> SpecialistRegistry:
        return cls.with_inventory(
            os.environ.get("INVENTORY_A2A_URL", "http://127.0.0.1:8001")
        )

    def get(self, specialist_id: str) -> SpecialistSpec:
        try:
            return self._specialists[specialist_id]
        except KeyError as exc:
            raise KeyError(f"Unknown specialist: {specialist_id}") from exc

    def __iter__(self) -> Iterator[SpecialistSpec]:
        return iter(self._specialists.values())


def positive_seconds(value: str, name: str) -> float:
    try:
        seconds = float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError(f"{name} must be positive")
    return seconds
