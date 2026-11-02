from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from services.inventory.models import (
    InboundInventory,
    InventoryAssessment,
    InventoryErrorDetail,
    InventoryIssue,
    InventoryPolicy,
    InventoryPosition,
    TransferCandidate,
)
from services.inventory.repository import (
    DatabaseConfigurationError,
    DatabaseQueryError,
    InventoryReadRepository,
    InventoryRepository,
    PostgresInventoryRepository,
    RepositoryError,
)


class InventoryServiceError(Exception):
    def __init__(self, detail: InventoryErrorDetail) -> None:
        super().__init__(detail.message)
        self.detail = detail


class UnknownSkuError(InventoryServiceError):
    pass


class UnknownLocationError(InventoryServiceError):
    pass


class InventoryPositionNotFoundError(InventoryServiceError):
    pass


class InventoryPolicyNotFoundError(InventoryServiceError):
    pass


class InventoryDatabaseError(InventoryServiceError):
    pass


class InventoryService:
    def __init__(self, repository: InventoryRepository) -> None:
        self._repository = repository

    @classmethod
    def from_environment(cls) -> InventoryService:
        try:
            repository = PostgresInventoryRepository.from_environment()
        except DatabaseConfigurationError as exc:
            raise InventoryServiceError(
                InventoryErrorDetail(
                    code="DATABASE_CONFIGURATION_ERROR", message=str(exc)
                )
            ) from exc
        return cls(repository)

    def get_inventory_position(self, sku: str, location_id: str) -> InventoryPosition:
        with self._snapshot() as repository:
            self._validate_scope(repository, sku, location_id)
            position = self._require_position(repository, sku, location_id)
            return self._with_available_quantity(position)

    def get_inbound_inventory(
        self, sku: str, location_id: str
    ) -> tuple[InboundInventory, ...]:
        with self._snapshot() as repository:
            self._validate_scope(repository, sku, location_id)
            return repository.get_inbound_inventory(sku, location_id)

    def get_inventory_policy(self, sku: str, location_id: str) -> InventoryPolicy:
        with self._snapshot() as repository:
            self._validate_scope(repository, sku, location_id)
            policy = repository.get_policy(sku, location_id)
            if policy is None:
                raise InventoryPolicyNotFoundError(
                    InventoryErrorDetail(
                        code="INVENTORY_POLICY_NOT_FOUND",
                        message=f"No inventory policy exists for {sku} at {location_id}",
                        sku=sku,
                        location_id=location_id,
                    )
                )
            return policy

    def assess_inventory(self, sku: str, location_id: str) -> InventoryAssessment:
        with self._snapshot() as repository:
            self._validate_scope(repository, sku, location_id)
            position = self._with_available_quantity(
                self._require_position(repository, sku, location_id)
            )
            policy = repository.get_policy(sku, location_id)
            inbound = repository.get_inbound_inventory(sku, location_id)
            issues = list(position.issues)

            if policy is None:
                issues.append(
                    InventoryIssue(
                        code="INVENTORY_POLICY_NOT_FOUND",
                        message=f"No inventory policy exists for {sku} at {location_id}",
                    )
                )

            if position.available_quantity is None:
                replenishment_triggered = None
                recommended_quantity = None
            else:
                replenishment_triggered = (
                    position.available_quantity < position.reorder_point
                )
                if not replenishment_triggered:
                    recommended_quantity = 0
                elif policy is None:
                    recommended_quantity = None
                else:
                    recommended_quantity = max(
                        0, policy.target_stock - position.available_quantity
                    )

            return InventoryAssessment(
                sku=sku,
                location_id=location_id,
                position=position,
                policy=policy,
                inbound_inventory=inbound,
                replenishment_triggered=replenishment_triggered,
                recommended_quantity=recommended_quantity,
                issues=tuple(issues),
            )

    def get_transfer_candidates(
        self, sku: str, destination_id: str
    ) -> tuple[TransferCandidate, ...]:
        with self._snapshot() as repository:
            self._validate_scope(repository, sku, destination_id)
            candidates: list[TransferCandidate] = []
            for rule in repository.get_transfer_rules(destination_id):
                donor_position = repository.get_position(sku, rule.origin_id)
                candidate_issues: list[InventoryIssue] = []
                if donor_position is None:
                    candidate_issues.append(
                        InventoryIssue(
                            code="DONOR_INVENTORY_POSITION_NOT_FOUND",
                            message=(
                                f"No donor position exists for {sku} at "
                                f"{rule.origin_id}"
                            ),
                        )
                    )
                else:
                    donor_position = self._with_available_quantity(donor_position)
                    candidate_issues.extend(donor_position.issues)

                candidates.append(
                    TransferCandidate(
                        origin_id=rule.origin_id,
                        destination_id=rule.destination_id,
                        origin_location_type=rule.origin_location_type,
                        eligible=rule.eligible,
                        transit_days=rule.transit_days,
                        donor_position=donor_position,
                        issues=tuple(candidate_issues),
                    )
                )
            return tuple(candidates)

    @contextmanager
    def _snapshot(self) -> Iterator[InventoryReadRepository]:
        try:
            with self._repository.read_snapshot() as repository:
                yield repository
        except (DatabaseConfigurationError, DatabaseQueryError) as exc:
            code = (
                "DATABASE_CONFIGURATION_ERROR"
                if isinstance(exc, DatabaseConfigurationError)
                else "DATABASE_ERROR"
            )
            raise InventoryDatabaseError(
                InventoryErrorDetail(code=code, message=str(exc))
            ) from exc
        except RepositoryError as exc:
            raise InventoryDatabaseError(
                InventoryErrorDetail(code="DATABASE_ERROR", message=str(exc))
            ) from exc

    @staticmethod
    def _validate_scope(
        repository: InventoryReadRepository, sku: str, location_id: str
    ) -> None:
        if not repository.product_exists(sku):
            raise UnknownSkuError(
                InventoryErrorDetail(
                    code="UNKNOWN_SKU",
                    message=f"Unknown SKU: {sku}",
                    sku=sku,
                    location_id=location_id,
                )
            )
        if not repository.location_exists(location_id):
            raise UnknownLocationError(
                InventoryErrorDetail(
                    code="UNKNOWN_LOCATION",
                    message=f"Unknown location: {location_id}",
                    sku=sku,
                    location_id=location_id,
                )
            )

    @staticmethod
    def _require_position(
        repository: InventoryReadRepository, sku: str, location_id: str
    ) -> InventoryPosition:
        position = repository.get_position(sku, location_id)
        if position is None:
            raise InventoryPositionNotFoundError(
                InventoryErrorDetail(
                    code="INVENTORY_POSITION_NOT_FOUND",
                    message=f"No inventory position exists for {sku} at {location_id}",
                    sku=sku,
                    location_id=location_id,
                )
            )
        return position

    @staticmethod
    def _with_available_quantity(position: InventoryPosition) -> InventoryPosition:
        if position.reserved > position.on_hand:
            issue = InventoryIssue(
                code="INCONSISTENT_INVENTORY_POSITION",
                message=(
                    f"Reserved quantity ({position.reserved}) exceeds on-hand "
                    f"quantity ({position.on_hand})"
                ),
            )
            return position.model_copy(
                update={"available_quantity": None, "issues": (issue,)}
            )
        return position.model_copy(
            update={
                "available_quantity": position.on_hand - position.reserved,
                "issues": (),
            }
        )
