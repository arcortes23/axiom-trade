"""Declarative strategy definitions and deterministic built-in signals."""
from .dsl import (
    ALLOWED_OPERATIONS,
    CRYPTO_FAMILIES,
    FAMILIES,
    PREDICTION_FAMILIES,
    StrategyDefinition,
    StrategyDSL,
    StrategySchemaError,
    StrategySpec,
    StrategyValidationError,
    load_strategy,
    parse_strategy,
    validate_strategy,
)
from .signals import (
    BuiltinSignalEvaluator,
    Signal,
    SignalEvaluator,
    evaluate_crypto_family,
    evaluate_prediction_family,
    evaluate_signal,
    evaluate_signal_record,
)

__all__ = [
    "ALLOWED_OPERATIONS", "BuiltinSignalEvaluator", "CRYPTO_FAMILIES", "FAMILIES",
    "PREDICTION_FAMILIES", "Signal", "SignalEvaluator", "StrategyDefinition", "StrategyDSL",
    "StrategySchemaError", "StrategySpec", "StrategyValidationError", "evaluate_crypto_family",
    "evaluate_prediction_family", "evaluate_signal", "evaluate_signal_record", "load_strategy",
    "parse_strategy", "validate_strategy",
]
