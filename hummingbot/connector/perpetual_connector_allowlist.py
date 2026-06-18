# Dated-futures connectors that must be treated as perpetual by the executor
# framework (PositionAction / position-side handling) despite not containing
# "perpetual" in their name. See JEP-167.
PERPETUAL_CONNECTOR_ALLOWLIST = frozenset({"kis_futures"})
