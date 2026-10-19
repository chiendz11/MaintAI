"""Typed failures which callers can map to HTTP, retries, or process failure."""


class MessagingError(Exception):
    pass


class ConfigurationError(MessagingError):
    pass


class ContractViolation(MessagingError):
    pass


class MalformedJSON(ContractViolation):
    pass


class PublishUnavailable(MessagingError):
    """No delivery acknowledgement; callers must retain the original event identity."""


class PublishUncertain(PublishUnavailable):
    """The broker might have accepted the record; retry its original ID and payload."""


class IdentityConflict(MessagingError):
    """An immutable event or archive identity was reused with different content."""


class ConsumerFailed(MessagingError):
    """No offset is advanced past the failed record."""


class LeaseLost(MessagingError):
    pass
