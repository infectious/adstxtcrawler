"""AdsTxt Exceptions."""


class AdsTxtBase(Exception):
    """Base AdsTxtException"""


class ConfigurationError(AdsTxtBase):
    """AdsTxt Configuration found to be incorrect."""
