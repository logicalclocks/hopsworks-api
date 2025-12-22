"""Custom exceptions for CLI"""


class CLIException(Exception):
    """Base exception for CLI errors"""
    pass


class AuthenticationError(CLIException):
    """Authentication failed"""
    pass


class ResourceNotFoundError(CLIException):
    """Resource not found"""
    pass


class ConfigurationError(CLIException):
    """Invalid configuration"""
    pass


class ValidationError(CLIException):
    """Input validation error"""
    pass
