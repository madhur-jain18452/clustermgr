class ActionAlreadyPerformedError(Exception):
    """Signifies that an action has already been performed
    Or that a resource has already undergone the expected change"""
    def __init__(self, message):
        super().__init__(message)
        self.message = message

class InconsistentCacheError(Exception):
    """Indicates that the cache is inconsistent.
    """
    def __init__(self, message):
        super().__init__(message)
        self.message = message

class SameTimestampError(Exception):
    """Indicates that the timed difference does not contain any different timestamps. 
    """
    def __init__(self, message):
        super().__init__(message)
        self.message = message

