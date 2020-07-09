class HarperDBError(Exception):

    """ Raised when the server returns an error (500), or a hash is not found.

    This is the only Exception raised explicitly.
    """
