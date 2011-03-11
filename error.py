#!/usr/bin/env python 

class LastfmError(Exception):
    def __init__(self, message="", code=0):
        super(LastfmError, self).__init__()
        self.code = code
        self.message = message

    def __str__(self):
        return "%s: %s" %(self.code, self.message)

class InvalidServiceError(LastfmError):             pass    #2
class InvalidMethodError(LastfmError):              pass    #3
class AuthenticationFailedError(LastfmError):       pass    #4
class InvalidFormatError(LastfmError):              pass    #5
class InvalidParametersError(LastfmError):          pass    #6
class InvalidResourceError(LastfmError):            pass    #7
class OperationFailedError(LastfmError):            pass    #8
class InvalidSessionKeyError(LastfmError):          pass    #9
class InvalidApiKeyError(LastfmError):              pass    #10
class ServiceOfflineError(LastfmError):             pass    #11
class SubscribersOnlyError(LastfmError):            pass    #12
class InvalidMethodSignatureError(LastfmError):     pass    #13
class TokenNotAuthorizedError(LastfmError):         pass    #14
class TokenExpiredError(LastfmError):               pass    #15
class SubscriptionRequiredError(LastfmError):       pass    #18

error_map = {
            1: LastfmError,
            2: InvalidServiceError,
            3: InvalidMethodError,
            4: AuthenticationFailedError,
            5: InvalidFormatError,
            6: InvalidParametersError,
            7: InvalidResourceError,
            8: OperationFailedError,
            9: InvalidSessionKeyError,
            10: InvalidApiKeyError,
            11: ServiceOfflineError,
            12: SubscribersOnlyError,
            13: InvalidMethodSignatureError,
            14: TokenNotAuthorizedError,
            15: TokenExpiredError,
            18: SubscriptionRequiredError
}

