import traceback


def single_request(method, args=None, kwargs=None, id=None, is_notification=False, reply_to=None, **options):
    sr = {"method": method}

    args = None if args is None or len(args) == 0 else args
    kwargs = None if kwargs is None or len(kwargs) == 0 else kwargs

    sr["is_notification"] = is_notification

    # Si es una notificación no incluimos id si es None

    if id is None and not is_notification:
        raise TypeError("id must be not null if not a notification")

    if id is not None:
        sr["id"] = id

    if reply_to is not None:
        sr["reply_to"] = reply_to

    if args is not None and args != []:
        sr["args"] = args[:]

    if kwargs is not None and kwargs != {}:
        sr["kwargs"] = kwargs

    if "args" in sr and "kwargs" in sr:
        raise TypeError("Only allowed either positional or named parameters")

    if len(options) > 0:
        sr["options"] = options

    return sr


def is_single_request(request):
    return isinstance(request, dict) and "method" in request


def is_batch_request(request):
    return isinstance(request, list) and len(request) > 0 and is_single_request(request[0])


def error_response(code, id=None, reply_to=None, is_notification=False, with_trace=False, message="Undefined error"):

    error_codes = {
        -32700: "Parse error.",
        -32600: "Invalid Request.",
        -32601: "The method does not exist/is not available.",
        -32602: "Invalid method parameters.",
        -32603: "Internal RPC error."}

    er = {
        "id": id,
        "is_notification": is_notification,
        "error": {"code": code, "message": error_codes.get(code, message)}}

    if reply_to is not None:
        er["reply_to"] = reply_to

    if with_trace:
        er["error"]["trace"] = traceback.format_exc()

    return er


def result_response(result=None, id=None, reply_to=None, is_notification=False):

    rr = {"result": result, "is_notification": is_notification}

    if id is not None:
        rr["id"] = id

    if reply_to is not None:
        rr["reply_to"] = reply_to

    return rr


def is_single_response(response):
    return isinstance(response, dict) and ("result" in response or "error" in response)


def is_multiple_response(response):
    return isinstance(response, list) and len(response) > 0 and is_single_response(response[0])
