class KeywordParameters:
    """Wrapper representing ``**kwargs`` to a callable.

    The actual ``kwargs`` can be obtained by calling either ``unpacking()`` or
    ``serializing()``. They behave almost the same and are only different if
    the containing ``kwargs`` is an Airflow Context object, and the calling
    function uses ``**kwargs`` in the argument list.

    In this particular case, ``unpacking()`` uses ``lazy-object-proxy`` to
    prevent the Context from emitting deprecation warnings too eagerly when it's
    unpacked by ``**``. ``serializing()`` does not do this, and will allow the
    warnings to be emitted eagerly, which is useful when you want to dump the
    content and use it somewhere else without needing ``lazy-object-proxy``.
    """

    def __init__(self, kwargs: Mapping[str, Any], *, wildcard: bool) -> None:
        self._kwargs = kwargs
        self._wildcard = wildcard

    @classmethod
    def determine(
        cls,
        func: Callable[..., Any],
        args: Collection[Any],
        kwargs: Mapping[str, Any],
    ) -> "KeywordParameters":
        import inspect
        import itertools

        signature = inspect.signature(func)
        has_wildcard_kwargs = any(p.kind == p.VAR_KEYWORD for p in signature.parameters.values())

        for name in itertools.islice(signature.parameters.keys(), len(args)):
            # Check if args conflict with names in kwargs.
            if name in kwargs:
                raise ValueError(f"The key {name!r} in args is a part of kwargs and therefore reserved.")

        if has_wildcard_kwargs:
            # If the callable has a **kwargs argument, it's ready to accept all the kwargs.
            return cls(kwargs, wildcard=True)

        # If the callable has no **kwargs argument, it only wants the arguments it requested.
        kwargs = {key: kwargs[key] for key in signature.parameters if key in kwargs}
        return cls(kwargs, wildcard=False)

    def unpacking(self) -> Mapping[str, Any]:
        """Dump the kwargs mapping to unpack with ``**`` in a function call."""
        return self._kwargs

    def serializing(self) -> Mapping[str, Any]:
        """Dump the kwargs mapping for serialization purposes."""
        return self._kwargs


def determine_kwargs(
    func: Callable[..., Any],
    args: Collection[Any],
    kwargs: Mapping[str, Any],
) -> Mapping[str, Any]:
    """
    Inspect the signature of a given callable to determine which arguments in kwargs need
    to be passed to the callable.

    :param func: The callable that you want to invoke
    :param args: The positional arguments that needs to be passed to the callable, so we
        know how many to skip.
    :param kwargs: The keyword arguments that need to be filtered before passing to the callable.
    :return: A dictionary which contains the keyword arguments that are compatible with the callable.
    """
    return KeywordParameters.determine(func, args, kwargs).unpacking()