# Forking

Do not use a connection across `fork()`. A forked child that uses an inherited connection gets `OperationalError`.

A pool resets itself in the child. On first use it discards the parent's connections without closing them, which would otherwise end the parent's sessions, and then opens its own.
