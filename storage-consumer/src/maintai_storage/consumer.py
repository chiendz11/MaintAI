"""Observe SDK consumer lifecycle without duplicating its receipt/commit algorithm."""

from maintai_messaging.consumer import Record


class ObservedConsumer:
    def __init__(self, client, role, state):
        self.client, self.role, self.state = client, role, state
        self.closed = False

    def __getattr__(self, name):
        return getattr(self.client, name)

    def poll(self, timeout):
        self.state.tick(self.role)
        message = self.client.poll(timeout)
        if message is not None and not message.error():
            owned = {(p.topic, p.partition) for p in self.client.assignment()}
            if (message.topic(), message.partition()) in owned:
                self.state.begin(self.role, Record.from_message(message))
        return message

    def subscribe(self, topics, **callbacks):
        def revoked(callback):
            def invoke(consumer, partitions):
                self.state.revoked(self.role)
                return callback(consumer, partitions)

            return invoke

        for name in ("on_assign", "on_revoke", "on_lost"):
            callbacks[name] = revoked(callbacks[name])
        return self.client.subscribe(topics, **callbacks)

    def commit(self, *, offsets, asynchronous):
        positions = self.client.commit(offsets=offsets, asynchronous=asynchronous)
        if not any(getattr(p, "error", None) for p in positions or ()):
            self.state.committed(self.role, offsets)
        return positions

    def close(self):
        if not self.closed:
            self.closed = True
            self.client.close()
            self.state.revoked(self.role)
