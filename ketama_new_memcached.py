import memcache
from consistent_hash import HashRing


class MemcacheClient(memcache.Client):
    """ A memcache subclass. It currently allows you to add a new host at run
    time.
    """
    available_algorithms = ['ketama', 'modulo']
    hash_algorithm_index = 0

    def __init__(self, hash_algorithm='ketama', *args, **kwargs):
        super(MemcacheClient, self).__init__(*args, **kwargs)

        if hash_algorithm in self.available_algorithms:
            self.hash_algorithm_index = self.available_algorithms.index(
                hash_algorithm)

            if hash_algorithm == 'ketama':
                self.consistent_hash_manager = HashRing(nodes=self.servers)
            else:
                self.consistent_hash_manager = None
        else:
            raise Exception(
                "The algorithm \"%s\" is not implemented for this client. The "
                "options are \"%s\""
                "" % (hash_algorithm, " or ".join(self.available_algorithms))
            )

    def _get_server(self, key):
        """ Returns the most likely server to hold the key
        """
        if self.hash_algorithm == 'ketama':
            servers_generator = self.consistent_hash_manager.get_nodes(key)
            for server in servers_generator:
                if server.connect():
                    return server, key
            return None, None

        else:
            return super(MemcacheClient, self)._get_server(key)

    def add_server(self, server):
        """ Adds a host at runtime to client
        """
        # when no reliable (modulo) consistent hash algorithm
        if not self.consistent_hash_manager:
            raise Exception("The current consistent hash algorithm (\"%s\") is"
                            " not reliable for adding a new server"
                            "" % self.hash_algorithm)

        # Create a new host entry
        server = memcache._Host(
            server, self.debug, dead_retry=self.dead_retry,
            socket_timeout=self.socket_timeout,
            flush_on_reconnect=self.flush_on_reconnect
        )
        # Add this to our server choices
        self.servers.append(server)

        for _i in range(server.weight):
            self.buckets.append(server)

        # Adds this node to the circle
        if self.consistent_hash_manager:
            self.consistent_hash_manager.add_node(server)
