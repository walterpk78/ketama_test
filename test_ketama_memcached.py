import random
import string
import logging
from ketama_new_memcached import MemcacheClient


path_logs = ""
LOG_LEVEL = logging.DEBUG


def random_key(size):
    """ Generates a random key
    """
    return ''.join(random.choice(string.letters) for _ in range(size))


def run_consistent_hash_test(client_obj):
    # We have 500 keys to split across our servers
    keys = [random_key(100) for _i in range(500)]

    log.info("KETAMA HASH ALGORITHM \"%s\" /""" % client_obj.hash_algorithm.upper())

    log.info("\n->These are the %s servers:" % len(client_obj.servers))
    str_servers = ""
    for server in client_obj.servers:
        str_servers += "%s:%s, " % (server.address[0], server.address[1])
    log.info(str_servers)

    # Clear all previous keys from memcache
    client_obj.flush_all()

    # Distribute the keys over the servers
    for key in keys:
        client_obj.set(key, 1)

    log.info("\n%d keys distributed for %d server(s)\n""" % (len(keys), len(client_obj.servers)))

    # Check how many keys come back
    valid_keys = client_obj.get_multi(keys)
    log.info(
        "%s percent of keys matched, before adding extra servers.\n" "" % ((len(valid_keys) / float(len(keys))) * 100)
    )
    # Add 5 new extra servers
    interval_extra_servers = range(19, 24)
    extra_servers = ['127.0.0.1:112%d' % _x for _x in interval_extra_servers]
    for server in extra_servers:
        client_obj.add_server(server)

    # Check how many keys come back after adding the extra servers
    valid_keys = client_obj.get_multi(keys)
    log.info(
        "Added %d new server(s).\n%s percent of keys still matched"
        "" % (len(interval_extra_servers), (len(valid_keys) / float(len(keys))) * 100)
    )


def create_logger(name):
    global loggers
    if loggers.get(name):
        return loggers.get(name)
    else:
        logger = logging.getLogger(name)
        logger.setLevel(LOG_LEVEL)
        handler = logging.FileHandler("".join([path_logs, 'memcache_ketama.log']))
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        loggers.update(dict(name=logger))
        return logger


if __name__ == '__main__':
    # 8 running memcached servers

    global log
    loggers = {}
    log = create_logger("[@main]")
    interval_servers = range(11, 19)
    servers = ['127.0.0.1:112%d' % _j for _j in interval_servers]
    """
    Init our subclass. The hash_algorithm paramether can be "modulo"<-
    (default) or "ketama" (the new one).
    """
    client = MemcacheClient(servers=servers, hash_algorithm='ketama')
    run_consistent_hash_test(client)
