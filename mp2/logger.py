import logging

logging.basicConfig(filename = 'vm.log', format='%(asctime)s:%(levelname)s:%(message)s', datefmt = "%Y-%m-%d %H:%M:%S", level = logging.INFO)

def log_join(node):
    logging.info(' %s joined membership' % node)

def log_leave(node):
    logging.info(' %s left membership' % node)

def log_suspect(node):
    logging.warning(' %s suspected failure' % node)

def log_fail(node):
    logging.warning(' %s failed' % node)

if __name__ == '__main__':
    pass