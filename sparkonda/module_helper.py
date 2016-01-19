# -*- coding: utf-8 -*-

SPARKONDA_UTILS_FILENAME = 'sparkonda_utils.py'


def __get_local_module_file_location():
    """Discover the location of the sparkonda module
    """
    import os.path
    module_file = '/'.join([os.path.abspath(os.path.dirname(__file__)), SPARKONDA_UTILS_FILENAME])
    return module_file


def add_module_to_workers(sc, debug=False):
    """ Add the sparkonda module it to the workers and return the filename
    """
    import sys
    import os
    module_path = __get_local_module_file_location()
    if debug:
        print('Adding module to workers, module_path:', module_path)

    sc.addPyFile(module_path)

    del sys.modules['sparkonda']
    del sys.modules['sparkonda.module_helper']

    return os.path.split(module_path)[-1]
