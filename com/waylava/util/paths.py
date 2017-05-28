import os


class Paths(object):
    UTIL_SUB_PATH = "com/waylava/util"

    @staticmethod
    def get_resources_path():
        path = os.path.dirname(os.path.realpath(__file__))
        path = path.replace(Paths.UTIL_SUB_PATH, "resources")
        return path

    @staticmethod
    def get_root_path():
        path = os.path.dirname(os.path.realpath(__file__))
        path = path.replace(Paths.UTIL_SUB_PATH, "")
        return path
