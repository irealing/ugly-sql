import sys

if sys.version_info[0] > 2:
    map = map


    def local_map(fun, it):
        return tuple(map(fun, it))
else:
    local_map = map
