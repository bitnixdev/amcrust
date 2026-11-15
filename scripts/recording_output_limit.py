"""POSIX exec helper: native recording output has an OS-enforced file-size ceiling."""
import os
import resource
import sys


def main():
    limit = int(sys.argv[1])
    if not 0 < limit <= 32 * 1024 * 1024 or len(sys.argv) != 4:
        raise ValueError("invalid recording output limit arguments")
    resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))
    os.execv(sys.argv[2], sys.argv[2:])


if __name__ == "__main__":
    main()
