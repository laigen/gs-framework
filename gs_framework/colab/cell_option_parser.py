import argparse


# This class serves as type definition. it won't be created
from typing import Optional


class CellOptions:

    @property
    def max_run_seconds(self) -> int:
        raise NotImplementedError()

    @property
    def mount_google_drive(self) -> bool:
        raise NotImplementedError()

    @property
    def stop_checking_seconds(self) -> Optional[int]:
        raise NotImplementedError()


class CellOptionParser:

    parser = argparse.ArgumentParser(description="Parse notebook options in cell comments")

    parser.add_argument('--max-run-seconds', default=60, type=int, dest='max_run_seconds',
                        help="the maximize run seconds the cell supposed to run, default 60 seconds")

    parser.add_argument('--mount-google-drive', action='store_true', dest='mount_google_drive',
                        help="indicate that this cell will mount google file system")

    parser.add_argument('--stop-checking-seconds', default=None, type=int, dest='stop_checking_seconds',
                        help="the seconds checking cell for stopped, default 10 seconds")

    # parser.add_argument('--init-notebook', action='store_true', dest='init_notebook',
    #                     help="indicate that this cell will be run to initialize the notebook")

    """
    A valid option line looks like
    # GS --mount-google-drive
    or
    #GS --max-run-seconds 600
    """
    @staticmethod
    def parse_line(line: str) -> CellOptions:
        words = None if line is None else line.split()
        if words is not None:
            if len(words) > 1 and words[0].upper() == '#GS':
                args = words[1:]
            elif len(words) > 2 and words[0] == '#' and words[1].upper() == 'GS':
                args = words[2:]
            else:
                args = []
        else:
            args = []

        return CellOptionParser.parser.parse_args(args)
