from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from accounting_efs.sampler.messager import EFSSamplerMessager


@pytest.fixture(
    params=[
        {
            "with_files": True,
            "with_sparse": False,
            "with_subdirs": False,
            "with_symlink": False,
            "with_hardlink": False,
        },
        {
            "with_files": True,
            "with_sparse": True,
            "with_subdirs": False,
            "with_symlink": False,
            "with_hardlink": False,
        },
        {
            "with_files": True,
            "with_sparse": False,
            "with_subdirs": True,
            "with_symlink": False,
            "with_hardlink": False,
        },
        {
            "with_files": True,
            "with_sparse": False,
            "with_subdirs": False,
            "with_symlink": True,
            "with_hardlink": False,
        },
        {
            "with_files": True,
            "with_sparse": False,
            "with_subdirs": False,
            "with_symlink": False,
            "with_hardlink": True,
        },
        {
            "with_files": True,
            "with_sparse": True,
            "with_subdirs": True,
            "with_symlink": True,
            "with_hardlink": True,
        },
        {
            "with_files": False,
            "with_sparse": False,
            "with_subdirs": False,
            "with_symlink": False,
            "with_hardlink": False,
        },
        {
            "with_files": False,
            "with_sparse": False,
            "with_subdirs": True,
            "with_symlink": False,
            "with_hardlink": False,
        },
        {
            "with_files": False,
            "with_sparse": False,
            "with_subdirs": False,
            "with_symlink": True,
            "with_hardlink": False,
        },
    ]
)
def test_dir(request):
    """
    Simulates a directory containing workspaces block stores. Depending on the settings, each
    workspace will contain:

    * (with_files=True) 3 files containing 500 bytes, 60000 bytes and 2 million bytes.
    * (with_sparse=True) 3 files with size 6GiB but only containing 500, 60000 and 2 million
      written bytes.
    * (with_subdirs=True) a ten-level-deep subdirectory tree with (if with_files=True) a 10kB file
      at the bottom.
    * (with_symlink=True) a symlink pointing to the 2 million byte file from with_files=True,
      whether it exists or not.
    * (with_hardlink=True) a second hard link to each of the with_files=True files.

    The workspaces will have names workspace0, workspace1, ...
    """

    with TemporaryDirectory() as tmpdir_str:
        tmpdir = Path(tmpdir_str)

        # The space used by a directory will vary by underlying FS.
        # It also varies by number of entries but we don't add many.
        dir_size = EFSSamplerMessager.count_size(tmpdir)

        expected_size = dir_size

        if request.param["with_files"]:
            with open(tmpdir / "500-byte-file", "wb") as fh:
                fh.write(b"1234" * 125)
                expected_size += 500

            with open(tmpdir / "60000-byte-file", "wb") as fh:
                fh.write(b"1234" * 15000)
                expected_size += 60000

            with open(tmpdir / "2000000-byte-file", "wb") as fh:
                fh.write(b"12345678" * 250000)
                expected_size += 2_000_000

        yield (tmpdir, expected_size)
