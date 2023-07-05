import os
import glob
from src.lasfile.lasfile import LASFile, api_from_las, error_check


def get_test_las_paths():
    """Gets a list of las files in the test folder"""
    return glob.glob(os.path.join(os.path.dirname(__file__), '*.las'))


# Dictionary of file name tags to version numbers
version_dict = {
    '1_2': '1.2',
    '2_0': '2.0',
    '3_0': '3.0'
}


def test_read_las():
    """Tests that las files can be read"""
    for las_path in get_test_las_paths():
        version = None
        # Get the version number from the file name
        for tag, ver in version_dict.items():
            if tag in las_path:
                version = ver
        if version is None:
            break
        # Test that the file can be read
        las = LASFile(file_path=las_path)
        assert las is not None
        # Test that the version section is present, the version
        # number is correct, and that there are no errors in
        # parsing or validating the version section
        assert 'open_error' not in vars(las).keys()
        assert 'version_error' not in vars(las).keys()
        assert 'version_tb' not in vars(las).keys()
        assert las.version is not None
        assert las.version_num == version
        assert las.version.validated
        # Test that the well section is present, and that there are
        # no errors in parsing or validating the well section
        assert getattr(las, "well") is not None
        assert 'parse_error' not in vars(getattr(las, "well")).keys()
        assert 'validate_error' not in vars(getattr(las, "well")).keys()
        if version == '1.2' or version == '2.0':
            # Test that the curves section is present, and that
            # there are no errors in parsing or validating the
            # curves section
            assert getattr(las, "curves") is not None
            assert 'parse_error' not in vars(getattr(las, "curves")).keys()
            assert 'validate_error' not in vars(getattr(las, "curves")).keys()
            # Test that the data section is present, and that there
            # are no errors in parsing or validating the data
            # section
            assert getattr(las, "data") is not None
            assert 'parse_error' not in vars(getattr(las, "data")).keys()
            assert 'validate_error' not in vars(getattr(las, "data")).keys()
        elif version == '3.0':
            # Test that all sections present have no errors in
            # parsing or validating
            for section in las.sections:
                assert 'parse_error' not in vars(section).keys()
                assert 'validate_error' not in vars(section).keys()
        assert error_check(las) is True


def test_api_from_las():
    """Tests that the API can be calculated from a las file"""
    for las_path in get_test_las_paths():
        # try the file paths
        assert api_from_las(las_path) is not None


test_read_las()
test_api_from_las()
