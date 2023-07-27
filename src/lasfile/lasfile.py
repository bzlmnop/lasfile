# A python module for reading LAS files

import json
import os
import re
import traceback
from io import StringIO
from numpy import genfromtxt
from numpy import array
from csv import reader
from pandas import DataFrame
import warnings
from apinum import APINumber


class LASFileError(Exception):
    """Raised when a LAS file experiences an error in the read, parse,
    or validata process"""
    pass


class LASFileCriticalError(LASFileError):
    """Raised when a LAS file experiences an error in the parse or
    validate process that is critical to the file's integrity"""
    pass


class LASVersionError(LASFileCriticalError):
    """Raised when a LAS file experiences an error in the version
    parsing and validation process"""
    pass


class UnknownVersionError(LASVersionError):
    """Raised when a version value extracted from an LAS file is not
    known"""
    pass


class VersionExtractionError(LASVersionError):
    """Raise when a version value cannot be extracted from an LAS"""
    pass


class LASFileOpenError(LASFileCriticalError):
    """Raised when a LAS file experiences an error in the open
    process"""
    pass


class LASFileReadError(LASFileCriticalError):
    """Raised when a LAS file experiences an error in the read
    process"""
    pass


class LASFileSplitError(LASFileCriticalError):
    """Raised when a LAS file experiences an error in the split
    process"""
    pass


class SectionTitleError(LASFileCriticalError):
    """Raised when a LAS file experiences an error in the section
    title parsing and validation process"""
    pass


class MissingRequiredSectionError(LASFileCriticalError):
    """Raised when a LAS file is missing a required section"""
    pass


class RequiredSectionParseError(LASFileCriticalError):
    """Raised when a required section in a LAS file fails to parse"""
    pass


class MissingCriticalMnemonicError(LASFileCriticalError):
    """Raised when a LAS file required section is missing a required
    mnemonic that is critical to properly parsing the file"""
    pass


class VersionValidationError(LASVersionError):
    """Raised when a LAS file experiences an error in the version
    validation process"""
    pass


class LASFileMinorError(LASFileError):
    """Raised when a LAS file experiences an error in the parse or
    validate process that is not critical to the file's integrity"""
    pass


class SectionLoadError(LASFileMinorError):
    """Raised when an error occurs when loading a non-required
    section of an LAS file"""
    pass


class SectionParseError(LASFileMinorError):
    """Raised when an error occurs when parsing a non-required
    section of an LAS file"""
    pass


class MissingMnemonicError(LASFileMinorError):
    """Raise when a LAS file required section is missing a required
    mnemonic that is not critical to properly parsing the file"""
    pass


# Set known versions
known_versions = ['1.2', '2.0', '3.0']
# Set known sections from json file
dir_path = os.path.dirname(os.path.realpath(__file__))
known_secs_path = os.path.join(dir_path, 'known_sections.json')

with open(known_secs_path, 'r') as f:
    known_secs = json.load(f)

# Build a dictionary of required sections for each version from
# known_secs
required_sections = {}
for version, sections in known_secs.items():
    req_secs_list = [
        section_name for section_name, section in sections.items()
        if section['required']
    ]
    required_sections[version] = req_secs_list

header_section_names = []
data_section_names = []
for version, sections in known_secs.items():
    for section_name, sec_dict in sections.items():
        if (
            sec_dict['type'] == 'header' and
            section_name not in header_section_names
        ):
            header_section_names.append(section_name)
        elif (
            sec_dict['type'] == 'data' and
            section_name not in data_section_names
        ):
            data_section_names.append(section_name)


def get_version_num(data,
                    handle_common_errors=True,
                    accept_unknown_versions=False,
                    allow_non_numeric=False,
                    unknown_value=None):
    """
    Extracts and validates the version number from the given data.

    This function accepts either a string containing a version section,
    or a DataFrame containing a mnemonic column with a "VERS" value.
    It then tries to extract the version number and validate it.
    It handles several common errors, such as non-numeric versions, and
    allows the acceptance of unknown versions.

    Parameters:
    ----------
    data : str or pandas.DataFrame
        The input data to extract the version number from.
        If a string, it should contain a version section marked with '~V'.
        If a DataFrame, it should contain a column named 'mnemonic' with a
        "VERS" value.

    handle_common_errors : bool, optional
        Whether to handle common errors, such as whole number versions.
        (default is True)

    accept_unknown_versions : bool, optional
        Whether to accept and return unknown versions. (default is False)

    allow_non_numeric : bool, optional
        Whether to allow and return non-numeric versions. This only works
        if `accept_unknown_versions` is also True. (default is False)

    Returns:
    -------
    version_num : str
        The extracted version number.

    Raises:
    ------
    ValueError:
        If the input data is neither a string nor a DataFrame.

    Exception:
        If the version number could not be retrieved, or if it
        was not recognized and `accept_unknown_versions` is False.
    """
    # Parse input data based on its type
    if isinstance(data, str):
        try:
            # If the data is a string, extract the version section
            section_regex = re.compile(r'(~[V].+?)(?=~[VW]|$)', re.DOTALL)
            version_section = re.findall(section_regex, data)[0]
            # Parse the version section into a DataFrame
            df = parse_header_section(version_section)
        except Exception as e:
            raise LASVersionError(
                f"Could not extract and parse version section: {str(e)}"
            )
    elif isinstance(data, DataFrame):
        df = data
    else:
        raise ValueError("Input must be str or DataFrame.")

    # Try to extract version number
    try:
        version_num = df.loc[df['mnemonic'] == "VERS", "value"].values[0]
    except Exception as e:
        raise VersionExtractionError(f"Could not get version: {str(e)}")

    # Check if version number is known
    if version_num in known_versions:
        # Return version number if it is known
        return version_num

    # Handle common errors like conversion to float
    if handle_common_errors:
        try:
            float_version_num = str(float(version_num))
            if float_version_num in known_versions:
                return float_version_num
        except ValueError:
            pass

    # Accept unknown versions, verify if they are integers
    if accept_unknown_versions:
        try:
            float(version_num)
            return version_num
        except ValueError:
            if allow_non_numeric:
                return version_num

    # Raise error if no known version number was found even after
    # handling common errors and if unknown versions are not accepted
    raise UnknownVersionError(
        "Could not get version, version number not recognized."
    )


def get_version_section(data,
                        handle_common_errors=True,
                        accept_unknown_versions=False,
                        allow_non_numeric=False,
                        unknown_value=None):
    """
    Extracts the version section from the given raw data, parses it,
    validates it, and returns a loaded section object.

    This function performs several steps to process the version section
    from the raw data. It extracts the version section, parses it into
    a DataFrame, attempts to extract and validate a version number, and
    then tries to load this all into a LASSection object.

    Parameters:
    ----------
    data : str
        The raw data string to extract the version section from. The
        version section should be marked with '~V'.

    handle_common_errors : bool, optional
        Whether to handle common errors when extracting the
        version number. (default is True)

    accept_unknown_versions : bool, optional
        Whether to accept unknown versions when extracting the version
        number. (default is False)

    allow_non_numeric : bool, optional
        Whether to allow non-numeric versions when extracting the
        version number. (default is False)

    unknown_value : any, optional
        The value to use when an unknown version number is encountered.
        This only applies if `accept_unknown_versions`
        is also True. (default is None)

    Returns:
    -------
    loaded_section : LASSection
        The loaded version section.

    Raises:
    ------
    Exception:
        If parsing the version section fails, if extracting the
        version number fails, if validating the version section fails,
        or if loading the section into a LASSection object fails.
    """
    # Extract whole text of version section from raw data
    # Regex matches everything between '~V' and '~V' or '~W'
    section_regex = re.compile(r'(~[V].+?)(?=~[VW]|$)', re.DOTALL)
    section_list = re.findall(section_regex, data)
    # Take the first match, which should be the version section
    version_section = section_list[0]

    # Try to parse version section into a DataFrame
    try:
        df = parse_header_section(version_section)
    except Exception as e:
        raise RequiredSectionParseError(
            f"Failed to parse version section: {e}"
        )

    # Attempt to extract a version number from the parsed section
    try:
        version_num = get_version_num(
            df,
            handle_common_errors=handle_common_errors,
            accept_unknown_versions=accept_unknown_versions,
            allow_non_numeric=allow_non_numeric,
            unknown_value=unknown_value
        )
    except Exception as e:
        raise VersionExtractionError(f"Failed to extract version number: {e}")

    # Define default values for dlm_val and wrap
    dlm_val = None
    wrap = None

    # Attempt to validate the section
    if validate_version(df, version_num=version_num) == []:
        if version_num is not None:
            try:
                wrap_val = df.loc[df['mnemonic'] == "WRAP", "value"].values[0]
                if wrap_val.upper() == 'YES':
                    wrap = True
                elif wrap_val.upper() == 'NO':
                    wrap = False
            except Exception as e:
                wrap = None
                if version_num in ["1.2", "2.0"]:
                    raise MissingCriticalMnemonicError(
                        f"Could not get WRAP: {str(e)}"
                    )
        if version_num == "3.0":
            try:
                dlm_val = df.loc[df['mnemonic'] == "DLM", "value"].values[0]
            except Exception as e:
                dlm_val = None
                raise MissingCriticalMnemonicError(
                    f"Could not get DLM: {str(e)}"
                )

        # Attempt to load the section into a section object
        try:
            loaded_section = LASSection(
                'version',
                version_section,
                'header',
                version_num,
                delimiter=dlm_val,
                parse_on_init=False,
                validate_on_init=False,
                wrap=wrap
            )
            loaded_section.df = df
            loaded_section.validated = True
            loaded_section.version_num = version_num
            return loaded_section
        except Exception:
            raise LASVersionError("Couldn't load into section object.")
    else:
        raise VersionValidationError("Could not validate the version section.")


def parse_title_line(title_line,
                     version_num,
                     all_lowercase=True,
                     assocs=False):
    """
    Parses the title line based on the provided version number.

    This function handles parsing of title lines differently based
    on the given version number. Specifically, it handles versions
    1.2 and 2.0 differently from version 3.0.

    Parameters:
    ----------
    title_line : str
        The title line to be parsed.

    version_num : str
        The version number, which determines how the title line is
        parsed. Can be "1.2", "2.0", or "3.0".

    all_lowercase : bool, optional
        Whether to convert all characters in the title line to
        lowercase before parsing. (default is True)

    assocs : bool, optional
        Whether to consider associations in the parsing process.
        This is only relevant for version "3.0". (default is False)

    Returns:
    -------
    Parsed title line : varies
        The result of the title line parsing, which depends on the
        version number.

    Raises:
    ------
    Exception:
        If the version number is not one of the expected values
        ("1.2", "2.0", "3.0").
    """
    if version_num == "1.2" or version_num == "2.0":
        return parse_v2_title(title_line, all_lowercase=all_lowercase)
    elif version_num == "3.0":
        return parse_v3_title(
            title_line,
            all_lowercase=all_lowercase,
            assocs=assocs
        )


def parse_v2_title(title_line, all_lowercase=True):
    """
    Parses a version 2.0 title line.

    This function specifically handles the parsing of title lines for
    version 2.0. It checks if the line begins with '~', extracts the
    section title, and optionally converts it to lowercase.

    Parameters:
    ----------
    title_line : str
        The title line to be parsed. This should begin with '~'.

    all_lowercase : bool, optional
        Whether to convert the section title to lowercase.
        (default is True)

    Returns:
    -------
    section_title : str
        The parsed section title from the title line.

    Raises:
    ------
    Exception:
        If the title line does not begin with '~'.
    """
    # Strip leading and trailing whitespace
    title_line = title_line.strip()
    # Check if it actually is a title line
    if title_line.startswith('~'):
        title_line = title_line.strip('~')
        section_title = title_line[0]
        if all_lowercase:
            section_title = section_title.lower()
        return section_title
    else:
        raise SectionTitleError(
            "Cannot parse title line. Title lines must begin with '~'."
        )


def parse_v3_title(title_line, all_lowercase=True, assocs=False):
    """
    Parses a version 3.0 title line.

    This function specifically handles the parsing of title lines for
    version 3.0. It checks if the line begins with '~', extracts the
    section title and optionally an associated value, and converts them
    to lowercase if requested.

    Parameters:
    ----------
    title_line : str
        The title line to be parsed. This should begin with '~'.

    all_lowercase : bool, optional
        Whether to convert the section title and association to
        lowercase. (default is True)

    assocs : bool, optional
        Whether to consider associations in the parsing process.
        (default is False)

    Returns:
    -------
    section_title : str or tuple
        The parsed section title from the title line. If 'associations'
        is True and an association is present, a tuple of
        (section_title, association) is returned.

    Raises:
    ------
    Exception:
        If the title line does not begin with '~'.
    """
    assoc = None
    has_assoc = False
    # Strip leading and trailing whitespace
    title_line = title_line.strip()
    # Check if it actually is a title line
    if title_line.startswith('~'):
        # Strip leading '~'
        title_line = title_line.strip('~')
        # Check if there is an association
        if '|' in title_line:
            # If so, split the title line into the title and association
            has_assoc = True
            title_line, assoc = title_line.split('|')
            # If the association is not empty, strip leading and
            # trailing whitespace
            if assoc.strip().split(' ')[0] != '':
                assoc = assoc.strip().split(' ')[0]
            # Otherwise, set the association to None
            else:
                assoc = None
        # Extract the section title
        if title_line.split(' ')[0] != '':
            section_title = title_line.split(' ')[0]
        else:
            section_title = None
        # Convert to lowercase if requested
        if all_lowercase:
            if section_title is not None:
                section_title = section_title.lower()
            if assocs and has_assoc:
                if assoc is not None:
                    assoc = assoc.lower()
        # Return the section title and association if requested
        if assocs and has_assoc:
            return section_title, assoc
        # Otherwise, just return the section title
        else:
            return section_title
    else:
        # If the line does not begin with '~', raise an exception
        raise SectionTitleError(
            "Cannot parse a line that does not begin with '~' as a "
            "title line."
        )


def split_sections(data, version_num, known_secs=known_secs):
    """
    Splits the input data into sections based on the
    provided version number.

    This function handles the splitting of input data
    differently based on the given version number. It
    handles versions 1.2 and 2.0 differently from version
    3.0. For each version, it uses regular expressions to
    identify and extract the sections, and then stores these
    sections in a dictionary using appropriate keys.

    Parameters:
    ----------
    data : str
        The input data to be split into sections.

    version_num : str
        The version number, which determines how the
        data is split. Can be "1.2", "2.0", or "3.0".

    known_secs : dict, optional
        A dictionary mapping known section names to
        their details for each version.
        (default is known_secs)

    Returns:
    -------
    section_dict : dict
        A dictionary with the parsed sections. The keys
        depend on the version number and the content of
        the sections.

    Raises:
    ------
    Exception:
        If the version number is not one of the expected
        values ("1.2", "2.0", "3.0").
    """
    # Split the data into sections based on the version number
    if version_num == '1.2' or version_num == '2.0':
        # Use a regular expression to split the data into sections
        section_regex = re.compile(r'(~[VWPCOA].+?)(?=~[VWPCOA]|$)', re.DOTALL)
        sections = re.findall(section_regex, data)
        # Store the sections in a dictionary
        section_dict = {}
        for section in sections:
            # Get the title line/header of the section
            header_end = section.index('\n')
            header = section[:header_end].strip().upper()
            # Store the section in the dictionary based on the header
            if header.startswith('~W'):
                section_dict['well'] = section.strip()
            elif header.startswith('~V'):
                section_dict['version'] = section.strip()
            elif header.startswith('~C'):
                section_dict['curves'] = section.strip()
            elif header.startswith('~P'):
                section_dict['parameters'] = section.strip()
            elif header.startswith('~O'):
                section_dict['other'] = section.strip()
            elif header.startswith('~A'):
                section_dict['data'] = section.strip()
        # Return the dictionary of sections
        return section_dict
    elif version_num == '3.0':
        # Use a regular expression to split the data into sections
        section_regex = re.compile(r'(~[A-Za-z].+?)(?=~[A-Za-z]|$)', re.DOTALL)
        sections = re.findall(section_regex, data)
        # Store the sections in a dictionary
        section_dict = {}
        # Get the known sections for version 3.0
        known_secs = known_secs["3.0"]
        known_sec_names = known_secs.keys()
        for section in sections:
            # Get the title line/header of the section
            section_found = False
            try:
                title_line_end = section.index('\n')
            except Exception as e:
                raise LASFileCriticalError(
                    f"Could not find the end of "
                    f"the title line for a section: {e}"
                    )
            title_line = section[:title_line_end].strip()
            # Parse the title line to get the section title
            section_title = parse_title_line(
                title_line,
                "3.0",
                all_lowercase=True
            )
            # If the section title is a known section name, store it
            # using the known section name as the key
            if section_title in known_sec_names:
                section_dict[section_title] = section.strip()
                section_found = True
            # If the section title is not a known section name, check
            # if it is an alias for a known section name then store it
            # using the known section name as the key
            else:
                for known_sec_name, known_sec in known_secs.items():
                    if section_title in known_sec["titles"]:
                        section_dict[known_sec_name] = section.strip()
                        section_found = True
                if not section_found:
                    section_dict[section_title] = section.strip()
        # Return the dictionary of sections
        return section_dict
    else:
        # Raise an exception if the version number is not one of the
        # expected values
        raise UnknownVersionError(
            "Unknown version. Must be '1.2','2.0', or '3.0'"
        )


def parse_header_section(section_string, version_num='2.0', delimiter=None):
    """
    Parses the header section of a LAS file.

    This function parses the header section of a LAS file and returns
    a dictionary with the parsed information. The version number is
    used to determine how the header section is parsed. The delimiter
    is used to split the header section into lines. If no delimiter is
    provided, the header section is split into lines using the newline
    character.

    Parameters:
    ----------
    section_string : str
        The header section of a LAS file.

    version_num : str, optional
        The version number of the LAS file. (default is '2.0')

    delimiter : str, optional
        The delimiter used to split the header section into lines.
        (default is None)

    Returns:
    -------
    header_dict : dict
        A DataFrame with the parsed header information.
    """
    results = []
    lines = section_string.strip().split("\n")
    if version_num == '2.0' or version_num == '1.2':
        # Skip comment, title, and empty lines
        for line in lines:
            mnemonic = None
            units = None
            value = None
            descr = None
            if (
                line.strip().startswith("#") or
                line.strip().startswith("~") or
                line.strip().strip("\n") == ""
            ):
                continue
            # Try to parse the line
            try:
                # Remove whitespace from the line
                line = line.strip()
                # Get the mnemonic.
                # The mnemonic is everything before the first period,
                # stripped of whitespace.
                frst_prd = line.index('.')
                mnemonic = line[:frst_prd].strip()
                if version_num == '1.2' and mnemonic in [
                    'COMP',
                    'WELL',
                    'FLD',
                    'LOC',
                    'PROV',
                    'SRVC',
                    'DATE',
                    'UWI',
                    'API',
                ]:
                    # Get the units.
                    # The units are everything between the first period
                    # and the first space after the first period,
                    # stripped of whitespace.
                    line_aft_frst_prd = line[frst_prd+1:]
                    frst_spc_aft_frst_prd = line_aft_frst_prd.index(' ')
                    units = line_aft_frst_prd[:frst_spc_aft_frst_prd].strip()
                    # Get the value.
                    # The value is everything between the first space
                    # after the first period and the last colon,
                    # stripped of whitespace.
                    lst_col = line_aft_frst_prd.rindex(':')
                    descr = line_aft_frst_prd[
                        frst_spc_aft_frst_prd:lst_col
                    ].strip()
                    # Get the description.
                    # The description is everything after the last colon,
                    # stripped of whitespace.
                    value = line_aft_frst_prd[lst_col+1:].strip()
                    results.append(
                        {
                            "mnemonic": mnemonic,
                            "units": units if units != "" else None,
                            "value": value if value != "" else None,
                            "description": descr if descr != "" else None,
                            "errors": None
                        }
                    )
                else:
                    # Get the units.
                    # The units are everything between the first period
                    # and the first space after the first period,
                    # stripped of whitespace.
                    line_aft_frst_prd = line[frst_prd+1:]
                    frst_spc_aft_frst_prd = line_aft_frst_prd.index(' ')
                    units = line_aft_frst_prd[:frst_spc_aft_frst_prd].strip()
                    # Get the value.
                    # The value is everything between the first space
                    # after the first period and the last colon,
                    # stripped of whitespace.
                    lst_col = line_aft_frst_prd.rindex(':')
                    value = line_aft_frst_prd[
                        frst_spc_aft_frst_prd:lst_col
                    ].strip()
                    # Get the description.
                    # The description is everything after the last colon,
                    # stripped of whitespace.
                    descr = line_aft_frst_prd[lst_col+1:].strip()
                    results.append(
                        {
                            "mnemonic": mnemonic,
                            "units": units if units != "" else None,
                            "value": value if value != "" else None,
                            "description": descr if descr != "" else None,
                            "errors": None
                        }
                    )
            except Exception as e:
                results.append(
                        {
                            "mnemonic": mnemonic,
                            "units": units if units != "" else None,
                            "value": value if value != "" else None,
                            "description": descr if descr != "" else None,
                            "errors": LASFileError(
                                f"Error parsing header line '{line}'. {e}"
                            )
                        }
                    )
        return DataFrame(results)
    elif version_num == '3.0':
        for line in lines:
            mnemonic = None
            units = None
            value = None
            descr = None
            format = None
            assocs = None
            # Skip comment, title, and empty lines
            if (
                line.strip().startswith("#") or
                line.strip().startswith("~") or
                line.strip().strip("\n") == ""
            ):
                continue
            # Try to parse the line
            try:
                # Remove whitespace from the line
                line = line.strip()
                # Get the mnemonic.
                # The mnemonic is everything before the
                # first period, stripped of whitespace
                frst_prd = line.index('.')
                mnemonic = line[:frst_prd].strip()
                # Get the units.
                # The units are everything between the first period and the
                # first space after the first period, stripped of whitespace.
                line_aft_frst_prd = line[frst_prd+1:]
                frst_spc_aft_frst_prd = line_aft_frst_prd.index(' ')
                units = line_aft_frst_prd[:frst_spc_aft_frst_prd].strip()
                # Get the value.
                # The value is everything between the first space after the
                # first period and the last colon, stripped of whitespace.
                lst_col = line_aft_frst_prd.rindex(':')
                value = (
                    line_aft_frst_prd[frst_spc_aft_frst_prd:lst_col].strip()
                )
                # Get the description, format, and associations.
                # The description is everything after the last colon and
                # before the first brace or pipe, if no braces are present,
                # stripped of whitespace.
                line_aft_lst_col = line_aft_frst_prd[lst_col+1:]
                if '{' in line_aft_lst_col and '}' in line_aft_lst_col:
                    frst_brc_aft_lst_col = line_aft_lst_col.index('{')
                    descr = line_aft_lst_col[:frst_brc_aft_lst_col]
                    line_aft_frst_brc = (
                        line_aft_lst_col[frst_brc_aft_lst_col+1:]
                    )
                    clsng_brc = line_aft_frst_brc.rindex('}')
                    format = line_aft_frst_brc[:clsng_brc].strip()
                    line_aft_clsng_brc = line_aft_frst_brc[clsng_brc+1:]
                    if '|' in line_aft_clsng_brc:
                        bar = line_aft_clsng_brc.index('|')
                        assocs = line_aft_clsng_brc[bar+1:].strip()
                elif '|' in line_aft_lst_col:
                    bar = line_aft_lst_col.index('|')
                    assocs = line_aft_lst_col[bar+1:].strip()
                else:
                    descr = line_aft_frst_prd[lst_col+1:].strip()
                # Add the parsed values to the results list
                results.append(
                    {
                        "mnemonic": mnemonic,
                        "units": units if units != "" else None,
                        "value": value if value != "" else None,
                        "description": descr if descr != "" else None,
                        "format": format if format != "" else None,
                        "associations": assocs if assocs != "" else None
                    }
                )
            except Exception as e:
                results.append(
                        {
                            "mnemonic": mnemonic,
                            "units": units if units != "" else None,
                            "value": value if value != "" else None,
                            "description": descr if descr != "" else None,
                            "errors": LASFileError(
                                f"Error parsing header line '{line}'. {e}"
                            )
                        }
                    )
        # Return the results as a DataFrame
        return DataFrame(results)


def parse_data_section(raw_data, version_num, wrap, delimiter=None):
    """
    Parses the data section of the input raw data based on the provided
    version number.

    This function handles the parsing of the data section from the
    input raw data. It removes lines beginning with '#' or '~', and
    then loads the data into a LASData object.

    Parameters:
    ----------
    raw_data : str
        The raw data to be parsed.

    version_num : str
        The version number, which is used when loading the data into a
        LASData object.

    delimiter : str, optional
        The delimiter character used to separate values in the data. If
        not provided, the default delimiter for the LASData class is
        used.

    Returns:
    -------
    loaded_data : LASData
        The loaded data as a LASData object.
    """
    filtered_data = re.sub(r'^[#~].*\n', '', raw_data, flags=re.MULTILINE)
    loaded_data = LASData(
        filtered_data,
        version_num,
        wrap=wrap,
        delimiter=delimiter)
    return loaded_data


def validate_version(df, version_num=None):
    """
    Validates the version of a dataframe.

    This function checks if the dataframe contains all required
    mnemonics for the given version number and verifies the values of
    these mnemonics. The specific checks differ based on the version
    number.

    Parameters:
    ----------
    df : DataFrame
        The dataframe to be validated. This dataframe should contain a
        'mnemonic' column and a 'value' column.

    version_num : str, optional
        The version number of the dataframe. Must be either "1.2",
        "2.0", or "3.0". If not provided, the function will use the
        value in the dataframe.

    Returns:
    -------
    bool:
        Returns True if the dataframe is valid for the given version
        number.

    Raises:
    ------
    Exception:
        If any of the required mnemonics are missing or have invalid
        values, or if the version number is not one of the expected
        values ("1.2", "2.0", "3.0").
    """
    validate_errors = []
    if version_num in ["1.2", "2.0"]:
        req_mnemonics = ["VERS", "WRAP"]
    elif version_num == "3.0":
        req_mnemonics = ["VERS", "WRAP", "DLM"]
    else:
        validate_errors.append(
            UnknownVersionError(
                "Unknown version. Must be '1.2','2.0', or '3.0'"
            )
        )
        return validate_errors

    # Test if all required mnemonics are present
    if not all(mnemonic in df.mnemonic.values for mnemonic in req_mnemonics):
        # Try and fix the missing mnemonic error by adjusting mnemonic
        # case to upper.
        if not all(
            mnemonic in [val.upper() for val in df.mnemonic.values]
            for mnemonic in req_mnemonics
        ):
            # Make a list of the missing mnemonics
            missing_mnemonics = [
                mnemonic for mnemonic in req_mnemonics
                if mnemonic not in df.mnemonic.values
            ]
            validate_errors.append(
                MissingCriticalMnemonicError(
                    f"Missing required version section mnemonics: "
                    f"{missing_mnemonics}"
                )
            )
            return validate_errors
        else:
            # Auto repair the mnemonic case
            df['mnemonic'] = df['mnemonic'].str.upper()

    # Set empty wrap value
    wrap = None

    try:
        wrap = df.loc[df['mnemonic'] == "WRAP", "value"].values[0]
    except Exception as e:
        if version_num in ["1.2", "2.0"]:
            validate_errors.append(
                LASVersionError(f"Couldnt get WRAP value: {str(e)}")
            )
            return validate_errors
        else:
            pass

    if version_num in ["1.2", "2.0"]:
        if wrap is not None and wrap.upper() not in ["YES", "NO"]:
            validate_errors.append(
                LASVersionError(
                    "Wrap value for versions 1.2 and 2.0 must be 'YES' "
                    "or 'NO'."
                )
            )
            return validate_errors
    elif version_num == "3.0":
        try:
            dlm = df.loc[df['mnemonic'] == "DLM", "value"].values[0]
            if "wrap" in locals():
                if wrap is not None and wrap.upper() != "NO":
                    validate_errors.append(
                        LASVersionError(
                            "Invalid wrap value. Must be 'NO' for version 3.0"
                        )
                    )
                    return validate_errors
            if dlm.upper() not in ["SPACE", "COMMA", "TAB", None, '']:
                validate_errors.append(
                    LASVersionError(
                        "Invalid delimiter value for version 3.0, should be "
                        "'SPACE', 'COMMA', or 'TAB'"
                    )
                )
                return validate_errors
        except Exception as e:
            validate_errors.append(
                LASVersionError(f"Couldnt get DLM value: {str(e)}")
            )
            return validate_errors
    return validate_errors


def validate_v2_well(df):
    """
    Validates the well section of a dataframe for version 2 LAS files.

    This function checks if the dataframe contains all the required
    mnemonics specific to the well section of a version 2 LAS file.

    Parameters:
    ----------
    df : DataFrame
        The dataframe to be validated. This dataframe should contain a
        'mnemonic' column.

    Returns:
    -------
    bool:
        Returns True if the dataframe is valid for the well section of
        a version 2 LAS file.

    Raises:
    ------
    Exception:
        If any of the required mnemonics are missing.
    """
    validate_errors = []
    # Set of required mnemonics for version 2.0 well sections
    req_mnemonics = [
        "STRT",
        "STOP",
        "STEP",
        "NULL",
        "COMP",
        "WELL",
        "FLD",
        "LOC",
        "SRVC",
        "DATE",
    ]
    # Instantiate an empty list to store missing mnemonics
    missing_mnemonics = []
    # Check if all required mnemonics are present
    if all(mnemonic not in df.mnemonic.values for mnemonic in req_mnemonics):
        # Make a list of which mnemonics are missing
        for mnemonic in req_mnemonics:
            if mnemonic not in df.mnemonic.values:
                missing_mnemonics.append(mnemonic)
    # Check that either PROV or CNTY, STAT, CTRY required mnemonics
    # are present
    if (
        "PROV" not in df.mnemonic.values and
        all(
            mnemonic not in df.mnemonic.values
            for mnemonic in ["CNTY", "STAT", "CTRY"]
        )
    ):
        # Make a list of which mnemonics are missing
        for mnemonic in ["CNTY", "STAT", "CTRY"]:
            if mnemonic not in df.mnemonic.values:
                missing_mnemonics.append(mnemonic)
    if (
            "API" not in df.mnemonic.values and
            "UWI" not in df.mnemonic.values
    ):
        missing_mnemonics.append("API")
        missing_mnemonics.append("UWI")
    if missing_mnemonics != []:
        validate_errors.append(
            MissingMnemonicError(
                f"Missing required mnemonics: {missing_mnemonics}"
            )
        )
    # Test if there is an errors column in the dataframe
    if "errors" in df.columns:
        # If the value in the mnemonic column is in the req_mnemonics
        # list, and the value in the errors column is not None, append
        # a the error to the validate_errors list
        for index, row in df.iterrows():
            if row["mnemonic"] in req_mnemonics and row["errors"] is not None:
                validate_errors.append(
                    LASFileCriticalError(
                        f"Error parsing required header line "
                        f"'{index}', {row['errors']}"
                    )
                )
            elif row["errors"] is not None:
                validate_errors.append(
                    LASFileMinorError(
                        f"Error parsing header line '{index}', "
                        f"{row['errors']}"
                    )
                )
    return validate_errors


def validate_v3_well(df):
    """
    Validates the well section of a DataFrame for
    version 3 LAS files.

    This function checks if the DataFrame contains all the required
    mnemonics specific to the well section of a version 3 LAS file. It
    also validates geographic coordinates and country code.

    Parameters:
    ----------
    df : DataFrame
        The DataFrame to be validated. This DataFrame should contain a
        'mnemonic' column.

    Returns:
    -------
    bool:
        Returns True if the DataFrame is valid for the well section of
        a version 3 LAS file.

    Raises:
    ------
    Exception:
        If any of the required mnemonics are missing, or if geographic
        coordinates or country code are not properly specified.
    """
    validate_errors = []
    # Set of required mnemonics for version 3.0 well sections
    req_mnemonics = [
        "STRT",
        "STOP",
        "STEP",
        "NULL",
        "COMP",
        "WELL",
        "FLD",
        "LOC",
        "SRVC",
        "CTRY",
        "DATE",
    ]
    # Set of valid country codes for the CTRY mnemonic
    valid_country_codes = ["US", "CA"]
    # Instantiate an empty list to store missing mnemonics
    missing_mnemonics = []
    # Check if all required mnemonics are present
    if all(
        mnemonic not in df.mnemonic.values
        for mnemonic in req_mnemonics
    ):
        # Make a list of which mnemonics are missing
        for mnemonic in req_mnemonics:
            if mnemonic not in df.mnemonic.values:
                missing_mnemonics.append(mnemonic)
    # Check that either LATI, LONG, GDAT or X, Y, GDAT, HZCS are present
    if (
        all(
            mnemonic not in df.mnemonic.values
            for mnemonic in ["LATI", "LONG", "GDAT"]
        )
        or
        all(
            mnemonic not in df.mnemonic.values
            for mnemonic in ["X", "Y", "GDAT", "HZCS"]
        )
    ):
        if (
            any(
                mnemonic in df.mnemonic.values
                for mnemonic in ["LATI", "LONG", "GDAT"]
            )
        ):
            # Make a list of which mnemonics are missing
            for mnemonic in ["LATI", "LONG", "GDAT"]:
                if mnemonic not in df.mnemonic.values:
                    missing_mnemonics.append(mnemonic)
        elif (
            any(
                mnemonic in df.mnemonic.values
                for mnemonic in ["X", "Y", "GDAT", "HZCS"]
            )
        ):
            # Make a list of which mnemonics are missing
            for mnemonic in ["X", "Y", "GDAT", "HZCS"]:
                if mnemonic not in df.mnemonic.values:
                    missing_mnemonics.append(mnemonic)
    # Check that CTRY is present and a valid valued and the required
    # mnemonics for the country code are present
    if "CTRY" in df.mnemonic.values:
        country_code = df.loc[
            df["mnemonic"] == "CTRY", 'value'
        ].values[0].upper()
        if country_code in valid_country_codes:
            if country_code == "CA":
                if all(
                    mnemonic not in df.mnemonic.values
                    for mnemonic in ["PROV", "UWI", "LIC"]
                ):
                    # Make a list of which mnemonics are missing
                    for mnemonic in ["PROV", "UWI", "LIC"]:
                        if mnemonic not in df.mnemonic.values:
                            missing_mnemonics.append(mnemonic)
            elif country_code == "US":
                if all(
                    mnemonic not in df.mnemonic.values
                    for mnemonic in ["STAT", "CNTY", "API"]
                ):
                    # Make a list of which mnemonics are missing
                    for mnemonic in ["STAT", "CNTY", "API"]:
                        if mnemonic not in df.mnemonic.values:
                            missing_mnemonics.append(mnemonic)
            elif (
                    country_code is None or
                    country_code == ''
            ):
                pass
            else:
                validate_errors.append(
                    LASFileMinorError(
                        "Value for country code mnemonic is invalid: "
                        f"{country_code}. Must be a valid internet "
                        "country code."
                    )
                )
    if missing_mnemonics != []:
        validate_errors.append(
            MissingMnemonicError(
                f"Missing required mnemonics: {missing_mnemonics}"
            )
        )
    # Test if there is an errors column in the dataframe
    if "errors" in df.columns:
        # If the value in the mnemonic column is in the req_mnemonics
        # list, and the value in the errors column is not None, append
        # a the error to the validate_errors list
        for index, row in df.iterrows():
            if row["mnemonic"] in req_mnemonics and row["errors"] is not None:
                validate_errors.append(
                    LASFileCriticalError(
                        f"Error parsing required header line "
                        f"'{index}', {row['errors']}"
                    )
                )
            elif row["errors"] is not None:
                validate_errors.append(
                    LASFileMinorError(
                        f"Error parsing header line '{index}', "
                        f"{row['errors']}"
                    )
                )
    return validate_errors


def validate_well(df, version_num):
    """
    Validates the well section of a DataFrame for specified LAS file
    versions.

    This function calls either validate_v2_well or validate_v3_well
    depending on the version number provided. It verifies the DataFrame
    for the required structure according to the LAS version.

    Parameters:
    ----------
    df : DataFrame
        The DataFrame to be validated. This DataFrame should contain a
        'mnemonic' column.

    version_num : str
        A string representing the LAS version. Valid values are "1.2",
        "2.0", and "3.0".

    Returns:
    -------
    bool:
        Returns True if the DataFrame is valid for the well section of
        the given LAS version.

    Raises:
    ------
    Exception:
        If validation fails in the respective validate function.
    """
    validate_errors = []
    if version_num == "1.2" or version_num == "2.0":
        try:
            errors = validate_v2_well(df)
            if errors is not None:
                for error in errors:
                    validate_errors.append(error)
        except Exception as e:
            validate_errors.append(
                LASFileCriticalError(
                    f"Error validating well section: {e}"
                )
            )
    if version_num == "3.0":
        try:
            errors = validate_v3_well(df)
            if errors is not None:
                for error in errors:
                    validate_errors.append(error)
        except Exception as e:
            validate_errors.append(
                LASFileCriticalError(
                    f"Error validating well section: {e}"
                )
            )
    return validate_errors


def validate_curves(df, version_num):
    """
    Validates the curves section of a specified LAS file.

    This function checks if the DataFrame contains all the required
    mnemonics specific to the curves section of a LAS file. It also
    checks that the mnemonics are in the correct order.

    Parameters:
    ----------
    df : DataFrame
        The DataFrame of the parsed curves section of a LAS file to be
        validated. This DataFrame should contain a 'mnemonic' column and
        an error column.

    version_num : str
        A string representing the LAS version. Valid values are "1.2",
        "2.0", and "3.0".

    Returns:
    -------
    bool:
        Returns True if the DataFrame is valid for the curves section
        of the given LAS version.
    """
    validate_errors = []
    # If ther is an errors column, check that there are no errors
    if "errors" in df.columns:
        if not df.errors.isnull().all():
            # If there are errors, append them to the validate_errors
            # list
            for index, row in df.iterrows():
                if row["errors"] is not None:
                    validate_errors.append(
                        LASFileCriticalError(
                            f"Error parsing curve line '{index}', "
                            f"{row['errors']}"
                        )
                    )
    return validate_errors


class LASData():
    """
    Class for storing and handling Log ASCII
    Standard (LAS) data.

    The class provides an interface to load, handle and validate LAS
    data. The LAS format is used to store well log data in the oil and
    gas industry.

    Attributes:
    ----------
    raw_data : str
        The raw LAS data as a string.

    version_num : str
        The version of the LAS file.

    wrap : bool

    delimiter : str, optional
        The delimiter used in the LAS data, such as SPACE, COMMA, or
        TAB. Defaults to None, indicating a space delimiter.

    invalid_raise : bool, optional
        Whether to raise an exception when invalid data is encountered.
        Defaults to False.

    unrecognized_delimiters : bool, optional
        If True, any unrecognized delimiters are replaced with the
        default delimiter.
        Defaults to True.

    default_delimiter : str, optional
        The default delimiter to use if an unrecognized delimiter is
        found. Defaults to a space.

    delimiter_error : str, optional
        Stores an error message if an unrecognized delimiter is found.

    data : numpy.ndarray
        The parsed LAS data, stored as a numpy array.

    df : pandas.DataFrame
        The parsed LAS data, stored as a pandas DataFrame.

    read_errors : list, optional
        Stores any read errors encountered when loading the data.

    Methods:
    -------
    __init__(self, raw_data, version_num, delimiter=None,
    invalid_raise=False, unrecognized_delimiters=True,
    default_delimiter=' ')
        Initializes the LASData object by parsing the provided
        raw LAS data.
    """
    def __init__(
        self,
        raw_data,
        version_num,
        wrap=False,
        delimiter=None,
        invalid_raise=False,
        unrecognized_delimiters=True,
        default_delimiter=' '
    ):
        # Initialize attributes
        self.raw_data = raw_data
        self.version_num = version_num
        self.wrap = wrap
        self.delimiter = delimiter
        # Initialize the delimiter dictionary
        delim_dict = {
            'SPACE': ' ',
            'COMMA': ',',
            'TAB': '\t'
        }
        delim = None
        # Get the delimiter value from the dictionary
        if self.delimiter is not None and self.delimiter in delim_dict.keys():
            delim = delim_dict[self.delimiter]
        # If the delimiter is a space, comma, tab, or None, use it
        elif self.delimiter in delim_dict.values() or self.delimiter is None:
            delim = self.delimiter
        elif unrecognized_delimiters:
            # If unrecognized delimiters are allowed, use the default
            # delimiter if it is in the dictionary
            if default_delimiter in delim_dict.values():
                delim = default_delimiter
            else:
                # Otherwise, raise an error
                self.delimiter_error = LASFileCriticalError(
                    f"Unrecognized delimiter: '{self.delimiter}', and default "
                    f"delimiter '{default_delimiter} unable to load!"
                )
        else:
            # If unrecognized delimiters are not allowed, and the
            # input delimiter is not in the dictionary, raise an error
            self.delimiter_error = (
                f"Unrecognized delimiter: '{self.delimiter}', unable to load!"
            )
            return
        if wrap:
            # If the data is wrapped...
            # Remove leading and trailing whitespace from depth lines;
            # lines with only one value.
            pattern = r'^\s*(\d+\.\d+)\s*$'
            processed_data = re.sub(pattern, r'\1', self.raw_data, flags=re.M)

            # Remove trailing whitespace from all lines
            processed_data = re.sub(r'\s*$', '', processed_data, flags=re.M)

            # Replace the newline characters before each depth value
            # with a unique separator
            processed_data = re.sub(r'\n(?=\d)', ' | ', processed_data)

            # Replace all newline characters with space
            processed_data = processed_data.replace('\n', ' ')

            # Split the data into records using the unique separator
            records = processed_data.split(' | ')

            # Convert the list of records into a single string with each
            # record on a separate line
            records_str = '\n'.join(records)

            # Create a file-like object from the string
            with StringIO(records_str) as f:
                # Catch any warnings that occur when reading the data
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    # Use numpy's genfromtxt to read the data into a
                    # numpy array
                    self.data = genfromtxt(
                        f,
                        delimiter=delim,

                        invalid_raise=invalid_raise
                    )
                    # Convert the numpy array to a pandas DataFrame
                    self.df = DataFrame(self.data)
                    for warn in w:
                        if issubclass(warn.category, UserWarning):
                            # Store any read errors
                            self.read_errors = [
                                err
                                for err
                                in str(warn.message).split("\n")
                            ]
                            return
        # If the data is not wrapped...
        else:
            # Create a file-like object from the string
            with StringIO(self.raw_data) as f:
                # Catch any warnings that occur when reading the data
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    # Use numpy's genfromtxt to read the data into a
                    # numpy array
                    if delim == ' ':
                        self.data = genfromtxt(
                            f,
                            invalid_raise=invalid_raise
                        )
                    elif delim == ',':
                        csv_data = reader(f, delimiter=delim)
                        csv_data = list(csv_data)
                        self.data = array(csv_data)
                    else:
                        self.data = genfromtxt(
                            f,
                            delimiter=delim,
                            invalid_raise=invalid_raise
                        )
                    # Convert the numpy array to a pandas DataFrame
                    self.df = DataFrame(self.data)
                    for warn in w:
                        if issubclass(warn.category, UserWarning):
                            # Store any read errors
                            self.read_errors = [
                                err
                                for err
                                in str(warn.message).split("\n")
                            ]
                            return


class LASSection():
    """
    Class representing a section of Log ASCII
    Standard (LAS) data.

    This class provides methods and properties
    for parsing, validating and handling a LAS section.

    Attributes:
    ----------
    name : str
        The name of the section.

    raw_data : str
        The raw data of the section as a string.

    type : str
        The type of the section, either 'header', 'data', or 'other'.

    version_num : str
        The version number of the LAS file the section belongs to.

    assoc : str, optional
        The association of the section.
        Defaults to None.

    delimiter : str, optional
        The delimiter used in the section.
        Defaults to None.

    parsed_section : obj, optional
        The parsed section. This is populated when the section is
        parsed.

    parsed : bool, optional
        Indicates whether the section has been parsed.
        Defaults to False.

    validated : bool, optional
        Indicates whether the section has been validated.
        Defaults to False.

    df : pandas.DataFrame
        The DataFrame representing the parsed data of the section.

    Methods:
    -------
    __init__(self, name, raw_data, section_type, version_num,
    assoc=None, delimiter=None, parse_on_init=True,
    validate_on_init=True)
        Initializes the LASSection object, parses the raw data,
        validates the parsed section if parse_on_init and
        validate_on_init are set to True.

    __repr__(self)
        Returns a string that represents the LASSection object in a way
        that can be used to recreate the object.

    __str__(self)
        Returns a user-friendly string representation of the LASSection
        object.

    parse(self)
        Parses the raw data into a usable format (not implemented in
        this code snippet).

    validate(self)
        Validates the parsed section (not implemented in this code
        snippet).

    """
    def __init__(
        self,
        name,
        raw_data,
        section_type,
        version_num,
        wrap,
        delimiter=None,
        assoc=None,
        parse_on_init=True,
        validate_on_init=True
    ):
        # initialize attributes
        self.name = name
        self.raw_data = raw_data
        self.type = section_type
        self.version_num = version_num
        self.assocation = assoc
        self.delimiter = delimiter
        self.validated = False
        self.wrap = wrap
        # parse section
        if parse_on_init:
            self.parse_errors = []
            self.parse_tbs = []
            try:
                self.parse()
            except Exception as e:
                self.parse_errors.append(
                    LASFileError(
                        f"Couldn't parse section {self.name}: {str(e)}"
                    )
                )
                self.parse_tbs.append(traceback.format_exc())
                return
            if self.parse_errors == []:
                del self.parse_errors
            if self.parse_tbs == []:
                del self.parse_tbs
        # validate section
        if (
            parse_on_init and validate_on_init
        ):
            self.validate_errors = []
            self.validate_tbs = []
            # If there are no parse errors, proceed normally with validation
            if not hasattr(self, 'parse_errors') or self.parse_errors == []:
                try:
                    self.validate()
                    if self.validate_errors != []:
                        self.validated = False
                    else:
                        self.validated = True
                except Exception as e:
                    self.validated = False
                    self.validate_errors.append(e)
                    self.validate_tbs.append(traceback.format_exc())
            # If there are parse errors...
            else:
                # If there are critical parse errors, return a critical
                # validation error
                if any(
                    isinstance(error, LASFileCriticalError)
                    for error in getattr(self, 'parse_errors')
                ):
                    self.validate_errors.append(
                        LASFileCriticalError(
                            "Couldn't validate section due to critical parse "
                            "errors."
                        )
                    )
                # If there are no critical parse errors...
                else:
                    # attempt validation
                    try:
                        self.validate()
                        if self.validate_errors != []:
                            self.validated = False
                        else:
                            self.validated = True
                    except Exception as e:
                        self.validate_errors.append(e)
                        self.validate_tbs.append(traceback.format_exc())
            if self.validate_errors == []:
                del self.validate_errors
            if self.validate_tbs == []:
                del self.validate_tbs

    def parse(self):
        """
        Parses the raw data of the section into a usable format.

        This method parses the raw data of the section into a usable
        format. The specific parsing steps differ based on the version
        number and the type of the section.

        Parameters:
        ----------
        None

        Returns:
        -------
        None
        """
        if '\n' in self.raw_data:
            # parse title line
            title_line_end = self.raw_data.index('\n')
            title_line = self.raw_data[:title_line_end].strip()
            if '|' in title_line:
                result = parse_title_line(
                    title_line,
                    version_num=self.version_num,
                    all_lowercase=True,
                    assocs=True
                )
                if type(result) != str and result is not None:
                    self.association = result[1]
                else:
                    self.association = None
            else:
                self.association = None
            # if the section is a header section, parse it as such
            if self.type.lower() == 'header':
                try:
                    self.parsed_section = parse_header_section(
                        self.raw_data,
                        version_num=self.version_num
                    )
                    self.df = self.parsed_section
                    # Test if there are any errors in the parsed section
                    # by check if the only value in the errors column is
                    # None
                    if 'errors' in self.df.columns:
                        if self.df['errors'].unique().tolist() == [None]:
                            # If there are no errors remove the errors column
                            self.df = self.df.drop(columns=['errors'])
                        else:
                            for error in self.df['errors']:
                                if error is not None:
                                    self.parse_errors.append(
                                        error
                                    )
                except Exception as e:
                    # if it is a required section, return a critical error
                    # otherwise return a minor error
                    if (
                        self.name.lower() in
                        required_sections[self.version_num]
                    ):
                        self.parse_errors.append(
                            RequiredSectionParseError(
                                f"Couldn't parse '{self.name}' data: {str(e)}"
                            )
                        )
                        self.parse_tbs.append(traceback.format_exc())
                        return
                    else:
                        self.parse_errors.append(
                            SectionParseError(
                                f"Couldn't parse '{self.name}' data: {str(e)}"
                            )
                        )
                        self.parse_tbs.append(traceback.format_exc())
                        return
            # if the section is a data section, parse it as such
            elif self.type.lower() == 'data':
                try:
                    # print(f"section name: {name}")
                    self.parsed_section = parse_data_section(
                        self.raw_data,
                        version_num=self.version_num,
                        wrap=self.wrap,
                        delimiter=self.delimiter
                    )
                    self.df = self.parsed_section.df
                    # Test if there are any errors in the parsed section
                    # by check if the only value in the errors column is
                    # None
                    if 'errors' in self.df.columns:
                        if (
                            getattr(
                                self, 'df'
                            )['errors'].unique().tolist() == [None]
                        ):
                            # If there are no errors remove the errors column
                            self.df = self.df.drop(columns=['errors'])
                        else:
                            for error in self.df['errors']:
                                self.parse_errors.append(
                                    error
                                )
                except Exception as e:
                    # if it is a required section, return a critical error
                    # otherwise return a minor error
                    if (
                        self.name.lower() in
                        required_sections[self.version_num]
                    ):
                        self.parse_errors.append(
                            RequiredSectionParseError(
                                f"Couldn't parse '{self.name}' data: {str(e)}"
                            )
                        )
                        self.parse_tbs.append(traceback.format_exc())
                        return
                    else:
                        self.parse_errors.append(
                            SectionParseError(
                                f"Couldn't parse '{self.name}' data: {str(e)}"
                            )
                        )
                        self.parse_tbs.append(traceback.format_exc())
                        return
            # parse other sections
            else:
                try:
                    self.parsed_section = parse_header_section(
                        self.raw_data,
                        version_num=self.version_num
                        )
                    self.df = self.parsed_section
                    # Test if there are any errors in the parsed section
                    if 'errors' in self.df.columns:
                        if self.df['errors'].unique().tolist() == [None]:
                            # If there are no errors remove the errors column
                            self.df = self.df.drop(columns=['errors'])
                            self.type = 'header'
                        else:
                            raise Exception('Not a header section')
                except Exception:
                    try:
                        self.parsed_section = parse_data_section(
                            self.raw_data,
                            version_num=self.version_num,
                            wrap=self.wrap,
                            delimiter=self.delimiter
                        )
                        self.df = self.parsed_section.df
                        self.type = 'data'
                    except Exception as e:
                        # Test if it is a required section or not and
                        # return the appropriate error
                        if (
                            self.name.lower() in
                            required_sections[self.version_num]
                        ):
                            self.parse_errors.append(
                                RequiredSectionParseError(
                                    f"Couldn't parse '{self.name}' data: "
                                    f"{str(e)}"
                                )
                            )
                            self.parse_tbs.append(traceback.format_exc())
                        else:
                            self.parse_errors.append(
                                SectionParseError(
                                    f"Couldn't parse '{self.name}' data: "
                                    f"{str(e)}"
                                )
                            )
                            self.parse_tbs.append(traceback.format_exc())
                        return
        # if raw data is only one line or less
        else:
            # Test if it is a critical required section or not and
            # return the appropriate error
            if self.name.lower() in required_sections[self.version_num]:
                self.parse_errors.append(
                    RequiredSectionParseError(
                        "Couln't parse, raw section data is only one line or "
                        "less."
                    )
                )
                self.parse_tbs.append(traceback.format_exc())
            else:
                self.parse_errors.append(
                    SectionParseError(
                        "Couln't parse, raw section data is only one line or "
                        "less."
                    )
                )
                self.parse_tbs.append(traceback.format_exc())

    def validate(self):
        """
        Validates parsed sections from a LAS file depending on the section
        name, type and the file's version.

        This function checks the validity of parsed sections from a LAS
        file based on their name and type. It calls specific validation
        functions (validate_version, validate_well) for version and well
        headers and checks for read_errors attribute in data section.

        Parameters:
        ----------
        None

        Returns:
        -------
        None
        """

        if self.name == 'version' and self.type == 'header':
            errors = validate_version(self.parsed_section, self.version_num)
            if errors != []:
                for error in errors:
                    self.validate_errors.append(error)
        elif self.name == 'well' and self.type == 'header':
            errors = validate_well(self.parsed_section, self.version_num)
            if errors != []:
                for error in errors:
                    self.validate_errors.append(error)
        elif self.name == 'curves' and self.type == 'header':
            errors = validate_curves(self.parsed_section, self.version_num)
            if errors != []:
                for error in errors:
                    self.validate_errors.append(error)
        elif self.name == 'data' and self.type == 'data':
            if hasattr(self.parsed_section, 'read_errors'):
                for error in self.parsed_section.read_errors:
                    self.validate_errors.append(error)
        elif '_definition' in self.name and self.type == 'header':
            errors = validate_curves(self.parsed_section, self.version_num)
            if errors != []:
                for error in errors:
                    self.validate_errors.append(error)
        elif '_data' in self.name and self.type == 'data':
            if hasattr(self.parsed_section, 'read_errors'):
                for error in self.parsed_section.read_errors:
                    self.validate_errors.append(error)

    def __repr__(self):
        return (
            f"<LASSection(name={self.name!r}, type={self.type!r}, "
            f"version_num={self.version_num!r})>"
        )

    def __str__(self):
        s = (
            "LASSection\n"
            f"    Name: {self.name}\n"
            f"    Type: {self.type}\n"
            f"    Version: {self.version_num}\n"
            f"    Delimiter: {self.delimiter}\n"
        )
        if not hasattr(self, 'parse_error'):
            s += "    Parsed: True\n"
        else:
            s += (
                "    Parsed: False\n"
                "    Errors:\n"
                f"        Parsing Error: {self.parse_errors}\n"
                f"        Traceback:\n{self.parse_tbs}\n"
            )
        if not hasattr(self, 'validate_errors'):
            s += f"    Validated: {self.validated}\n"
        else:
            s += (
                f"    Validated: {self.validated}\n"
                "    Errors:\n"
                f"        Validation Error: {self.validate_errors}\n"
                f"        Traceback:\n{self.validate_tb}\n"
            )
        if hasattr(self, 'df'):
            s += f"    Rows: {len(self.df)}\n"

        return s

    def add_validate_errors(self, error, tb=None):
        if not hasattr(self, 'validate_errors'):
            self.validate_errors = []
        if not hasattr(self, 'validate_tb'):
            self.validate_tb = []
        self.validate_errors.append(error)
        if tb is not None:
            self.validate_tb.append(tb)


# Check for definition/curve and data column congruency
def check_definitions_and_format_data(def_section, data_section):
    """
    Checks the congruency between the definitions (header) and data
    sections of a LAS file and formats the data section accordingly.

    Parameters:
    -----------
    def_section : LASSection
        An instance of LASSection representing the definition section
        (usually the header) of a LAS file.

    data_section : LASSection
        An instance of LASSection representing the data section of a
        LAS file.

    Returns:
    --------
    None

    Note:
    ----
    If the number of rows in the definition section matches the number
    of columns in the data section, it renames the columns of the data
    section with the column names of the definition section. This
    operation modifies the data_section in-place.
    """
    def_rows = def_section.df.shape[0]
    # print(def_rows)
    data_cols = data_section.df.shape[1]
    # print(data_cols)
    if hasattr(def_section, 'df') and hasattr(data_section, 'df'):
        def_rows = def_section.df.shape[0]
        # print(def_rows)
        data_cols = data_section.df.shape[1]
        # print(data_cols)
        if def_rows == data_cols:
            data_section.df.rename(
                columns=dict(zip(
                        data_section.df.columns,
                        def_section.df.columns
                )),
                inplace=True
            )


class LASFile():
    """
    Class representing a Log ASCII Standard (LAS) file.

    This class provides methods for reading an LAS file, parsing its
    sections, validating those sections, and handling errors that occur
    during these processes.

    Attributes:
    ----------
    file_path : str
        The path of the LAS file.

    always_try_split : bool
        Whether or not to try to split the sections of the LAS file even
        when errors occur.

    sections : list of LASSection
        The sections of the LAS file, parsed and validated.

    version : LASSection
        The version section of the LAS file.

    version_num : str
        The version number of the LAS file.

    wrap : str
        The wrap of the LAS file.

    delimiter : str
        The delimiter used in the LAS file.

    read_error : str
        Error encountered during reading the file, if any.

    read_tb : str
        Traceback information for the read error, if any.

    open_error : str
        Error encountered during opening the file, if any.

    open_tb : str
        Traceback information for the open error, if any.

    version_error : str
        Error encountered during extraction of the version section,
        if any.

    version_tb : str
        Traceback information for the version error, if any.

    split_error : str
        Error encountered during splitting of the sections, if any.

    parse_error : dict
        Errors encountered during parsing of the sections, if any.

    validate_errors : dict
        Errors encountered during validation of the sections, if any.

    errors : dict
        All errors encountered during the processing of the LAS file.

    Methods:
    -------
    __init__(self, file_path=None, always_try_split=False)
        Initializes the LASFile object, reads the file, parses and validates
        its sections.

    read_file(self, file_path)
        Attempts to read the LAS file and handle any errors that occur during
        this process.

    get_version(self, data)
        Attempts to extract the version, wrap, and delimiter from the LAS file
        data.

    get_sections(self, data)
        Attempts to split the LAS file data into sections.

    parse_and_validate_sections(self, sections_dict)
        Attempts to parse and validate the sections of the LAS file.

    ensure_curve_and_data_congruency(self)
        Checks if the definition/curve and data columns of the LAS file are
        congruent.

    __str__(self)
        Returns a user-friendly string representation of the LASFile object,
        including any errors that occurred.
    """
    def __init__(self, file_path=None):
        if file_path is not None:
            self.file_path = file_path
            self.sections = []
            # Try to open and read the file into a string
            data = self.read_file(self.file_path)

            if data is not None:
                # If it read correctly try to extract the version,
                # wrap, and delimiter and append it to the sections
                self.get_version(data)

                sections_dict = self.get_sections(data)

                self.parse_and_validate_sections(sections_dict)

            self.set_error_attributes()

    def read_file(self, file_path):
        # Try to open the file
        try:
            with open(self.file_path, 'r') as f:
                try:
                    data = f.read()
                    return data
                except Exception as e:
                    self.read_error = LASFileCriticalError(
                        f"Couldn't read file: {str(e)}"
                    )
                    tb = traceback.format_exc()
                    self.read_tb = f"Couldn't read file: {tb}"
                    return
        except FileNotFoundError:
            self.open_error = LASFileOpenError(
                f"File not found: {self.file_path}"
            )
            tb = traceback.format_exc()
            self.open_tb = f"File not found: {tb}"
            return
        except Exception as e:
            self.open_error = LASFileOpenError(
                f"Error opening file: {str(e)}"
            )
            tb = traceback.format_exc()
            self.open_tb = f"Error opening file: {tb}"
            return

    def get_version(self, data):
        # If it read correctly try to extract the version,
        # wrap, and delimiter and append it to the sections
        try:
            self.version = get_version_section(data)
            self.version_num = self.version.version_num
            self.wrap = self.version.wrap
            self.delimiter = self.version.delimiter
            self.sections.append(self.version)
            return
        # If a version couldn't be extracted, set the version error and
        # traceback, and return
        except Exception as e:
            self.version_error = LASFileCriticalError(
                f"Couldn't get version: {str(e)}"
            )
            tb = traceback.format_exc()
            self.version_tb = LASFileCriticalError(
                f"Couldn't get version: {tb}"
            )
            self.version_num = None
            return

    def get_sections(self, data):
        # Try to split the file into sections
        # Test if a version number was extracted and associated with
        # the LASFile object
        if hasattr(self, 'version_num'):
            # If a version number is associated with the LASFile object,
            # test if it is not None or an empty string
            if (
                getattr(self, 'version_num') is not None and
                getattr(self, 'version_num') != ''
            ):
                # If the version number is not None or an empty string,
                # try to split the file into sections
                try:
                    s = split_sections(data, self.version_num)
                except Exception as e:
                    # If an error occurs during the splitting of the
                    # sections, set the split error and traceback, and
                    # return
                    self.split_error = LASFileSplitError(
                        f"Couldn't split into sections: {str(e)}"
                    )
                    self.split_tb = traceback.format_exc()
                    return
                # If the sections were split correctly, check that the
                # minimum required sections are present and not empty
                # then return the sections dictionary
                if (
                    'version' in s.keys() and
                    'well' in s.keys() and
                    'curves' in s.keys() and
                    'data' in s.keys()
                ):
                    if (
                        s['version'] != '' and s['version'] is not None and
                        s['well'] != '' and s['well'] is not None and
                        s['curves'] != '' and s['curves'] is not None and
                        s['data'] != '' and s['data'] is not None
                    ):
                        return s
                    else:
                        self.split_error = LASFileSplitError(
                            "Couldn't split into minimum required "
                            "sections: Version, Well, Curves or Data "
                            "sections is empty."
                        )
                        return
                else:
                    self.split_error = LASFileSplitError(
                        "Couldn't split into minimum required "
                        "sections: Version, Well, Curves or Data "
                        "sections is missing."
                    )
                    return
        else:
            self.split_error = LASFileSplitError(
                "Couldn't split into minimum required "
                "sections: Version Number is missing."
            )
            return

    def parse_and_validate_sections(self, sections_dict):
        # If it split correctly and therefore the sections_dict is not
        # None, then generate las section items from the strings
        if sections_dict is not None:
            # Get the section name and raw text data from sections_dict
            for name, raw_data in sections_dict.items():
                # Skip the version section, it should already be parsed
                if name == 'version':
                    continue
                # Get the section type
                if (
                    name in header_section_names or
                    '_parameters' in name or
                    '_definition' in name
                ):
                    section_type = 'header'
                elif (
                    name in data_section_names or
                    '_data' in name
                ):
                    section_type = 'data'
                else:
                    section_type = ''
                # Try to create the section
                try:
                    section = LASSection(
                        name,
                        raw_data,
                        section_type,
                        self.version_num,
                        self.wrap,
                        delimiter=self.delimiter
                    )
                    self.sections.append(section)
                except Exception as e:
                    # Try to create the section anyway without parsing
                    # or validating
                    try:
                        section = LASSection(
                            name,
                            raw_data,
                            section_type,
                            self.version_num,
                            self.wrap,
                            delimiter=self.delimiter,
                            parse_on_init=False,
                            validate_on_init=False
                        )
                        section.parse_errors = [e]
                        self.sections.append(section)
                    except Exception as e:
                        if hasattr(self, 'parse_errors'):
                            self.parse_errors[name] = e
                        else:
                            self.parse_errors = {name: e}

            # Set LASFile section attributes
            for section in self.sections:
                # Set each section as an attribute of the LASFile
                setattr(self, section.name, section)

        # Run the function to ensure the curve and data sections
        # have the same number of curve mnemonics/data columns
        self.ensure_curve_and_data_congruency()

        # Aggregate parse_errors and validate_errors from all sections
        self.aggregate_section_errors()
        return

    def ensure_curve_and_data_congruency(self):
        """Check for definition/curve and data column congruency"""
        # Check that the curves and data sections exist
        if hasattr(self, "curves") and hasattr(self, "data"):
            # Check that the curves and data sections were parsed
            # correctly into dataframes
            if (
                hasattr(getattr(self, "curves"), 'df') and
                hasattr(getattr(self, "data"), 'df')
            ):
                # Get the number of rows/curve definitions and the
                # number of columns/data points
                def_rows = getattr(self, "curves").df.shape[0]
                curves_df = getattr(self, "curves").df
                data_cols = getattr(self, "data").df.shape[1]
                # If the number of rows/curve definitions in the
                # definition section matches the number of columns in
                # the data section, rename the columns of the data
                # section to the curve mnemonics
                if def_rows == data_cols:
                    # Check if there are repeated curve mnemonics
                    if (
                        not len(
                            curves_df.mnemonic.unique()
                        ) == len(
                            curves_df.mnemonic
                        )
                    ):
                        # Get a list of the mnemonics that are repeated
                        repeated_mnemonics = curves_df.mnemonic[
                            curves_df.mnemonic.duplicated(keep=False)
                        ].unique()
                        # For each unique repeated mnemonic make a df of
                        # the repeated mnemonics
                        for mnemonic in repeated_mnemonics:
                            # Get a df of the repeated mnemonics
                            repeated_mnemonics_df = (
                                curves_df.loc[
                                    curves_df['mnemonic'] == mnemonic
                                ]
                            )
                            # Reset the index of the repeated mnemonics df
                            repeated_mnemonics_df.reset_index(inplace=True)

                            # for each row in the repeated mnemonics df,
                            # append an underscore and the index digit
                            # to the end of the mnemonic, except the
                            # first instance of the repeated mnemonic
                            # format: {old_index: [new_mnemonic]}
                            new_repeated_mnemonics = {}
                            for index, row in repeated_mnemonics_df.iterrows():
                                if index == 0:
                                    new_repeated_mnemonics[row['index']] = (
                                        row['mnemonic']
                                    )
                                else:
                                    new_repeated_mnemonics[row['index']] = (
                                        f"{row['mnemonic']}_{index}"
                                    )

                            # Create a copy of the curves df
                            new_curves_df = curves_df.copy()
                            # replace the old mnemonics with the new
                            # ones by index
                            for index, new_mnemonic in \
                                    new_repeated_mnemonics.items():
                                new_curves_df.loc[index, 'mnemonic'] = (
                                    new_mnemonic
                                )
                            # Replace the curves df with the new one
                            setattr(
                                getattr(self, 'curves'),
                                'df',
                                new_curves_df)
                            # Rename the columns of the data section
                            getattr(self, "data").df.rename(
                                columns=dict(zip(
                                    getattr(self, "data").df.columns,
                                    new_curves_df.mnemonic.values
                                )),
                                inplace=True
                            )
                    else:
                        # Rename the columns of the data section
                        getattr(self, "data").df.rename(
                            columns=dict(zip(
                                getattr(self, "data").df.columns,
                                getattr(self, "curves").df.mnemonic.values
                            )),
                            inplace=True
                        )
                # If the number of rows/curve definitions in the
                # definition section does not match the number of
                # columns in the data section, set a validation error
                else:
                    getattr(self, 'curves').validate_errors.append(
                        LASFileCriticalError(
                            "Curves and data sections are not "
                            "congruent."
                        )
                    )
                    getattr(self, 'data').validate_errors.append(
                        LASFileCriticalError(
                            "Curves and data sections are not "
                            "congruent."
                        )
                    )

    def aggregate_section_errors(self):
        if hasattr(self, 'sections'):
            for section in self.sections:
                # Aggregate parse_errors
                # If the current section has a parse error
                if hasattr(section, 'parse_errors'):
                    # Instantiate the parse_errors attribute if it
                    # doesn't exist
                    if not hasattr(self, 'parse_errors'):
                        setattr(self, 'parse_errors', {})
                    # Add the current parse error for the current
                    # section to the LASFile parse_errors dictionary
                    self.parse_errors[section.name] = (
                            section.parse_errors
                        )

                # Aggregate validate_errors
                # If the current section has validate errors
                if hasattr(section, 'validate_errors'):
                    # Instantiate the validate_errors attribute if it
                    # doesn't exist
                    if not hasattr(self, 'validate_errors'):
                        setattr(self, 'validate_errors', {})
                    # Add the current validate error for the current
                    # section to the LASFile validate_errors dictionary
                    getattr(self, 'validate_errors')[section.name] = (
                            section.validate_errors
                        )

    def set_error_attributes(self):
        # Aggregate all the errors into one dictionary and store it
        # in lasfile.errors
        self.errors = {}
        if hasattr(self, 'open_error'):
            self.errors['open_error'] = getattr(self, 'open_error')
        if hasattr(self, 'read_error'):
            self.errors['read_error'] = getattr(self, 'read_error')
        if hasattr(self, 'split_error'):
            self.errors['split_error'] = getattr(self, 'split_error')
        if hasattr(self, 'version_error'):
            self.errors['version_error'] = getattr(self, 'version_error')
        if hasattr(self, 'parse_errors'):
            self.errors['parse_errors'] = getattr(self, 'parse_errors')
        if hasattr(self, 'validate_errors'):
            self.errors['validate_errors'] = getattr(self, 'validate_errors')

    def __str__(self):
        s = f"LASFile: {self.file_path}\n"
        if hasattr(self, 'open_error'):
            s += f"Open Error: {self.open_error}\n"
        if hasattr(self, 'read_error'):
            s += f"Reading Error: {self.read_error}\n"
        if hasattr(self, 'version_error'):
            s += f"Version Extraction Error: {self.version_error}\n"
        if hasattr(self, 'split_error'):
            s += f"Section Splitting Error: {self.split_error}\n"
        if hasattr(self, 'parse_errors'):
            s += f"Parsing Error: {self.parse_errors}\n"
        if hasattr(self, 'validate_errors'):
            s += f"Validation Error: {getattr(self, 'validate_errors')}"
        if hasattr(self, 'sections'):
            for section in self.sections:
                s += str(f"  {section}")
        return s


def read(fp):
    """
    Read a LAS file and return a LASFile object

    Parameters
    ----------
    las_file : str
        Path to the LAS file

    Returns
    -------
    LASFile object
    """
    return LASFile(file_path=fp)


def api_from_las(input):
    # If the input is a string, assume it's a file path and try to read
    # it into a LASFile object
    if isinstance(input, str):
        try:
            las = read(input)
        except Exception as e:
            raise e
    elif isinstance(input, LASFile):
        las = input
    else:
        las = LASFile()
    # If the las has a well section, try to get the api from it
    if hasattr(las, 'well'):
        try:
            # Check if 'UWI', 'uwi', 'API', or 'api' is present in the
            # 'mnemonic' column
            mask = getattr(las, "well").df['mnemonic'].str.lower().isin(
                ['uwi', 'api']
            )
            # Filter the DataFrame using the mask
            filtered_df = getattr(las, "well").df[mask]
            # Get the corresponding values for the matched mnemonics
            matched_values = filtered_df['value'].tolist()

            # Attempt to load all matched values into an APINumber
            # objects
            valid_values = []
            for value in matched_values:
                try:
                    APINumber(value)
                    valid_values.append(value)
                except Exception:
                    pass

            # Remove Null values from valid_values
            valid_values = [x for x in valid_values if x is not None]

            # If there are matched values, check if they have the same
            # first 10 characters
            if len(valid_values) > 0:
                if all(
                    APINumber(x).unformatted_10_digit == APINumber(
                        valid_values[0]
                        ).unformatted_10_digit
                    for x in valid_values
                ):
                    return APINumber(valid_values[0])
                else:
                    # If they don't have the same first 10 characters,
                    # return the longest valid_value as an APINumber
                    return APINumber(max(valid_values, key=len))
            else:
                return None
        except Exception as e:
            raise e
    else:
        return None


def error_check(las, critical_only=True):
    """
    Check if a LASFile object has any errors

    Parameters
    ----------
    las : LASFile object
        The LASFile object to check for errors
    critical_only : bool
        If True, only check for critical errors, otherwise check for all
        errors

    Returns
    -------
    bool
        True if no errors are found, False if any errors are found
    """
    # Check if an las object, either LASSection or LASFile, has any
    # errors and if critical_only is True, if any are of type
    # LASFileCriticalError return False when one is found,
    # otherwise return False when any error is found.
    if critical_only:
        # Check if the las object has an open_error
        if hasattr(las, 'open_error'):
            if isinstance(las.open_error, LASFileCriticalError):
                return False
        # Check if the las object has a read_error
        if hasattr(las, 'read_error'):
            if type(las.read_error) == dict:
                for sec_name, error in las.read_error.items():
                    if isinstance(error, LASFileCriticalError):
                        return False
            elif type(las.read_error) == Exception:
                if isinstance(las.read_error, LASFileCriticalError):
                    return False
        # Check if the las object has a version_error
        if hasattr(las, 'version_error'):
            if type(las.version_error) == dict:
                for sec_name, error in las.version_error.items():
                    if isinstance(error, LASFileCriticalError):
                        return False
            elif type(las.version_error) == Exception:
                if isinstance(las.version_error, LASFileCriticalError):
                    return False
        # Check if the las object has a split_error
        if hasattr(las, 'split_error'):
            if isinstance(las.split_error, LASFileCriticalError):
                return False
        # Check if the las object has a parse_errors
        if hasattr(las, 'parse_errors'):
            if type(las.parse_errors) == dict:
                for sec_name, error in las.parse_errors.items():
                    if isinstance(error, LASFileCriticalError):
                        return False
            elif type(las.parse_errors) == Exception:
                if isinstance(las.parse_errors, LASFileCriticalError):
                    return False
        # Check if the las object has a validate_errors
        if hasattr(las, 'validate_errors'):
            if type(las.validate_errors) == dict:
                for sec_name, error_list in las.validate_errors.items():
                    for error in error_list:
                        if isinstance(error, LASFileCriticalError):
                            return False
            if type(las.validate_errors) == list:
                for error in las.validate_errors:
                    if isinstance(error, LASFileCriticalError):
                        return False
            if type(las.validate_errors) == Exception:
                if isinstance(las.validate_errors, LASFileCriticalError):
                    return False
        # If no critical errors are found, return True
        return True
    else:
        # Check if the las object has any errors
        if hasattr(las, 'open_error'):
            return False
        if hasattr(las, 'read_error'):
            return False
        if hasattr(las, 'version_error'):
            return False
        if hasattr(las, 'split_error'):
            return False
        if hasattr(las, 'parse_errors'):
            return False
        if hasattr(las, 'validate_errors'):
            return False
        return True
