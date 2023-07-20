# lasfile
 Library for reading CWLS Log ASCII Standard v. 1.2, 2.0, and 3.0 .las files.

## Installation
```bash
pip install lasfile
```

## Usage
### Read LAS file
```python
import lasfile

las = lasfile.LASFile(file_path='path/to/file.las')
```
#### In order for an lasfile to properly read, and pass a critical error check, it must have the following sections:
##### Version
##### Well
##### Curves
##### Data

### View sections in LAS file
```python
las.sections
```
### View section data in LAS file
#### Using dot notation
##### As raw ascii text
```python
las.well.raw_data
```
##### As pandas dataframe
```python
las.well.df
```
#### Using dictionary notation
##### As raw ascii text
```python
las['well']['raw_data']
```
##### As pandas dataframe
```python
las['well']['df']
```

### Errors
#### View all errors
```python
las.errors
```

#### View specific errors
```python
las.open_error
las.read_error
las.split_error
las.version_error
las.parse_error
las.validate_error
```

#### Check for errors
##### Check for only critical errors
```python
lasfile.error_check(las)
```
##### Check for all errors
```python
lasfile.error_check(las, critical_only=False)
```
##### LASSection objects can also be passed to error_check
```python
lasfile.error_check(las.well)
```


## License
[MIT](https://choosealicense.com/licenses/mit/)
```