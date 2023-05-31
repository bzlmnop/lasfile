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

## License
[MIT](https://choosealicense.com/licenses/mit/)
```