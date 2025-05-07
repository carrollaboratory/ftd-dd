from enum import Enum

"""
For now, we really only support strings, numbers and enumerations. However, 
there is a real need to provide support for dates, but that will require a 
format field. 
"""

class DataType(str, Enum):
    INT = 'integer'         # Whole Integer Values
    NUM = 'number'          # Floating Point Numbers
    STR = 'string'          # Strings
    ENUM = 'enumeration'    # Enumerated Values

datatype_lookup = {
    'int': DataType.INT, 
    'integer': DataType.INT,
    'number': DataType.NUM,
    'quantity': DataType.NUM,
    'numeric': DataType.NUM
}