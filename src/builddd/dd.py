from enum import Enum
from pathlib import Path
import csv

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


class Enumeration:
    def __init__(self, name, description):
        self.name = name 
        self.description = description 

    def __repr__(self):
        if self.description is not None:
            return f"{self.name}={self.description}"
        return self.name

class DdVar:
    def __init__(self, name, description):
        self.name = name 
        self.description = description 
        self.data_type = None       # dd.DataType
        self.required = False 
        self.enumerations = []
        self.comment = ""
        self.units = ""

    def set_type(self, data_type): 
        match type(data_type).__name__:
            case "Text":
                self.data_type = DataType.STR 
            case "Enum":
                self.data_type = DataType.ENUM 
            case "Float":
                self.data_type = DataType.NUM 
            case "Integer":
                self.data_type = DataType.INT 
            case _:
                self.data_type = DataType.STR
        return self.data_type
    
    def add_enumeration(self, name, description):
        self.enumerations.append(Enumeration(name, description))

    def write_to_csv(self, writer, dd_format):
        enums = ""
        if len(self.enumerations) > 0:
            enums = ";".join([str(x) for x in self.enumerations])

        if dd_format==DataDictionaryFormat.MD:
            writer.writerow([
                self.name, 
                self.description,
                self.data_type, 
                "",
                "",
                self.units,
                enums,
                self.comment
            ])

class DataDictionaryFormat(str, Enum):
    MD = "Map Dragon Format"

class DdTable:
    _md_header = [
        "variable_name",
        "description",
        "data_type",
        "min",
        "max",
        "units",
        "enumerations",
        "comment"
    ]
    def __init__(self, name, description):
        self.name = name 
        self.description = description 
        self.variables = []
        self.sltlkup = {}

    def add_variable(self, name, description):
        slot = DdVar(name, description)
        self.variables.append(slot)
        self.sltlkup[name] = slot 
        return slot

    def set_datatype(self, varname, datatype):
        self.sltlkup[varname].data_type = datatype
    
    def set_required(self, varname):
        self.sltlkup[varname].required = True 

    def write_dd_header(self, writer, dd_format=DataDictionaryFormat.MD):
        if dd_format==DataDictionaryFormat.MD:
            writer.writerow(DdTable._md_header)

    def write_csv(self, outdir, dd_format=DataDictionaryFormat.MD):
        filename = Path(outdir) / f"{self.name}-dd.csv"
        with filename.open('wt') as f:
            writer = csv.writer(f)

            self.write_dd_header(writer, dd_format)
            for variable in self.variables:
                variable.write_to_csv(writer, dd_format)
        return filename

class DataDictionary:
    def __init__(self):
        """Collection of data-dictionary tables to be written as CSVs."""
    
        #self.ignored_classes = ignored_classes
        self.tables = {}
        
    def add_table(self, table_name, description):
        cls = DdTable(table_name, description)
        self.tables[cls.name] = cls 
        return cls

    def table(self, table_name):
        return self.tables.get(table_name)
    
    def write_csv(self, outputdir):
        Path(outputdir).mkdir(parents=True, exist_ok=True)
        filenames = []
        for tname, table in self.tables.items():
            filenames.append(str(table.write_csv(outputdir)))

        return "\n".join(filenames)
