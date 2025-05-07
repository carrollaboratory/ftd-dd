
import dd

class Enumeration:
    def __init__(self, name, description):
        self.name = name 
        self.description = description 

class Slot:
    def __init__(self, name, description):
        self.name = name 
        self.description = description 
        self.data_type = None       # dd.DataType

class DdTable:
    def __init__(self, name, description):
        self.name = name 
        self.description = description 
        self.slots = []

    def add_slot(self, name, description):
        self.slots.append(Slot(name, description))

