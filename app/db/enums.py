from enum import Enum  

class TipoOrgano(str, Enum):
    GEOGRAFICO = "G"
    AUTONOMICO = "A"
    LOCAL = "L"
    CENTRAL = "C"
    OTRO = "O"
    
   
    