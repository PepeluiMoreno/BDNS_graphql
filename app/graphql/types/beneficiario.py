import strawberry
from typing import Optional, List

@strawberry.type
class Beneficiario:
    id: strawberry.ID
    identificador: str
    nombre: str
    tipo: str

@strawberry.input
class BeneficiarioInput:
    identificador: Optional[str] = None
    tipo: Optional[str] = None
