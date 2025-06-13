import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime
import re
from etl.utils.nif_classifier import classify_nif

class BDNSTransformer:
    """Transformador de datos de la BDNS"""
    
    def transform_convocatorias(self, convocatorias: List[Dict[str, Any]]) -> pd.DataFrame:
        """Transformar datos de convocatorias"""
        if not convocatorias:
            return pd.DataFrame()
        
        # Convertir a DataFrame
        df = pd.DataFrame(convocatorias)
        
        # Limpiar y transformar campos
        if 'fecha_registro' in df.columns:
            df['fecha_registro'] = pd.to_datetime(df['fecha_registro']).dt.date
        if 'fecha_publicacion' in df.columns:
            df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion']).dt.date
        if 'fecha_inicio_solicitud' in df.columns:
            df['fecha_inicio_solicitud'] = pd.to_datetime(df['fecha_inicio_solicitud']).dt.date
        if 'fecha_fin_solicitud' in df.columns:
            df['fecha_fin_solicitud'] = pd.to_datetime(df['fecha_fin_solicitud']).dt.date
        
        # Extraer IDs de relaciones
        if 'organo' in df.columns and isinstance(df['organo'].iloc[0], dict):
            df['organo_id'] = df['organo'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
        
        if 'finalidad' in df.columns and isinstance(df['finalidad'].iloc[0], dict):
            df['finalidad_id'] = df['finalidad'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
        
        if 'region_impacto' in df.columns and isinstance(df['region_impacto'].iloc[0], dict):
            df['region_impacto_id'] = df['region_impacto'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
        
        return df
    
    def transform_concesiones(self, concesiones: List[Dict[str, Any]]) -> pd.DataFrame:
        """Transformar datos de concesiones"""
        if not concesiones:
            return pd.DataFrame()
        
        # Convertir a DataFrame
        df = pd.DataFrame(concesiones)
        
        # Limpiar y transformar campos
        if 'fecha_concesion' in df.columns:
            df['fecha_concesion'] = pd.to_datetime(df['fecha_concesion']).dt.date
            # Extraer año para particionamiento
            df['año'] = df['fecha_concesion'].apply(lambda x: x.year if x else None)
        
        # Extraer IDs de relaciones
        if 'convocatoria' in df.columns and isinstance(df['convocatoria'].iloc[0], dict):
            df['convocatoria_id'] = df['convocatoria'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
        
        if 'organo' in df.columns and isinstance(df['organo'].iloc[0], dict):
            df['organo_id'] = df['organo'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
        
        # Procesar beneficiarios
        if 'beneficiario_id' in df.columns and 'beneficiario_nombre' in df.columns:
            # Clasificar tipo de beneficiario por NIF
            df['tipo_beneficiario'] = df['beneficiario_id'].apply(classify_nif)
        
        return df
    
    def transform_beneficiarios(self, concesiones_df: pd.DataFrame) -> pd.DataFrame:
        """Extraer y transformar datos de beneficiarios desde concesiones"""
        if concesiones_df.empty:
            return pd.DataFrame()
        
        # Extraer datos de beneficiarios únicos
        beneficiarios = concesiones_df[['beneficiario_id', 'beneficiario_nombre', 'tipo_beneficiario']].copy()
        beneficiarios = beneficiarios.rename(columns={
            'beneficiario_id': 'identificador',
            'beneficiario_nombre': 'nombre',
            'tipo_beneficiario': 'tipo'
        })
        
        # Eliminar duplicados
        beneficiarios = beneficiarios.drop_duplicates(subset=['identificador'])
        
        return beneficiarios
