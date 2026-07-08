


import { GraphQLClient } from 'graphql-request';
import {
  USE_MOCK_DATA,
  mockEstadisticasPorOrgano,
  mockEstadisticasPorTipoEntidad,
  mockConcentracion
} from './mockData.js';

// Configurar el cliente GraphQL - Compose URL from environment variables
const API_URL = `${import.meta.env.VITE_GRAPHQL_PROTOCOL}://${import.meta.env.VITE_GRAPHQL_HOST}:${import.meta.env.VITE_GRAPHQL_PORT}${import.meta.env.VITE_GRAPHQL_PATH}`;

const client = new GraphQLClient(API_URL, {
  headers: {
    'Content-Type': 'application/json',
  },
});

// Queries para obtener datos de subvenciones
export const queries = {
  // Obtener estadísticas por región (concedente)
  estadisticasPorRegionConcedente: `
    query {
      estadisticas_por_organo {
        organo_nombre
        anio
        numero_concesiones
        importe_total
      }
    }
  `,

  // Obtener estadísticas por tipo de entidad (beneficiario)
  estadisticasPorTipoEntidad: `
    query {
      estadisticas_por_tipo_entidad {
        tipo_entidad
        anio
        numero_concesiones
        importe_total
      }
    }
  `,

  // Obtener concesiones con filtros
  concesiones: `
    query getConcesiones($filtros: ConcesionInput, $limite: Int, $offset: Int) {
      concesiones(filtros: $filtros, limite: $limite, offset: $offset) {
        id
        codigo_bdns
        convocatoria {
          id
          codigo_bdns
          titulo
          organo {
            id
            nombre
          }
        }
        beneficiario {
          id
          identificador
          nombre
          tipo
        }
        fecha_concesion
        importe
        descripcion_proyecto
        tipo_ayuda
        anio
      }
    }
  `,

  // Obtener beneficiarios
  beneficiarios: `
    query getBeneficiarios($filtros: BeneficiarioInput, $limite: Int, $offset: Int) {
      beneficiarios(filtros: $filtros, limite: $limite, offset: $offset) {
        id
        identificador
        nombre
        tipo
      }
    }
  `,

  // Obtener concentración de subvenciones
  concentracion: `
    query getConcentracion($anio: Int, $tipo_entidad: String, $limite: Int) {
      concentracion_subvenciones(anio: $anio, tipo_entidad: $tipo_entidad, limite: $limite) {
        beneficiario_nombre
        numero_concesiones
        importe_total
      }
    }
  `,

  // Obtener concesión por ID
  concesionPorId: `
    query getConcesionById($id: ID!) {
      concesion(id: $id) {
        id
        codigo_bdns
        convocatoria {
          id
          codigo_bdns
          titulo
          organo {
            id
            nombre
            codigo
          }
        }
        organo {
          id
          nombre
          codigo
        }
        beneficiario {
          id
          identificador
          nombre
          tipo
        }
        fecha_concesion
        importe
        descripcion_proyecto
        programa_presupuestario
        tipo_ayuda
        anio
      }
    }
  `,

  // Obtener concesiones por beneficiario
  concesionesPorBeneficiario: `
    query getConcesionesPorBeneficiario($beneficiario_id: ID!, $anio: Int, $limite: Int, $offset: Int) {
      concesiones_por_beneficiario(beneficiario_id: $beneficiario_id, anio: $anio, limite: $limite, offset: $offset) {
        id
        codigo_bdns
        convocatoria {
          id
          codigo_bdns
          titulo
          organo {
            id
            nombre
          }
        }
        beneficiario {
          id
          identificador
          nombre
          tipo
        }
        fecha_concesion
        importe
        descripcion_proyecto
        tipo_ayuda
        anio
      }
    }
  `,
};

// Funciones para hacer queries
export async function fetchEstadisticasPorOrgano() {
  // Retornar datos mock si el flag está activado
  if (USE_MOCK_DATA) {
    console.log('Using mock data for estadísticas por órgano');
    return mockEstadisticasPorOrgano;
  }

  try {
    const data = await client.request(queries.estadisticasPorRegionConcedente);
    return data.estadisticas_por_organo || [];
  } catch (error) {
    console.error('Error fetching estadísticas por órgano:', error);
    return [];
  }
}

export async function fetchEstadisticasPorTipoEntidad() {
  // Retornar datos mock si el flag está activado
  if (USE_MOCK_DATA) {
    console.log('Using mock data for estadísticas por tipo entidad');
    return mockEstadisticasPorTipoEntidad;
  }

  try {
    const data = await client.request(queries.estadisticasPorTipoEntidad);
    return data.estadisticas_por_tipo_entidad || [];
  } catch (error) {
    console.error('Error fetching estadísticas por tipo entidad:', error);
    return [];
  }
}

export async function fetchConcesiones(filtros = {}, limite = 100, offset = 0) {
  try {
    const data = await client.request(queries.concesiones, {
      filtros: filtros || undefined,
      limite,
      offset,
    });
    return data.concesiones || [];
  } catch (error) {
    console.error('Error fetching concesiones:', error);
    return [];
  }
}

export async function fetchConcesionById(id) {
  try {
    const data = await client.request(queries.concesionPorId, { id });
    return data.concesion;
  } catch (error) {
    console.error('Error fetching concesión by ID:', error);
    return null;
  }
}

export async function fetchConcesionesPorBeneficiario(beneficiario_id, anio = null, limite = 100, offset = 0) {
  try {
    const data = await client.request(queries.concesionesPorBeneficiario, {
      beneficiario_id,
      anio,
      limite,
      offset,
    });
    return data.concesiones_por_beneficiario || [];
  } catch (error) {
    console.error('Error fetching concesiones por beneficiario:', error);
    return [];
  }
}

export async function fetchBeneficiarios(filtros = {}, limite = 100, offset = 0) {
  try {
    const data = await client.request(queries.beneficiarios, {
      filtros: filtros || undefined,
      limite,
      offset,
    });
    return data.beneficiarios || [];
  } catch (error) {
    console.error('Error fetching beneficiarios:', error);
    return [];
  }
}

export async function fetchConcentracion(anio = null, tipo_entidad = null, limite = 10) {
  // Retornar datos mock si el flag está activado
  if (USE_MOCK_DATA) {
    console.log('Using mock data for concentración');
    return mockConcentracion.slice(0, limite);
  }

  try {
    const data = await client.request(queries.concentracion, {
      anio,
      tipo_entidad,
      limite,
    });
    return data.concentracion_subvenciones || [];
  } catch (error) {
    console.error('Error fetching concentración:', error);
    return [];
  }
}

export default client;