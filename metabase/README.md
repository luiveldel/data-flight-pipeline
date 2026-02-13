# Metabase Dashboard Setup

Este directorio contiene scripts para configurar automáticamente dashboards en Metabase.

## Prerequisitos

1. **Metabase corriendo**: Asegúrate de que Metabase esté corriendo:
   ```bash
   make docker/up
   ```

2. **Metabase inicializado**: Accede a `http://localhost:3000` y completa el setup inicial:
   - Crea una cuenta de administrador
   - Salta la configuración de base de datos (el script la configura)

3. **Datos disponibles**: Ejecuta el pipeline de dbt para crear las tablas:
   ```bash
   make dbt/run
   ```

## Uso

### Opción 1: Usando Make

```bash
make metabase/setup-dashboard EMAIL=tu-email@example.com PASSWORD=tu-password
```

### Opción 2: Ejecutando el script directamente

```bash
uv run python metabase/setup_dashboard.py \
    --url http://localhost:3000 \
    --email tu-email@example.com \
    --password tu-password
```

### Opciones adicionales

```bash
# Si ya configuraste la base de datos manualmente:
uv run python metabase/setup_dashboard.py \
    --url http://localhost:3000 \
    --email admin@example.com \
    --password admin123 \
    --skip-db-setup \
    --database-name "Mi DuckDB"
```

## Dashboard Creado

El script crea un dashboard llamado **"Flight Analytics Dashboard"** con las siguientes visualizaciones:

### KPIs (métricas principales)
- **Total Flights**: Número total de vuelos
- **On-Time Rate (%)**: Porcentaje de vuelos a tiempo
- **Average Delay (min)**: Promedio de retraso en minutos

### Gráficos de tendencia
- **Total Flights by Date**: Vuelos por día (línea)
- **Daily Average Delay Trend**: Tendencia de retraso diario con media móvil de 7 días

### Análisis de retrasos
- **Delay Category Distribution**: Distribución de categorías de retraso (pie)
- **Average Delay by Time of Day**: Retraso promedio por momento del día

### Análisis de rutas y aerolíneas
- **Top 10 Routes by Delay Percentage**: Rutas con mayor porcentaje de retrasos
- **Flights by Airline**: Vuelos por aerolínea
- **Top 10 Departure Airports**: Aeropuertos con más salidas

### Análisis temporal
- **Flights by Departure Hour**: Distribución de vuelos por hora
- **Weekend vs Weekday Performance**: Comparativa fin de semana vs días laborales

## Estructura de datos

El dashboard usa las siguientes tablas de dbt:

- `fct_flights`: Tabla de hechos con todos los vuelos y sus KPIs
- `fct_route_daily`: Agregación diaria por ruta

## Solución de problemas

### Error: "Could not connect to Metabase"
- Verifica que Metabase esté corriendo: `docker compose ps`
- Verifica que el puerto 3000 esté accesible

### Error: "Required tables not found"
- Ejecuta dbt para crear las tablas: `make dbt/run`
- El script intentará sincronizar la base de datos automáticamente

### Error de autenticación
- Verifica las credenciales de Metabase
- Asegúrate de haber completado el setup inicial de Metabase
