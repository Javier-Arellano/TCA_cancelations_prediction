"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.13
"""
import pandas as pd


def procesar_reservas(reservas_raw: pd.DataFrame, id_agencias:pd.DataFrame, id_canales:pd.DataFrame,id_empresa:pd.DataFrame,
                      id_estados: pd.DataFrame, id_habitacion:pd.DataFrame, id_paises:pd.DataFrame, id_paquete:pd.DataFrame,
                      id_programa:pd.DataFrame, id_reservacioens:pd.DataFrame, id_segmento:pd.DataFrame) -> pd.DataFrame:

    # Realizar merges
    df = pd.merge(reservas_raw, id_agencias[['ID_Agencia', 'Agencia_nombre']], on='ID_Agencia', how='left')
    df = pd.merge(df, id_canales[['ID_canal', 'Canal_nombre']], on='ID_canal', how='left')
    df = pd.merge(df, id_empresa[['ID_empresa', 'Empresa_nombre']], on='ID_empresa', how='left')
    df = pd.merge(df, id_agencias[['ID_Agencia', 'Ciudad_Nombre']], on='ID_Agencia', how='left')
    df = pd.merge(df, id_estados[['Estado_cve', 'Estado_Nombre']], left_on='h_edo', right_on='Estado_cve', how='left')
    df = pd.merge(df, id_habitacion[['ID_Tipo_Habitacion', 'Tipo_Habitacion_nombre']], on='ID_Tipo_Habitacion', how='left')
    df = pd.merge(df, id_paises[['ID_Pais_Origen', 'Pais_Nombre']], on='ID_Pais_Origen', how='left')
    df = pd.merge(df, id_paquete[['ID_paquete', 'Paquete_nombre']], left_on='ID_Paquete', right_on='ID_paquete', how='left')
    df = pd.merge(df, id_programa[['ID_programa', 'Programa_nombre']], left_on='ID_Programa', right_on='ID_programa', how='left')
    df = pd.merge(df, id_reservacioens[['ID_estatus_reservaciones', 'estatus_reservaciones']], on='ID_estatus_reservaciones', how='left')
    df = pd.merge(df, id_segmento[['ID_Segmento_Comp', 'Segmento_Comp_Nombre']], on='ID_Segmento_Comp', how='left')

    # Renombrar columnas
    columnas_renombrar = {
        'ID_Reserva': 'id_reservaciones',
        'Fecha_hoy': 'fecha_hoy',
        'h_res_fec': 'fecha_reservacion',
        'h_fec_lld': 'fecha_llegada',
        'h_fec_sda': 'fecha_salida',
        'h_num_per': 'numero_personas',
        'aa_h_num_per': 'numero_personas_anio_anterior',
        'aa_h_num_adu': 'numero_adultos_anio_anterior',
        'aa_h_num_men': 'numero_menores_anio_anterior',
        'aa_h_num_noc': 'numero_noches_anio_anterior',
        'aa_h_tot_hab': 'total_habitaciones_anio_anterior',
        'h_num_adu': 'numero_adultos',
        'h_num_men': 'numero_menores',
        'h_num_noc': 'numero_noches',
        'h_tot_hab': 'total_habitaciones',
        'ID_Programa': 'id_programa',
        'Programa_nombre': 'nombre_programa',
        'ID_Paquete': 'id_paquete',
        'Paquete_nombre': 'nombre_paquete',
        'ID_Segmento_Comp': 'id_segmento',
        'Segmento_Comp_Nombre': 'nombre_segmento',
        'ID_Agencia': 'id_agencia',
        'Agencia_nombre': 'nombre_agencia',
        'ID_empresa': 'id_empresa',
        'Empresa_nombre': 'nombre_empresa',
        'ID_Tipo_Habitacion': 'id_tipo_habitacion',
        'Tipo_Habitacion_nombre': 'nombre_tipo_habitacion',
        'ID_canal': 'id_canal',
        'Canal_nombre': 'nombre_canal',
        'ID_Pais_Origen': 'id_pais_origen',
        'Pais_Nombre': 'nombre_pais_origen',
        'ID_estatus_reservaciones': 'id_estatus_reservacion',
        'estatus_reservaciones': 'nombre_estatus_reservacion',
        'h_edo': 'clave_estado',
        'Estado_Nombre': 'nombre_estado',
        'h_tfa_total': 'total_tarifa',
        'moneda_cve': 'id_moneda',
        'h_ult_cam_fec': 'fecha_ultimo_cambio',
        'aa_Cliente_Disp': 'cliente_disp_anio_anterior',
        'Reservacion': 'reservacion',
        'Cliente_Disp': 'cliente_disp',
        'Ciudad_Nombre': 'ciudad_agencia'
    }

    df = df.rename(columns=columnas_renombrar)

    # Conversión de columnas de fecha
    fecha_columnas = ['fecha_llegada', 'fecha_reservacion', 'fecha_salida', 'fecha_ultimo_cambio']
    for col in fecha_columnas:
        df[col] = pd.to_datetime(
            df[col].astype(str).str.strip().replace('', pd.NA),
            format='%Y%m%d',
            errors='coerce'
        )

    # Filtrar solo columnas deseadas (basado en tu imagen)
    columnas_finales = [
        'id_reservaciones', 'fecha_hoy', 'fecha_reservacion', 'fecha_llegada', 'fecha_salida',
        'numero_personas', 'numero_personas_anio_anterior', 'numero_adultos', 'numero_adultos_anio_anterior',
        'numero_menores', 'numero_menores_anio_anterior', 'numero_noches', 'numero_noches_anio_anterior',
        'total_habitaciones', 'total_habitaciones_anio_anterior', 'id_programa', 'nombre_programa',
        'id_paquete', 'nombre_paquete', 'id_segmento', 'nombre_segmento', 'id_agencia', 'nombre_agencia',
        'id_empresa', 'nombre_empresa', 'id_tipo_habitacion',
        'nombre_tipo_habitacion', 'id_canal', 'nombre_canal', 'id_pais_origen', 'nombre_pais_origen',
        'id_estatus_reservacion', 'nombre_estatus_reservacion',
        'clave_estado', 'nombre_estado', 'total_tarifa', 'id_moneda', 'fecha_ultimo_cambio',
        'reservacion', 'cliente_disp', 'cliente_disp_anio_anterior','ciudad_agencia'
    ]

    df = df[columnas_finales]

    # Agregar columnas históricas
    df['hist_personas'] = df['numero_personas'].astype(str) + '-' + df['numero_personas_anio_anterior'].astype(str)
    df['hist_adultos'] = df['numero_adultos'].astype(str) + '-' + df['numero_adultos_anio_anterior'].astype(str)
    df['hist_menores'] = df['numero_menores'].astype(str) + '-' + df['numero_menores_anio_anterior'].astype(str)
    df['hist_noches'] = df['numero_noches'].astype(str) + '-' + df['numero_noches_anio_anterior'].astype(str)
    df['hist_total_habitaciones'] = df['total_habitaciones'].astype(str) + '-' + df['total_habitaciones_anio_anterior'].astype(str)

    # Eliminar columnas ID salvo id_reservaciones
    columns_to_drop = [col for col in df.columns if col.startswith('id_') and col != 'id_reservaciones']
    df.drop(columns=columns_to_drop, inplace=True)

    # Crear columna cancelacion
    df['cancelacion'] = df['nombre_estatus_reservacion'].apply(lambda x: 1 if x == 'RESERVACION CANCELADA' else 0)

    return df


def get_pesos(processed_reservas:pd.DataFrame) -> pd.DataFrame:
    df_features = processed_reservas.copy()

    # 1. Columnas a reemplazar
    columnas_a_utilizar = [
        'hist_personas', 'hist_adultos', 'hist_menores', 'hist_noches', 'hist_total_habitaciones',
        'nombre_programa', 'nombre_paquete', 'nombre_segmento', 'nombre_agencia', 'ciudad_agencia',
        'nombre_empresa', 'nombre_tipo_habitacion', 'nombre_canal', 'nombre_pais_origen',
        'nombre_estado', 'total_tarifa'
    ]

    # 2. Filtrar cancelaciones para aprender
    df_cancelaciones = processed_reservas[processed_reservas['cancelacion'] == 1]

    # 3. Calcular y aplicar pesos
    for col in columnas_a_utilizar:
        conteo = df_cancelaciones[col].astype(str).value_counts().reset_index()
        conteo.columns = [col, 'conteo']
        total = conteo['conteo'].sum()
        conteo['proporcion'] = (conteo['conteo'] / total * 100).round(3)
        mapa = dict(zip(conteo[col], conteo['proporcion']))
        df_features[col] = df_features[col].map(mapa)

    return df_features
