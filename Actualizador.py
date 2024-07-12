
import pandas as pd 
import numpy as np 
from sqlalchemy import create_engine 
from sqlalchemy import text 
from datetime import datetime
import configparser

# Función para determinar alzas y bajas
def set_ab(value):
    if value > 0:
        return 'Alza'
    elif value < 0:
        return 'Baja'
    else:
        return ''

# Año y mes actuales
anio = int(datetime.now().strftime('%Y'))
mes = int(datetime.now().strftime('%m'))

# Leer el archivo de configuración
config = configparser.ConfigParser()
config.read('config.ini')

# Obtener las credenciales
server = config['database']['server']
user = config['database']['user']
password = config['database']['password']
driver = config['database']['driver']

try:
    database_connection = f'mssql://{user}:{password}@{server}/db-indices?driver={driver}'
    engine = create_engine(database_connection)
    connection = engine.connect()
    print('Conexión exitosa')
except:
    print('Fallo en la conexión')

# Funcion para extraer los precios
def boletas (anio, mes, connect):
    querycod = text(f"EXEC [dbo].[sp_get_precios_recolectados_mes] {anio}, {mes};")
    return pd.read_sql(querycod, connect)
base = boletas (anio, mes, connection)

base_p = base.loc[
    ~(
        (base['nt_tipo_nombre']=='Periodo de espera') |
        (base['nt_tipo_nombre']=='Cambio de referencia')
    ) &
    (base['estado_registro']=='Validada')
]

# Funcion para extraer los precios
def boletas (anio, mes, connect):
    querycod = text(f"EXEC [dbo].[sp_get_precios_recolectados_mes] {anio}, {mes};")
    return pd.read_sql(querycod, connect)

Precios1 = boletas (anio, mes, connection)
Precios_v = Precios1.loc[(Precios1['estado_registro']=='Validada')] # Obtener solo voletas validadas
Precios_v = Precios_v.loc[(Precios_v['estado_imputacion']!='Imputado')] # No tomar en cuenta a los precios imputados

# Importamos la correspondencia de la CBA
variedades_cba = 'Productos y variedades CBA UR 2024 Cod sin puntos W2.xlsx'
vur = 'Variedades_UR'
vu = 'Variedades_CBA_u'
vr = 'Variedades_CBA_r'

PCBA_ur = pd.read_excel(variedades_cba, sheet_name=vur)
CBA_u = pd.read_excel(variedades_cba, sheet_name=vu)
CBA_r = pd.read_excel(variedades_cba, sheet_name=vr)

PCBA_ur['Codigo_articulo'] = PCBA_ur['Codigo_articulo'].astype('string')
CBA_u['codigo_articulo'] = CBA_u['codigo_articulo'].astype('string')
CBA_r['codigo_articulo'] = CBA_r['codigo_articulo'].astype('string')

lista_u = PCBA_ur.set_index('Codigo_articulo')['cant_CBA'].to_dict()

# Filtrar y almacenar en un diccionario
Pcba = {}
for codigo, unidad in lista_u.items():
    if unidad == 0:
        filtro = (base_p['codigo_articulo'] == codigo)
    else:
        filtro = (base_p['codigo_articulo'] == codigo) & (base_p['cantidad_actual'] == unidad)
    Pcba[codigo] = base_p[filtro]

# Crear un DataFrame final concatenando los resultados
df_ur = pd.concat(Pcba.values(), ignore_index=True)

#df_ur["Precio_base"] = (df_ur["precio_actual"] * df_ur["cantidad_base"]) / df_ur["cantidad_actual"]
df_ur["pb_ant"] = (df_ur["precio_anterior"] * df_ur["cantidad_base"]) / df_ur["cantidad_anterior"]
df_ur["pb_act"] = (df_ur["precio_actual"] * df_ur["cantidad_base"]) / df_ur["cantidad_actual"]

df_ur.loc[(df_ur['codigo_articulo'] == '1191011'), 'pb_ant'] = (df_ur['pb_ant'] * 454) / 200
df_ur.loc[(df_ur['codigo_articulo'] == '1143012'), 'pb_ant'] = (df_ur['pb_ant'] * 1000) / 800

df_ur.loc[(df_ur['codigo_articulo'] == '1191011'), 'pb_act'] = (df_ur['pb_act'] * 454) / 200
df_ur.loc[(df_ur['codigo_articulo'] == '1143012'), 'pb_act'] = (df_ur['pb_act'] * 1000) / 800

#df_ur.loc[(df_ur['codigo_articulo'] == '1191011'), 'Precio_base2'] = (df_ur['Precio_base2'] * 454) / 200
#df_ur.loc[(df_ur['codigo_articulo'] == '1143012'), 'Precio_base2'] = (df_ur['Precio_base2'] * 1000) / 800

# Filtrar valores no nulos y diferentes de 0
df_ur_fil = df_ur[df_ur['pb_act'].notnull() & (df_ur['pb_act'] != 0)]

pcba = df_ur_fil[['codigo_articulo', 'articulo', 'cantidad_base',
    'cantidad_anterior', 'cantidad_actual', 'precio_anterior', 'precio_actual',
    'cantidad_actual', "pb_ant", "pb_act", 'nt_tipo', "ine_poll_id"]]

pcba['rel'] = pcba['pb_act']/pcba['pb_ant']
pcba['var%'] = ((pcba['rel']*100)-100)

#pcba["rel"] = pcba["rel"].replace ([np.NaN,np.inf ], 1)
#pcba["var%"] = pcba["var%"].replace ([np.NaN,np.inf ], 0)

pcba.loc[(pcba['pb_ant'].isnull())|(pcba['pb_ant'] == 0), 'var%'] = 0
pcba.loc[(pcba['pb_ant'].isnull())|(pcba['pb_ant'] == 0), 'rel'] = 1

def medians_qrel(data, rel, c=3):
    dfil = data[~((data[rel] >= 0.95)&(data[rel] <= 1.05))]
    
    med = dfil[rel].median()
    # Calcular los cuartiles
    Q1 = dfil[rel].quantile(0.25)
    Q3 = dfil[rel].quantile(0.75)
    DM = (Q3 - Q1) / 2
    li = (med - (c * DM))
    ls = (med + (c * DM))
    return dfil.loc[(dfil[rel] < li) | (dfil[rel] > ls)]

def Analisis_mq4(df, cod, valor, um=2):
    out = df.groupby(cod).apply(medians_qrel, rel=valor, c=um).reset_index(drop=True)
    return (out)

atipic = Analisis_mq4(pcba,'codigo_articulo','rel', um=1.5)

def medians_qrel(data, rel, c=2):
    dfil = data[~((data[rel] >= 0.95) & (data[rel] <= 1.05))]
    
    med = dfil[rel].median()
    # Calcular los cuartiles
    Q1 = dfil[rel].quantile(0.25)
    Q3 = dfil[rel].quantile(0.75)
    DM = (Q3 - Q1) / 2
    li = (med - (c * DM))
    ls = (med + (c * DM))
    return (dfil[rel] < li) | (dfil[rel] > ls)

# Aplicar la función medians_qrel por grupo y luego resetear los índices
pcba['fil'] = pcba.groupby('codigo_articulo').apply(lambda x: medians_qrel(x, 'rel', c=1.5)).reset_index(level=0, drop=True)
pcba['fil'] = pcba['fil'].replace(np.nan, False)
print(len(pcba['fil']))
print(pcba['fil'].unique())
# Filtrar las filas que no son atípicas
pcba_sinout = pcba[(pcba['fil'] == False)]

def fil_outliers(grupo): 
    C = 1.5
    Q1 = grupo.quantile(0.25)
    Q3 = grupo.quantile(0.75)
    IQR = Q3 - Q1
    limite_inferior = Q1 - C * IQR
    limite_superior = Q3 + C * IQR
    
    return (grupo >= limite_inferior) & (grupo <= limite_superior)

# Aplicar la función fil_outliers a cada grupo de productos
rf_ur = pcba_sinout.groupby('codigo_articulo')['pb_act'].transform(fil_outliers)
result_f_ur = pcba_sinout[rf_ur]

#Obtenemos los precios medianos de cada variedad
cba_u = pd.merge(result_f_ur,CBA_u, how='inner', on=['codigo_articulo'])
canasta_u = cba_u.groupby(['Codigo_Cepal','Grupo_alimenticio','Codigo_enigh', 'Producto_enigh']).agg(
    n=("codigo_articulo", "count"),
    precio_med_m=("pb_act", "median"),
    Cant_base= ("Cant_base", "max"),
    Cantgbxdia= ("Cantgbxdia", "max"),
    CantgNxdia=("CantgNxdia", "max"),
    kcalxdia=("Kilocalorias_xdia", "max"),
    Aporte_de_Proteinas=("Proteinas_aj", "max"),
    Aporte_de_Grasas= ("Grasas_aj", "max"),
    Aporte_de_Carbohidratos= ("Carbohidratos_aj", "max"),
).reset_index()

# Estimamos los costos
canasta_u['Costo_diarioxpersona'] = ((canasta_u['Cantgbxdia']*canasta_u['precio_med_m'])/canasta_u['Cant_base'])
canasta_u['Cantidadxdia_h'] = canasta_u['Cantgbxdia']*4.16
canasta_u['kcal_xdia_xhogar'] = canasta_u['kcalxdia']*4.16
canasta_u['Costo_diarioxhogar'] = canasta_u['Costo_diarioxpersona']*4.16

# Agrupamos por Grupo alimenticio y obtenemos totales
grupoalim_u = canasta_u.groupby(['Codigo_Cepal','Grupo_alimenticio',]).agg(
    n=("Codigo_enigh", "count"),
    Cantkgb_xdia_xp=("Cantgbxdia", "sum"),
    Kcal_xdia_xpersona = ("kcalxdia", "sum"),
    Costo_diarioxpersona=("Costo_diarioxpersona", "sum"),
    Cantkgb_xdia_xh= ("Cantidadxdia_h", "sum"),
    Kcal_xdia_xhogar = ("kcal_xdia_xhogar", "sum"),
    Costo_diarioxhogar=("Costo_diarioxhogar", "sum"),
    Proteinas_d=("Aporte_de_Proteinas", "sum"),
    Grasas_d=("Aporte_de_Grasas", "sum"),
    Carbohidratos_d=("Aporte_de_Carbohidratos", "sum"),
).reset_index()

total_u = grupoalim_u.drop(columns=['Codigo_Cepal']).sum().to_frame().T
total_u['Grupo_alimenticio'] = 'Total'
grupoalim_u = pd.concat([grupoalim_u, total_u], ignore_index=True)

#Canasta Rural
cba_r = pd.merge(result_f_ur,CBA_r, how='inner', on=['codigo_articulo'])
canasta_r = cba_r.groupby(['Codigo_Cepal','Grupo_alimenticio','Codigo_enigh', 'Producto_enigh']).agg(
    n=("codigo_articulo", "count"),
    precio_med_m=("pb_act", "median"),
    Cant_base=("Cant_base", "max"),
    Cantgbxdia=("Cantgbxdia", "max"),
    CantgNxdia=("CantgNxdia", "max"),
    kcalxdia=("Kilocalorias_xdia", "max"),
    Aporte_de_Proteinas=("Proteinas_aj", "max"),
    Aporte_de_Grasas= ("Grasas_aj", "max"),
    Aporte_de_Carbohidratos= ("Carbohidratos_aj", "max"),
).reset_index()

# Estimamos los costos
canasta_r['Costo_diarioxpersona'] = ((canasta_r['Cantgbxdia']*canasta_r['precio_med_m'])/canasta_r['Cant_base'])
canasta_r['Cantidadxdia_h'] = canasta_r['Cantgbxdia']*4.8
canasta_r['kcal_xdia_xhogar'] = canasta_r['kcalxdia']*4.8
canasta_r['Costo_diarioxhogar'] = canasta_r['Costo_diarioxpersona']*4.8

# Agrupamos por Grupo alimenticio y obtenemos totales
grupoalim_r = canasta_r.groupby(['Codigo_Cepal','Grupo_alimenticio',]).agg(
    n=("Codigo_enigh", "count"),
    Cantkgb_xdia_xp=("Cantgbxdia", "sum"),
    Kcal_xdia_xpersona = ("kcalxdia", "sum"),
    Costo_diarioxpersona=("Costo_diarioxpersona", "sum"),
    Cantkgb_xdia_xh= ("Cantidadxdia_h", "sum"),
    Kcal_xdia_xhogar = ("kcal_xdia_xhogar", "sum"),
    Costo_diarioxhogar=("Costo_diarioxhogar", "sum"),
    Proteinas_d=("Aporte_de_Proteinas", "sum"),
    Grasas_d=("Aporte_de_Grasas", "sum"),
    Carbohidratos_d=("Aporte_de_Carbohidratos", "sum"),
).reset_index()

total_r = grupoalim_r.drop(columns=['Codigo_Cepal']).sum().to_frame().T
total_r['Grupo_alimenticio'] = 'Total'
grupoalim_r = pd.concat([grupoalim_r, total_r], ignore_index=True)

masterBaseGrupo = pd.read_excel("master_base_stable.xlsx", sheet_name="Grupos")
masterBaseProd = pd.read_excel("master_base_stable.xlsx", sheet_name="Productos")

canasta_u["Área"] = "Urbana"
canasta_r["Área"] = "Rural"

canasta = pd.merge(
    pd.concat(
        [
            canasta_u,
            canasta_r
        ],
        ignore_index=True
    ),
    masterBaseProd[
        (masterBaseProd["Mes"] == ((mes - 2) % 12) + 1) &
        (masterBaseProd["Año"] == (anio - (mes == 1)))
    ][
        [
            "Codigo_enigh",
            "Área",
            "precio_med_m"
        ]
    ].rename(
        columns={
            "precio_med_m": "precio_anterior"
        }
    ),
    on=["Codigo_enigh", "Área"]
)

canasta["variación"] = (100 * canasta["precio_med_m"] / canasta["precio_anterior"]) - 100
canasta["Mes"] = mes
canasta["Año"] = anio

up_canasta = pd.merge(
    canasta,
    masterBaseProd[
        (masterBaseProd["Mes"] == 12) &
        (masterBaseProd["Año"] == anio - 1)
    ][
        ["Codigo_enigh", "Área", "precio_med_m",]
    ].rename(
        columns={
            "precio_med_m": "Variación acumulada"
        }
    ),
    on=["Codigo_enigh", "Área"]
)

up_canasta["Variación acumulada"] = \
    100 * up_canasta["precio_med_m"] /\
    up_canasta["Variación acumulada"] - 100

up_canasta["Alza / Baja"] = up_canasta["variación"].apply(set_ab)

grupoalim_u["Área"] = "Urbana"
grupoalim_r["Área"] = "Rural"

grupoalim = pd.merge(
    pd.concat(
        [
            grupoalim_r,
            grupoalim_u
        ],
        ignore_index=True
    ),
    masterBaseGrupo[
        (masterBaseGrupo["Mes"] == ((mes - 2) % 12) + 1) &
        (masterBaseGrupo["Año"] == anio - (mes == 1))
    ][
        ["Codigo_Cepal", "Costo_diarioxpersona", "Área"]
    ].rename(
        columns={
            "Costo_diarioxpersona": "Costo anterior"
        }
    ),
    on=["Codigo_Cepal", "Área"],
    how="inner"
)

grupoalim["Mes"] = mes
grupoalim["Año"] = anio

with pd.ExcelWriter(
    "master_base.xlsx",
    engine="xlsxwriter"
) as writer:
    pd.concat(
        [masterBaseProd, up_canasta]
    ).to_excel(
        writer,
        sheet_name="Productos",
        index=False
    )
    pd.concat(
        [masterBaseGrupo, grupoalim]
    ).to_excel(
        writer,
        sheet_name="Grupos",
        index=False
    )