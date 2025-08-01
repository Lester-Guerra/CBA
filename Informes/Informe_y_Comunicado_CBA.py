from datetime import date
import pandas as pd
import subprocess
import sys

if len(sys.argv) == 1:
    mes = (date.today().month - 2) % 12 + 1
    anio = date.today().year - (mes == 12)
elif len(sys.argv) == 3:
    anio = int(sys.argv[1])
    mes = int(sys.argv[2])
else:
    sys.exit(
        "Introduzca el año y el mes para los cuales desea calcular la CBA." + \
        "\n" + \
        "Si no hay argumentos se usará la fecha del equipo atrasada 1 mes."
    )

# El comunicado se publica al mes siguiente.
# Estos son el año y mes de publicación
aniopub = anio + (mes == 12)
mespub = mes % 12 + 1

# Determina si el dia 7 del mes es en fin de semana
diasiete = date(aniopub , mespub, 7).weekday()
match diasiete:
    # Cae sábado
    case 5:
        # Se publica lunes 9
        diapub = 9
    # Cae domingo
    case 6:
        # Se publica lunes 8
        diapub = 8
    case _:
        diapub = 7

meses = [
    "enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
]

nMes = str(mes).zfill(2)

# Creamos un archivo con los macros que usaremos al compilar el .tex
with open('macros.tex', 'w') as macros:
    macros.write(
        "\\newcommand{\\nMes}{" + nMes + "}\n" +
        "\\newcommand{\\anio}{" + str(anio) + "}\n" +
        "\\newcommand{\\diapub}{" +str(diapub)+ "}\n" +
        "\\newcommand{\\mes}{" + meses[mes - 1] + "}\n" +
        "\\newcommand{\\anioant}{" +str(anio - 1)+ "}\n" +
        "\\newcommand{\\aniopub}{" + str(aniopub) + "}\n" +
        "\\newcommand{\\mespub}{" + meses[mespub - 1] + "}\n" +
        "\\newcommand{\\mesant}{" + meses[(mes - 2) % 12] + "}\n" +
        "\\newcommand{\\Mes}{" + meses[mes - 1].capitalize() + "}\n"
    )

filename = "../CBA_wg_" + str(anio) + "_" + str(mes) + ".xlsx"

def productos(filename, area):
    prodframe = pd.read_excel(
        filename, sheet_name="CBA_" + area.lower()
    ).sort_values(by="codigo_enigh").sort_values(by="Codigo_Cepal"
    ).reset_index()[[
        "Codigo_Cepal",
        "Grupo_alimenticio",
        "Producto_enigh",
        "Cantgbxdia",
        "Kilocalorias_xdia",
        "precio_anterior",
        "precio_med_m",
        "Costo_diarioxpersona",
        "var"
    ]]
    prodframe["Costo_anterior"] = prodframe["precio_anterior"] * \
        prodframe["Costo_diarioxpersona"] / prodframe["precio_med_m"]
    del prodframe["precio_anterior"]
    del prodframe["precio_med_m"]
    # Ahora se publica el costo mensual por lo que multiplicamos por 30
    prodframe["Costo_diarioxpersona"] *= 30
    prodframe["Costo_anterior"] *= 30
    prodframe["Cantgbxdia"] *= 30
    return prodframe

def signo(group_var):
    var = round(group_var, 2)
    if var > 0:
        return "+"
    elif var == 0:
        return "="
    else:
        return "-"

for area in ["U", "R"]:
    grupo_anterior = None
    macros_input = "\\newcommand{\\Prods" + area + "}{\n"
    canasta = productos(filename, area)
    for index, row in canasta.iterrows():
        grupo_actual = row['Grupo_alimenticio']
        if grupo_actual != grupo_anterior:
            macros_input += "& {\\bf " + grupo_actual + \
                "} & & & & & \\\\ \\hline\n"
            grupo_anterior = grupo_actual
        macros_input += str(index + 1) + " & " + \
            row["Producto_enigh"] + " & " + \
            f'{row["Kilocalorias_xdia"]:,.2f}' + " & " + \
            f'{row["Cantgbxdia"]:,.2f}' + " & " + \
            f'{row["Costo_anterior"]:,.2f}' + " & " + \
            f'{row["Costo_diarioxpersona"]:,.2f}' + " & " + \
            f'{row["var"]:,.2f}' + " \\\\ \\hline\n"
    macros_input += "}\n"
    with open('macros.tex', 'a', encoding="utf-8") as macros:
        macros.write(macros_input)
    canasta = canasta.groupby(by="Codigo_Cepal").agg({
        'Grupo_alimenticio': 'first',
        'Kilocalorias_xdia': 'sum',
        'Cantgbxdia': 'sum',
        'Costo_anterior': 'sum',
        'Costo_diarioxpersona': 'sum'
    })
    canasta["var"] = \
        canasta["Costo_diarioxpersona"] / canasta["Costo_anterior"] * 100 - 100
    macros_input = "\\newcommand{\\Grupos" + area + "}{\n"
    for index, row in canasta.iterrows():
        macros_input += str(index) + " & " + \
            row["Grupo_alimenticio"] + " & " + \
            f'{row["Kilocalorias_xdia"]:,.2f}' + " & " + \
            f'{row["Cantgbxdia"]:,.2f}' + " & " + \
            f'{row["Costo_anterior"]:,.2f}' + " & " + \
            f'{row["Costo_diarioxpersona"]:,.2f}' + " & " + \
            f'{row["var"]:,.2f}' + " \\\\ \\hline \n"
    macros_input += "}\n"
    with open('macros.tex', 'a', encoding="utf-8") as macros:
        macros.write(macros_input)

    macros_input = "\\newcommand{\\GruposCom" + area + "}{\n"
    for index, row in canasta.sort_values(by="var", ascending=False).iterrows():
        macros_input += str(index) + " & " + \
            row["Grupo_alimenticio"] + " & " + \
            f'{row["Cantgbxdia"]:,.2f}' + " & " + \
            f'{row["Costo_anterior"]:,.2f}' + " & " + \
            f'{row["Costo_diarioxpersona"]:,.2f}' + " & " + \
            f'{row["var"]:,.2f}' + " & " + signo(row["var"]) + \
            " \\\\ \\hline \n"
    macros_input += "}\n"
    with open('macros.tex', 'a', encoding="utf-8") as macros:
        macros.write(macros_input)

    CBA = canasta["Costo_diarioxpersona"].sum()
    CBAant = canasta["Costo_anterior"].sum()
    CBAvar = CBA / CBAant * 100 - 100
    with open('macros.tex', 'a') as macros:
        macros.write(
            "\\newcommand{\\CBA" + area + "}{" + f"{CBA:,.2f}" + "}\n" +
            "\\newcommand{\\CBA" + area + "ant}{" + f"{CBAant:,.2f}" + "}\n" +
            "\\newcommand{\\Var" + area + "}{" +
            f"{CBAvar:,.2f}" + "}\n" +
            "\\newcommand{\\CA" + area + "}{" +
            f'{(CBA * {"R": 1.968, "U": 2.421}[area]):,.2f}' + "}\n" +
            "\\newcommand{\\Signo" + area + "}{" + signo(CBAvar) + "}\n"
        )

subprocess.run([
    "xelatex",
    "-jobname=Informe_CBA_" + str(anio) + "_" + nMes,
    "Informe_CBA.tex"
])
# Se debe compilar dos veces
subprocess.run([
    "xelatex",
    "-jobname=Informe_CBA_" + str(anio) + "_" + nMes,
    "Informe_CBA.tex"
])

# Lo mismo para el comunicado
subprocess.run([
    "xelatex",
    "-jobname=Comunicado_" + str(anio) + "_" + nMes,
    "Comunicado_CBA.tex"
])

subprocess.run([
    "xelatex",
    "-jobname=Comunicado_" + str(anio) + "_" + nMes,
    "Comunicado_CBA.tex"
])