# created by Joel Paiz
# hpaiz@imsa.com.gt
# Automatizacion Agricola 2022

from datetime import datetime, date, time, timedelta
import cx_Oracle
import pandas as pd
import recursos.oracledb as dbu
import pandas as pan
import streamlit as st
import numpy as np


def lamina_lluvia(device_name, dev_eui, fecha_ini, fecha_fin, ciclo):
    total = 0
    aumenta = 0
    promedio = 0
    dia = fecha_ini.day
    mes = fecha_ini.month
    año = fecha_ini.year
    hora_ini = "12:00:00 AM"
    hora_fin = "11:59:59 PM"
    fecha_inicial = datetime(año, mes, dia, 12, 00, 00).strftime("'%d-%B-%y %H:%M:%S AM'")
    fecha_final = datetime(año, mes, dia, 11, 59, 59).strftime("'%d-%B-%y %H:%M:%S PM'")
    while aumenta < ciclo:
        try:
            conn = cx_Oracle.connect(user=dbu.usuario, password=dbu.contraseña, dsn=dbu.dsn)
            cursor2 = conn.cursor()

            sql2 = '''
               SELECT data_lluvia FROM SDEUSR.data_pluviometros_lluvia_imsa
                where  fk_dev_eui =''' + "'" + dev_eui + "'" + ''' and data_lluvia !=0
                and  fecha_cap between ''' + fecha_inicial + '''  and ''' + fecha_final + '''
            '''
            cursor2.execute(sql2)
            data2 = cursor2.fetchall()
            aumenta = aumenta + 1
        except cx_Oracle.Error as error:
            print(error)

    # st.write(fecha_inicial + " " + fecha_final)
    # print("")


def dispositivos(fecha_ini, fecha_fin, ciclo):
    f = fecha_ini
    f2 = fecha_fin
    c = ciclo
    try:
        conn = cx_Oracle.connect(user=dbu.usuario, password=dbu.contraseña, dsn=dbu.dsn)
        cursor = conn.cursor()
        sql = '''
             select device_name, dev_eui 
             from SDEUSR.DATA_SENSORBASE_IMSA 
             where application_id = 2 order by device_name asc
        '''
        cursor.execute(sql)
        data = cursor.fetchall()
        for dispositivo in data:
            data_list = list(dispositivo)  # --> se almacena los datos en una lista

            if data_list[0] == '12-250':  # --> condicional que detecta el pluv 250
                print('')
            else:
                if data_list[0] == '12-251':  # --> condicional que detecta el pluv 251
                    print('')
                else:
                    # print("device_name: " + data_list[0] + " dev_eui " + data_list[1])
                    dvn = data_list[0]  # --> se almacena el nombre del device
                    dve = data_list[1]  # --> se almacena el dev_eui
                    # lamina_lluvia(str(dvn), str(dve), f, f2, c)  # --> ejecuta la funcion Query line 6
        lamina_lluvia('str(dvn)', 'str(dve)', f, f2, c)
    except cx_Oracle.Error as error:
        print(error)
        conn.close()
    conn.close()


def dibujarTabla():
    return 0


def main():
    with st.sidebar:
        add_radio = st.radio(
            "Choose a shipping method",
            ("Standard (5-15 days)", "Express (2-5 days)")
        )
    # fecha_text = '<p style="font-family:sans-serif; color:Green; font-size: 42px;">Ingrese Fecha</p>'
    # st.markdown(fecha_text, unsafe_allow_html=True)
    st.title('Lamina de agua ' + str(datetime.now().strftime(" %Y")))
    d = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_ini")
    d2 = st.date_input(label="INGRESE FECHA FINAL", key="fecha_fin")
    # fecha_ini = d.strftime("'%d-%B-%y 12:00:00 AM'")
    # fecha_fin = d2.strftime("'%d-%B-%y 11:59:59 PM'")
    ciclo = d2.strftime("%d")

    dispositivos(d, d2, int(ciclo))
    # st.write('inicial: ', fecha_ini)
    # st.write('Final: ', fecha_fin)
