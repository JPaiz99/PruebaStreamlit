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


def lamina_lluvia(dev_eui, fecha_ini, ciclo):
    total = 0
    promedio = 0
    dia = fecha_ini.day
    contador = 0
    ar = []
    while dia <= ciclo:
        mes = fecha_ini.month
        año = fecha_ini.year
        fecha_inicial = datetime(año, mes, dia, 12, 00, 00).strftime("'%d-%B-%y %H:%M:%S AM'")
        fecha_final = datetime(año, mes, dia, 11, 59, 59).strftime("'%d-%B-%y %H:%M:%S PM'")
        # st.write(str(contador) + fecha_inicial)
        # t = {dia: [1, 2, 3, 4], dia + 1: [1, 3, 4, 2]}
        # x = pan.DataFrame(data=t, index=lista)

        # st.write(ar)
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
            for dispositivo in data2:
                data_list = list(dispositivo)
                if data_list[0] is None:
                    print("")
                else:
                    lluvia = data_list[0]
                    total = total + lluvia

        except cx_Oracle.Error as error:
            print(error)
        contador = contador + 1
        ar.append(total)
        dia = dia + 1
        total = 0
        # st.write(ar)

    # st.write(ar)
    return ar


def dispositivos(fecha_ini, fecha_fin, ini_ciclo, ciclo):
    f = fecha_ini
    f2 = fecha_fin
    c = ciclo
    ic = ini_ciclo
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
        # st.write(data)

        df = pan.DataFrame(index=('%d' % i for i in range(ic, ciclo + 1)))

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
                    # ar = []
                    # ar.append(data_list[0])
                    # st.write(ar[5])
                    # ar = lamina_lluvia(str(dvn), str(dve), f, c)  # --> ejecuta la funcion Query line 6
                    # device_name = str(dvn)
                    # df[device_name] = ar

                    # st.write('You selected:', options)

        ar = lamina_lluvia('0A34AE2A93B6B2EB', f, c)
        device_name = '12-001'
        df[device_name] = ar
        newdf = df.transpose()
        st.dataframe(newdf)
        st.line_chart(ar)
        csv = convert_df(df)

        st.download_button(
            label="Descargar como CSV",
            data=csv,
            file_name='lamina ' + str(f.strftime("%B %Y")) + '.csv',
            mime='text/csv',
        )
        # st.write(ar)
    except cx_Oracle.Error as error:
        print(error)
        st.error('Error al ejecutar la consulta')
        conn.close()
    conn.close()


def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


def lluvialam(dvn, fecha_ini, fecha_fin, ini_ciclo, ciclo):
    f = fecha_ini
    f2 = fecha_fin
    c = ciclo
    ic = ini_ciclo
    df = pan.DataFrame(index=('%d' % i for i in range(ic, ciclo + 1)))
    try:
        conn = cx_Oracle.connect(user=dbu.usuario, password=dbu.contraseña, dsn=dbu.dsn)
        cursor = conn.cursor()
        for dev in dvn:
            sql = '''
                    select dev_eui 
                    from SDEUSR.DATA_SENSORBASE_IMSA 
                    where application_id = 2 and device_name = ''' + "'" + dev + "'" + '''
                    order by device_name asc
            '''
            cursor.execute(sql)
            data = cursor.fetchall()
            # st.write(data)

            for dispositivo in data:
                data_list = list(dispositivo)  # --> se almacena los datos en una lista
                dve = data_list[0]  # --> se almacena el dev_eui
                str
                # ar = []
                # ar.append(data_list[0])
                # st.write(ar[5])
                ar = lamina_lluvia(str(dve), f, c)  # --> ejecuta la funcion Query line 6
                df[str(dev)] = ar

                # st.write('You selected:', options)

        # ar = lamina_lluvia('0A34AE2A93B6B2EB', f, c)
        # device_name = '12-001'
        # df[str(dev)] = ar
        newdf = df.transpose()
        st.dataframe(newdf)
        st.line_chart(df)
        # st.bar_chart(df)
        csv = convert_df(newdf)
        st.download_button(
            label="Descargar como CSV",
            data=csv,
            file_name='lamina de lluvia de ' + f.strftime('%d-%b-%Y') + ' a ' + f2.strftime('%d-%b-%Y') + '.csv',
            mime='text/csv',
        )
        # st.write(ar)
    except cx_Oracle.Error as error:
        print(error)
        st.error('Error al ejecutar la consulta')
        conn.close()
    conn.close()


def obtenerDvn():
    dvn = []
    try:
        conn = cx_Oracle.connect(user=dbu.usuario, password=dbu.contraseña, dsn=dbu.dsn)
        cursor = conn.cursor()
        sql = '''
                           select device_name
                           from SDEUSR.DATA_SENSORBASE_IMSA 
                           where application_id = 2 order by device_name asc
                      '''
        cursor.execute(sql)
        data = cursor.fetchall()
        # st.write(data)
        for dispositivo in data:
            data_list = list(dispositivo)  # --> se almacena los datos en una lista

            if data_list[0] == '12-250':  # --> condicional que detecta el pluv 250
                print('')
            else:
                if data_list[0] == '12-251':  # --> condicional que detecta el pluv 251
                    print('')
                else:
                    # print("device_name: " + data_list[0] + " dev_eui " + data_list[1])
                    dvn.append(data_list[0])
        return dvn

    except cx_Oracle.Error as error:
        print(error)
        st.error('Error al ejecutar la consulta')
        conn.close()
    conn.close()


# device = []

def main():
    # with st.sidebar:
    #   add_radio = st.radio(
    #     "Choose a shipping method",
    #   ("Standard (5-15 days)", "Express (2-5 days)")
    # )

    # fecha_text = '<p style="font-family:sans-serif; color:Green; font-size: 42px;">Ingrese Fecha</p>'
    # st.markdown(fecha_text, unsafe_allow_html=True)
    st.title('Lamina de agua ' + str(datetime.now().strftime(" %Y")))
    d = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_ini")
    d2 = st.date_input(label="INGRESE FECHA FINAL", key="fecha_fin")
    # fecha_ini = d.strftime("'%d-%B-%y 12:00:00 AM'")
    # fecha_fin = d2.strftime("'%d-%B-%y 11:59:59 PM'")
    ciclo = d2.strftime("%d")
    ini_ciclo = d.strftime("%d")
    ch = st.radio("Escoja", ('todos', 'algunos'))

    if ch == 'algunos':
        dvn2 = obtenerDvn()
        options = st.multiselect('Seleccione Pluviometros', dvn2, key='msl')
        if st.button('Aceptar'):
            with st.spinner('Cargando...'):
                lluvialam(options, d, d2, int(ini_ciclo), int(ciclo))
            # st.balloons()

    else:
        if st.button('Aceptar'):
            with st.spinner('Cargando...'):
                dispositivos(d, d2, int(ini_ciclo), int(ciclo))
            # st.balloons()

    # dispositivos(d, d2,int(ini_ciclo), int(ciclo))

    # st.write('inicial: ', fecha_ini)
    # st.write('Final: ', fecha_fin)
