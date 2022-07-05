from datetime import datetime, date, time, timedelta
import cx_Oracle
import pandas as pd
import recursos.oracledb as dbu
import pandas as pan
import streamlit as st
import numpy as np


def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


def paquetes_promedio(dve, dia, mes, año, ciclo):
    # d = dia
    total = 0
    d = dia
    m = mes
    a = año
    hi = "12:00:00 AM"
    hf = "11:59:59 PM"
    ar = []
    df = pan.DataFrame(index=('%d' % i for i in range(dia, ciclo + 1)))
    try:
        conn = cx_Oracle.connect(user=dbu.usuario, password=dbu.contraseña, dsn=dbu.dsn)
        cursor = conn.cursor()
        cursor2 = conn.cursor()
        for dv in dve:
            sql = '''
                   select dev_eui 
                   from SDEUSR.DATA_SENSORBASE_IMSA 
                   where application_id = 2 and device_name = ''' + "'" + dv + "'" + '''
                   order by device_name asc
            '''
            cursor.execute(sql)
            data = cursor.fetchall()
            # st.write(data)
            # while d <= ciclo

            for dispositivo in data:
                data_list = list(dispositivo)  # --> se almacena los datos en una lista
                dven = data_list[0]  # --> se almacena el dev_eui
                while d<=ciclo:
                    f3 = datetime(a, m, d).strftime("%d/%m/%Y")
                    f = datetime(a, m, d, 12, 00, 00).strftime("'%d-%B-%y " + hi + "'")
                    f2 = datetime(a, m, d, 12, 00, 00).strftime("'%d-%B-%y " + hf + "'")
                    sql2 = '''
                    select fecha_cap
                        from SDEUSR.data_pluviometros_lluvia_imsa 
                        where fecha_cap between ''' + f + ''' and ''' + f2 + ''' 
                        and fk_dev_eui=''' + "'" + dven + "'" + ''' GROUP BY fk_dev_eui, fecha_cap
                        order by fecha_cap desc
                    '''
                    cursor2.execute(sql2)
                    data2 = cursor2.fetchall()
                    total = total + len(data2)
                    ar.append(total)
                    d = d + 1
                    total = 0
            df[str(dv)] = ar
            d = dia
            ar = []
        newdf = df.transpose()
        st.dataframe(newdf)
        # st.bar_chart(df)
        csv = convert_df(newdf)
        st.download_button(
            label="Descargar como CSV",
            data=csv,
            file_name='Consulta Paquetes.csv',
            mime='text/csv',
        )

        cursor.close()
        conn.close()
    except cx_Oracle.Error as error:

        print(error)
