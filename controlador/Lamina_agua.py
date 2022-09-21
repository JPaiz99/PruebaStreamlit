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
import controlador.promedioPaquetes as pp


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
        # st.write(fecha_inicial)
        # st.write(ar)
        try:
            conn = cx_Oracle.connect(user=dbu.usuario, password=dbu.contraseña, dsn=dbu.dsn)
            cursor2 = conn.cursor()

            sql2 = '''
               SELECT data_lluvia FROM SDEUSR.data_pluviometros_lluvia_imsa
                where fk_dev_eui =''' + "'" + dev_eui + "'" + ''' and data_lluvia !=0
                and fecha_cap between ''' + fecha_inicial + ''' and ''' + fecha_final + '''
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


def laminaLluviaTodos(fecha_ini, fecha_fin, ini_ciclo, ciclo):
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
                    dvn = data_list[0]  # --> se almacena el nombre del device
                    dve = data_list[1]  # --> se almacena el dev_eui
                    ar = lamina_lluvia(str(dve), f, c)  # --> ejecuta la funcion Query line 6
                    df[str(dvn)] = ar

        # ar = lamina_lluvia('0A34AE2A93B6B2EB', f, c)
        # device_name = '12-001'
        # df[device_name] = ar
        newdf = df.transpose()
        st.dataframe(newdf)
        # st.line_chart(df)
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



def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


def laminaLluviaEspecificos(dvn, fecha_ini, fecha_fin, ini_ciclo, ciclo):
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
                ar = lamina_lluvia(str(dve), f, c)  # --> ejecuta la funcion Query line 6
                df[str(dev)] = ar
        # ar = lamina_lluvia('0A34AE2A93B6B2EB', f, c)
        # device_name = '12-001'
        # df[str(dev)] = ar
        newdf = df.transpose()
        st.dataframe(newdf)
        st.line_chart(df)
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


def lluvia_min(dvn, dia, mes, año, conn):
    d = dia
    fecha_inicial = datetime(año, mes, d, 12, 00, 00).strftime("'%d-%B-%y %H:%M:%S AM'")
    # st.write(fecha_inicial)
    fecha_final = datetime(año, mes, d, 11, 59, 59).strftime("'%d-%B-%y %H:%M:%S PM'")
    # c = ciclo
    # ic = ini_ciclo
    dve = []
    lluvia = []
    fecha = []
    try:
        # conn = cx_Oracle.connect(user=dbu.usuario, password=dbu.contraseña, dsn=dbu.dsn)
        cursor = conn.cursor()
        cursor2 = conn.cursor()
        curol = conn.cursor()

        for dev in dvn:
            sqlrol = '''set role all'''
            sql = '''
                       select dev_eui 
                       from SDEUSR.DATA_SENSORBASE_IMSA 
                       where application_id = 2 and device_name = ''' + "'" + dev + "'" + '''
                       order by device_name asc
               '''
            curol.execute(sqlrol)
            cursor.execute(sql)
            data = cursor.fetchall()
            # st.write(data)
            for dispositivo in data:
                data_list = list(dispositivo)  # --> se almacena los datos en una lista
                # while dia <= ciclo:
                sqlrol2 = '''set role all'''
                sql2 = '''
                SELECT data_lluvia,  fecha_cap FROM SDEUSR.data_pluviometros_lluvia_imsa
                    where fk_dev_eui =''' + "'" + data_list[0] + "'" + ''' 
                    and fecha_cap between ''' + fecha_inicial + ''' and ''' + fecha_final + ''' ORDER by fecha_cap asc
                '''
                # st.write(sql2)
                cursor2.execute(sqlrol2)
                cursor2.execute(sql2)
                data2 = cursor2.fetchall()
                # st.write(data2)
                for dat in data2:
                    data_list2 = list(dat)
                    lluvia_data = data_list2[0]
                    fecha_data = data_list2[1]
                    lluvia.append(lluvia_data)
                    fecha.append(fecha_data.strftime("%d/%m/%y %H:%M:%S:%f"))
                dia = dia + 1

        # st.write(lluvia)
        # st.write(dev)
        # st.write(dia)
        df = pan.DataFrame(data=lluvia, index=fecha, columns=dvn)
        newdf = df.transpose()
        st.dataframe(newdf)
        # st.line_chart(df)
        csv = convert_df(newdf)
        st.download_button(
            label="Descargar como CSV",
            data=csv,
            file_name='lamina de lluvia' + dev + ' de ' + fecha_inicial + ' a ' + fecha_final + '.csv',
            mime='text/csv',
        )
        # st.write(ar)
    except cx_Oracle.Error as error:
        print(error)
        st.error('Error al ejecutar la consulta')
        conn.close()



def obtenerDvn(conn):
    dvn = []
    try:
        cursor = conn.cursor()
        cursorol = conn.cursor()
        sqlrol = '''set role all'''
        sql = '''
               select device_name
               from SDEUSR.DATA_SENSORBASE_IMSA 
               where application_id = 2 order by device_name asc
        '''
        cursorol.execute(sqlrol)
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
        st.error('Error al ejecutar la consulta' + str(error))


# device = []

def main():
    st.title('Reportes' + str(datetime.now().strftime(" %Y")))
    opcion = st.radio('Seleccione reporte',
                      ('Seleccione', 'Paquetes Pluviometros', 'Lamina de agua x min', 'Lamina de agua x dia'))
    if opcion == 'Lamina de agua x min':
        d = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_ini")
        d2 = st.date_input(label="INGRESE FECHA FINAL", key="fecha_fin")
        ciclo = d2.strftime("%d")
        ini_ciclo = d.strftime("%d")
        ch = st.radio("Escoja", ('todos', 'algunos', 'Reporte 10 min'))
        if ch == 'algunos':
            dvn2 = obtenerDvn()
            options = st.multiselect('Seleccione Pluviometros', dvn2, key='msl')
            if st.button('Lamina Aceptar'):
                with st.spinner('Cargando...'):
                    laminaLluviaEspecificos(options, d, d2, int(ini_ciclo), int(ciclo))
                # st.balloons()
        elif ch == 'Reporte 10 min':
            conn = cx_Oracle.connect(user=dbu.usuario, password=dbu.contraseña, dsn=dbu.dsn)
            dvn2 = obtenerDvn(conn)
            options = st.multiselect('Seleccione Pluviometros', dvn2, key='msl')
            if st.button('Lamina Aceptar'):
                with st.spinner('Cargando...'):
                    lluvia_min(options, d.day, d.month, d.year, conn)
                    conn.close()
                # st.balloons()
        else:
            if st.button('Aceptar'):
                with st.spinner('Cargando...'):
                    st.info('RECUERDA QUE ENTRE MAYOR SEA EL NUMERO DE DISPOSITIVOS MAYOR SERA EL TIEMPO DE ESPERA')
                    laminaLluviaTodos(d, d2, int(ini_ciclo), int(ciclo))
                # st.balloons()
    elif opcion == 'Paquetes Pluviometros':
        d = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_ini")
        d2 = st.date_input(label="INGRESE FECHA FINAL", key="fecha_fin")
        # ciclo = d2.strftime("%d")
        # ini_ciclo = d.strftime("%d")
        ch = st.radio("Escoja", ('todos', 'algunos'))
        if ch == 'algunos':
            conn = cx_Oracle.connect(user=dbu.usuario, password=dbu.contraseña, dsn=dbu.dsn)
            dvn2 = obtenerDvn(conn)
            options = st.multiselect('Seleccione Pluviometros', dvn2, key='msl')
            if st.button('Aceptar'):
                with st.spinner('Cargando...'):
                    pp.paquetes_promedio(options, d.day, d.month, d.year, d2.day)
                    # laminaLluviaEspecificos(options, d, d2, int(ini_ciclo), int(ciclo))
                # st.balloons()

        else:
            if st.button('Aceptar'):
                with st.spinner('Cargando...'):
                    st.info('RECUERDA QUE ENTRE MAYOR SEA EL NUMERO DE DISPOSITIVOS MAYOR SERA EL TIEMPO DE ESPERA')
                    # laminaLluviaTodos(d, d2, int(ini_ciclo), int(ciclo))
                    pp.paquetes_promedio_todos(d.day, d.month, d.year, d2.day)
                # st.balloons()

    elif opcion == 'Lamina de agua x dia':
        d = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_ini")
        d2 = st.date_input(label="INGRESE FECHA FINAL", key="fecha_fin")
        dvn2 = obtenerDvn()
        options = st.multiselect('Seleccione Pluviometros', dvn2, key='msl')
        if st.button('Aceptar'):
            with st.spinner('Cargando...'):
                lluviaXdia(options, d.day, d.month, d.year, d2.day)


# funcion de lluvia por dia

def lluviaXdia(dvn, dia, mes, año, ciclo):
    lluvia = 0
    lluviaT = []
    dTotal = []
    diasTotal = 0
    diaAumenta = dia

    columnas =['dias >1mm', 'total lluvia']
    df = pan.DataFrame()
    try:
        conn = cx_Oracle.connect(user=dbu.usuario, password=dbu.contraseña, dsn=dbu.dsn)
        cursor = conn.cursor()
        cursor2 = conn.cursor()
        curol = conn.cursor()
        curol2 = conn.cursor()
        for dev in dvn:
            sqlrol = '''set role all'''
            sql = '''
                        select dev_eui 
                        from SDEUSR.DATA_SENSORBASE_IMSA 
                        where application_id = 2 and device_name = ''' + "'" + dev + "'" + '''
                        order by device_name asc
                '''
            cursor.execute(sqlrol)
            cursor.execute(sql)
            eui = cursor.fetchall()

            for dispositivo in eui:
                data_list = list(dispositivo)  # --> se almacena los datos en una lista
                while diaAumenta < ciclo:
                    fecha_inicial = datetime(año, mes, diaAumenta, 12, 00, 00).strftime("'%d-%B-%y %H:%M:%S AM'")
                    # st.write(fecha_inicial)
                    fecha_final = datetime(año, mes, diaAumenta, 11, 59, 59).strftime("'%d-%B-%y %H:%M:%S PM'")
                    sqlrol2 = '''set role all'''
                    sql2 = '''
                                SELECT data_lluvia FROM SDEUSR.data_pluviometros_lluvia_imsa
                                    where fk_dev_eui =''' + "'" + data_list[0] + "'" + ''' 
                                    and fecha_cap between ''' + fecha_inicial + ''' and ''' + fecha_final + '''and 
                                    data_lluvia !=0 ORDER by data_lluvia desc '''
                    # st.write(sql2)
                    cursor2.execute(sqlrol2)
                    cursor2.execute(sql2)
                    for data2 in cursor2.fetchall():
                        data_list2 = list(data2)
                        lluvia = lluvia + float(data_list2[0])

                    if lluvia > 1.0:
                        diasTotal = diasTotal + 1

                    diaAumenta = diaAumenta + 1
                    #st.write(fecha_inicial)
                    #st.write(fecha_final)
                lluviaT.append(lluvia)
                dTotal.append(int(diasTotal))
                diaAumenta = dia
                lluvia = 0
                diasTotal = 0
            # st.write(lluviaT)
            # st.write(dTotal)
            arrayGod = [ dTotal ,lluviaT]
            #st.write(arrayGod)
            df = pan.DataFrame(index=columnas, data=arrayGod)
            # df.append(lluviaT)
        st.dataframe(df)
        csv = convert_df(df)
        st.download_button(
            label="Descargar como CSV",
            data=csv,
            file_name='lamina de lluvia por dia de ' + fecha_inicial + ' a ' + fecha_final + '.csv',
            mime='text/csv',
        )
    except cx_Oracle.Error as error:
        print(error)
        st.error('Error al ejecutar la consulta')
        conn.close()
