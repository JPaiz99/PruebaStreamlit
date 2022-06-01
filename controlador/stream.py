import pandas as pan
import streamlit as st
import numpy as np

'''
first app
'''


def streaml():
    st.title('Prueba Streamlit (>w<)/')

    st.header('Encabezado')

    st.subheader(" codigo Ejemplo")
    st.code("a = 1234")

    st.subheader("Ejemplo texto")
    st.text('"texto simple"')

    st.subheader("Ejemplo Ecuacion")
    st.latex("\int a x^2\,dx" + "" + "<--- expresion matematica")

    st.subheader(" Dataframe Ejemplo")
    df = pan.DataFrame({
        'primera columna': [5, 25, 10, 40],
        'segunda columna': [10, 20, 5, 15]
    })
    st.write(df)
    st.subheader("Ejemplo SLider")
    x = st.slider('Slider')
    dataf = np.random.randn(10, 20)
    st.write(x, dataf)

    st.subheader("Ejemplo Tabla")
    st.table(df)

    xs =st.number_input("Ingresa numero")
    st.subheader("Ejemplo Metrica")
    st.metric(label="Temperature", value=str(xs)+"°C", delta="-2.5 °C")  # metrica

    st.subheader("Ejemplo Chart normal")
    st.line_chart(df)

    st.subheader("Ejemplo Descarga archivo")

    def convert_df(df1):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode('utf-8')

    csv = convert_df(dataf)

    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name='large_df.csv',
        mime='text/csv',
    )

