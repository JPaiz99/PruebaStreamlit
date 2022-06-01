import controlador.stream as st
import controlador.Lamina_agua as la
import streamlit as st
from PIL import Image

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # st.streaml()
    image= Image.open("recursos\logo-magdalena.png")
    st.set_page_config(page_title="Lamina Agua IMSA", page_icon=image)
    la.main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
