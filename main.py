# This is a sample Python script.
# importing libraries
from __future__ import print_function
import glob
import sys
import io
import subprocess
import warnings
from datetime import date
from sys import exit
import docx
import msoffcrypto
import numpy as np
import pandas as pd
import streamlit as st
from PIL import Image
from mailmerge import MailMerge
from modules import *
from io import BytesIO



def upload_locked(uploaded_file, password):
    decrypted_workbook = io.BytesIO()
    office_file = msoffcrypto.OfficeFile(uploaded_file)
    office_file.load_key(password=password)
    office_file.decrypt(decrypted_workbook)
    # `filename` can also be a file-like object.
    workbook = pd.read_excel(decrypted_workbook)
    return workbook


def main():
    # reading the logo image
    st.set_page_config(
    initial_sidebar_state="expanded", 
    page_title = 'מערכת לניטור דיווחים בלתי רגילים',
    page_icon = 'fav.jfif',
    menu_items = {'Report a bug': 'mailto:talya@tovtech.org',
                  'Get help': None,
                  'About': 'https://tovtech.org/en'})

    hide_streamlit_style = """
                <style>
                footer {visibility: hidden;}
                </style>
                """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)

    logo, title_text = st.columns([1, 3])

    with logo:
        st.markdown("""
            <a href = 'https://tovtech.org/en/'>
            <img src='https://tovtech.org/wp-content/uploads/2022/04/3-tovtech-logo-blue-e1651390125630.png', width = "100%"></a>
        """, unsafe_allow_html  = True)

    with title_text:
        st.header('מערכת לניטור דיווחים בלתי רגילים')


 
    top_left, top_right = st.columns(2)
    # a widget for reading multiple files
    with top_left:
        info_upload = st.file_uploader('Choose **Business Info** file:')
        if info_upload is not None:
            business_info = pd.read_excel(info_upload)

            # defining the "fields" column of the business_info df as index
            business_info = business_info.set_index('fields')

            # defining password as a variable containing the clients' id number
            password = business_info.loc['מספר מזהה של הגורם המדווח'].values[0]

        tr_upload = st.file_uploader('Choose **Transaction** file:')
        if tr_upload is not None:
            try:
                check = upload_locked(tr_upload, str(password))
            except:
                check = pd.read_excel(tr_upload)


    with top_right:
        client_upload = st.file_uploader('Choose **Clients** file:')
        if client_upload is not None:
            try:
                client = upload_locked(client_upload, str(password))
            except:
                client = pd.read_excel(client_upload)

        reported_upload = st.file_uploader('Choose **Reported** file:')
        if reported_upload is not None:
            try:
                reported = upload_locked(reported_upload, str(password))
            except:
                reported = pd.read_excel(reported_upload)


    st.markdown('---')


    module = st.selectbox('Please choose the desired module',
                          options=['Yeshut', 'Cox', 'Changemat', 'G_Money', 'Yeshut Exchange'])

    st.markdown("""<style>
                    div.stButton > button:first-child {
                    width : 100px;
                    height:40px;}</style>""", unsafe_allow_html=True)


    col1, col2, col3 = st.columns([2, 1, 2])
    with col2:
        run = st.button('Run', type='primary')
    if run:
        if module == 'Yeshut':
            run_yeshut(check, client, business_info, reported)
        elif module == 'G_Money':
            run_gmt(check, business_info, reported)
        elif module == 'Changemat':
            run_changemat(check, client, business_info, reported)
        elif module == 'Cox':
            run_cox(check, client, business_info, reported)
########## FOOTER ##########

    footer = st.container()

    with footer:

        st.write('[Contact Us 📝](mailto:talya@tovtech.org) | [Report a bug 🐛](mailto:talya@tovtech.org) | [About Us](https://tovtech.org/en)')
if __name__ == '__main__':
    main()