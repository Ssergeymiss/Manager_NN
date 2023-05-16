import numpy as np
import pandas as pd
from tensorflow import keras
from sklearn.preprocessing import MinMaxScaler
import streamlit as st
import pandas as pd
import xlsxwriter
import mysql.connector

st.title("Анализ деятельности менеджеров")
def fetch_table_data(table_name):

    cnx = mysql.connector.connect(
        host="193.164.150.80",
        database='fedinst_office',
        user='user_view',
        password='vH56ui2GdKHQ7Em'
    )

    cursor = cnx.cursor()

    if table_name == 'qq85_sttlkofficeinvoice':
        cursor.execute('select id, user_id from ' + table_name)
    if table_name == 'qq85_sttlkofficepayment':
        cursor.execute('select invoice_id from ' + table_name)
    if table_name == 'qq85_stthomeoffice_label':
        cursor.execute('select * from ' + table_name)

    header = [row[0] for row in cursor.description]

    rows = cursor.fetchall()


    cnx.close()

    return header, rows


def export(table_name):

    workbook = xlsxwriter.Workbook(table_name + '.xlsx')
    worksheet = workbook.add_worksheet('MENU')

    # Create style for cells
    header_cell_format = workbook.add_format({'bold': True, 'border': True, 'bg_color': 'yellow'})
    body_cell_format = workbook.add_format({'border': True})

    header, rows = fetch_table_data(table_name)

    row_index = 0
    column_index = 0

    for column_name in header:
        worksheet.write(row_index, column_index, column_name, header_cell_format)
        column_index += 1

    row_index += 1
    for row in rows:
        column_index = 0
        for column in row:
            worksheet.write(row_index, column_index, column, body_cell_format)
            column_index += 1
        row_index += 1

    print(str(row_index) + ' rows written successfully to ' + workbook.filename)
    st.write(table_name + " Успешно загружено")

    # Closing workbook
    workbook.close()

def refactor_data(data_path,name_new):
    read_file = pd.read_excel (data_path+".xlsx")
    read_file.to_csv (name_new,
                      index = None,
                      header=True)


# Загружаем необходимые таблицы из БД
import os
if os.path.isfile("qq85_sttlkofficeinvoice.xlsx") == False:
    export('qq85_sttlkofficeinvoice')#Заявки на услуги
    refactor_data('qq85_sttlkofficeinvoice', "invoice.csv")

if os.path.isfile("qq85_sttlkofficepayment.xlsx") == False:
    export('qq85_sttlkofficepayment')#Заявки на услуги
    refactor_data('qq85_sttlkofficepayment', "payment.csv")
    
if os.path.isfile("qq85_stthomeoffice_label.xlsx") == False:
    export('qq85_stthomeoffice_label')#Заявки на услуги
    refactor_data('qq85_stthomeoffice_label', "label.csv")

# export('')#Оплаченные заявки
# export('qq85_stthomeoffice_label')#Лэйблы

# позволяет преобразовать данные из xlsx to csv










# В этом блоке мы выполняем подсчет общего количества менеджеров с заявками

invoice=pd.read_csv("invoice.csv")
invoice_id=invoice["id"]

invoice_id=invoice_id.tolist()

user_check_id=invoice["user_id"]
user_check_id=user_check_id.tolist()



while True:
    #print("Введите id менеджера")
    #your_name = st.text_input("Enter your name")
    user_id_initial = st.text_input("Введите id менеджера")
    user_id_initial=int(user_id_initial)
    if user_id_initial in user_check_id:
        #print("Введите начальный invoice_id")
        id_initial =  st.text_input("Введите invoice_id")
        id_initial=int(id_initial)
        if id_initial in invoice_id:
            break
        else:
            continue
    else:
        continue


invoice=invoice[invoice.user_id==user_id_initial]
invoice=invoice[invoice.id>=id_initial]

st.write("Сформированы записи по менеджеру")
st.write(invoice.head(),"Количество записей",len(invoice),sep="\n")


user_id=invoice["user_id"]
unik_user=user_id.unique()

Manager_df={}

user_count=user_id.tolist()

for i in unik_user:
    Manager_df[i] = user_count.count(i)

ls = list(Manager_df.items())# В этом списке хранится: 1-й элемент- id, 2-й элемент количество заявок на менеджера


df_M=pd.DataFrame(columns=["user_id","count"])


for i in range(len(ls)):
   df_M.loc[i] = ls[i]# в этом датафрейме хранятся по колонкам идентификатор и количество заявок




#В этом блоке мы считаем количество успешных заявок на менеджера

payment=pd.read_csv("payment.csv")
#print(payment.shape)
invoice_id=payment["invoice_id"]# в таблице Пэймент хранятся id не менеджеров, а операций,
# поэтому мы сопоставляем invoice id в таблице payment и id в таблице invoice

invoice_len=invoice["id"]
invoice_id=invoice_id.tolist()


soft_df=pd.DataFrame()

soft_df[["user_id","invoice_id"]]=invoice[["user_id","id"]]

DF_iskl=soft_df[soft_df.user_id !=0]# Исключим нулевые строки в датафрейме
DF_iskl=soft_df.loc[soft_df.invoice_id.isin(invoice_id)==True]# Исключим из копии таблицы invoice все значения,
# которые отсуттсвуют в таблице payment. Так мы получим только те значения, которые превратились в оплаченные заявки


iskl_unik=DF_iskl["user_id"].unique()#Получим уникальных менеджеров в датасете

df_iscl={}



user_count_iscl=DF_iskl["user_id"].tolist()#Переведем все строки с user id в список и посмотрим количество вхождений

for i in iskl_unik:

    df_iscl[i] = user_count_iscl.count(i)

ls_iscl = list(df_iscl.items())


df_end=pd.DataFrame(columns=["user_id","count"])


for i in range(len(ls_iscl)):
   df_end.loc[i] = ls_iscl[i]# В этом датафрейме хранятся значения менеджеров которые совершили успешные сделки(кол-во)





df_M=df_M.loc[df_M.user_id.isin(df_end["user_id"])==True]# Исключим из датасета, где хранятся общее количество заявок
#Всех менеджеров, которые не совершили успешные сделки

df_M=df_M.sort_values(by=["user_id"])


df_end=df_end.sort_values(by=['user_id'])


count=df_M["count"].tolist()# наши датасеты отсортированы и равны, => перенесем колонку с общим количеством в датафрейм
# df_end
df_end["count_all"]=count


convers=[]

for i in range(len(df_end["user_id"])):
    convers.append(df_end["count"][i]/df_end["count_all"][i])# Считаем конверсию

df_end["convers"]=convers# Добавим столбец Конверсии в наш датафрейм

user_end=df_end["user_id"]# Получим список менеджеров


# Получим количество лэйблов
label=pd.read_csv("label.csv")



df_label=label.loc[label.user_id.isin(df_end["user_id"])==True]# Исключим из датафрейма все значения, которые не соответствуют таблице DF_END


user_label=df_label["user_id"].unique()
user_label=sorted(user_label)

df_end=df_end.loc[df_end.user_id.isin(user_label)==True]# сключим из датафрейма все значения, которые не соответствуют таблице df_label



df_label=df_label.sort_values(by=["user_id"])

df_end = df_end.reset_index(drop=True)
df_label = df_label.reset_index(drop=True)



labels=[]
for i in range(len(df_end["user_id"])):#Добавим в список среднее значения всех лэйблов на менеджера
    labels.append(#[df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label1'].mean(),
                   #df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label2'].mean(),
                   #df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label3'].mean(),
                   [df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label4'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label5'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label6'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label7'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label8'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label9'].mean(),
                   #df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label10'].mean(),
                   #df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label11'].mean(),
                   #df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label12'].mean(),
                  # df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label13'].mean(),
                   #df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label14'].mean(),
                   #df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label15'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label16'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label17'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label18'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label19'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label20'].mean(),
                   #df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label21'].mean(),
                   #df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label22'].mean(),
                   #df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label23'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label24'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label25'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label26'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label27'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label28'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label29'].mean(),
                   ])




col=[#"label1","label2","label3",
     "label4","label5","label6","label7","label8","label9",
     #'label10',"label11","label12","label13","label14","label15",
     "label16","label17","label18","label19",'label20',
     #"label21","label22","label23",
     "label24","label25","label26","label27","label28","label29"]



Averange_M=pd.DataFrame(labels,columns=col)

A_user_id=df_end["user_id"].tolist()
Averange_M.insert (loc= 0 , column='user_id', value=A_user_id)

A_convers=[]
for i in range(len(df_end["user_id"])):
    A_convers.append(df_end["count"][i]/df_end["count_all"][i])


print("Вы хотите сформировать анализ?\n Введите 1 если да, 0 если нет")
answer =  st.text_input("Нажмите 1 если вы хотите сформировать анализ")
answer=int(answer)

if answer==0:
    Averange_M["convers"]=A_convers
    Averange_M.to_excel("Averange_M_df.xlsx")
    refactor_data('Averange_M_df', "Averange_M_df.csv")

else:
    Averange_M.to_excel("Averange_M.xlsx")
    refactor_data('Averange_M',"Averange_M.csv")













scaler=MinMaxScaler()
# def load_dataset():
#     """Создание формы для загрузки изображения"""
#     # Форма для загрузки изображения средствами Streamlit
#     data_file = st.file_uploader("Upload CSV",type=["csv"])
#     if data_file is not None:
#
#         df = pd.read_csv(data_file)
#         st.dataframe(df)
#     return df
# Выводим заголовок страницы средствами Streamlit

# Вызываем функцию создания формы загрузки изображения


def load_model():
    model = keras.models.load_model("model_Man_3.2.h5")
    model.load_weights("Manager_weight_4.2.h5")
    if model:
        st.write("Модель загружена!")
    return model

def Preprocess_df(arr):
    st.write("Усредненные значения меток")
    arr=arr.drop(arr.columns[[0]], axis=1)
    st.write(arr)
    arr = arr.astype(float)
    arr = arr.to_numpy()
    arr = np.array(arr)

    #arr=arr.drop(["Unnamed: 0","user_id"],axis=1)

    #arr=scaler.fit_transform(arr)
    return arr

def print_predictions(preds):
    print(preds)



model = load_model()



result = st.button('Провести анализ')

import os
if result:
    # Предварительная обработка изображения
    x = Preprocess_df(Averange_M)
    # Распознавание изображения
    preds = model.predict(x)
    # Выводим заголовок результатов распознавания жирным шрифтом
    # используя форматирование Markdown
    st.write('**Результаты распознавания:**')
    # Выводим результаты распознавания
    #print(preds)
    st.write(preds)
    # os.remove("Averange_M.csv")
    # os.remove("Averange_M.xlsx")


# from lime import lime_tabular
# df_lime=pd.read_csv("D:\encoding\Manager_Analysys\Averange_M_df.csv")
# df_lime=df_lime.drop(["user_id"],axis=1)
#
# df_lime=df_lime.astype(float)
#
# X=df_lime.drop(["convers"],axis=1)
# y=df_lime["convers"]
#
# X,y=X.to_numpy(),y.to_numpy(y)
# X=np.array(X)
# y=np.array(y)
#
# X=scaler.fit_transform(X)
#
# from sklearn.model_selection import train_test_split
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0,shuffle=True)
#
#
# df_an=df_lime.drop(["convers"],axis=1)
# explainer = lime_tabular.LimeTabularExplainer(X_train, mode="regression", feature_names= df_an.columns)
#
# import random
#
# idx = random.randint(1, len(X_test))
#
# print("Prediction : ", model.predict(X_test[idx].reshape(1,-1)))
# print("Actual :     ", y_test[idx])
#
# explanation = explainer.explain_instance(X_test[idx], model.predict, num_features=len(df_an.columns))
#
# import matplotlib.pyplot as plt
# with plt.style.context("ggplot"):
#     explanation.as_pyplot_figure()
# plt.show()






