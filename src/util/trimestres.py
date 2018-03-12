# _*_ coding: utf-8 *_*
#Autor: Darwin Rosero Vaca

"""Descripción: sintetizar las tablas de treimestrales
"""
import numpy as np
import pandas as pd
from glob import glob

class Trimestres():
    def __init__(self):
        """Constructor """

    def sintetizar(self,pathFile,pathcoor):
        tablaT=pd.read_csv(pathFile,sep=";",encoding="utf-8")
        estaciones=pd.read_csv(pathcoor,sep=";",encoding="utf-8")

        for e in range(0,len(estaciones)):
            print(estaciones.iat[e,0],estaciones.iat[e,1],estaciones.iat[e,2],estaciones.iat[e,3])
            df=[]

    def separaEs(self,archivo,rutaguradar,sufijo):
        print(archivo)
        total=pd.read_csv(archivo,sep=";")
        codigos=total['codigo'].unique()
        contar =0

        for c in codigos:
            contar += 1
            guardar=total[total['codigo']==c]
            #print(guardar.iat[0,0]," -- ",contar)
            guardar.to_csv(rutaguradar+guardar.iat[0,0]+sufijo,sep=";",encoding="utf-8")

        for i in self.listFile(rutaguradar,sufijo=sufijo):
            print(i)

    def listFile(self,filePath, prefijo="nada", sufijo="nada"):
        """Lee los archivos dado un directorio y un sufijo igual para todo los archivos"""
        #print("def listFile(self,filePath, prefijo=\"nada\", sufijo=\"nada\"):")
        #print(prefijo,"",sufijo)
        if prefijo == "nada":
            return glob(filePath+"*"+sufijo)
        else:
            return glob(filePath+""+prefijo+"*")





t=Trimestres()

pathFile="/home/darwin/Documentos/Chrips/Tmax/temp-max-trim.csv"
pathcoor="/home/darwin/Documentos/Chrips/Tmax/coor.csv"
#t.sintetizar(pathFile,pathcoor)
archvio="/home/darwin/Descargas/maxt.csv"
rutaguradar="/home/darwin/Documentos/Chrips/Tmax/"
t.separaEs(archvio,rutaguradar,"_Tmax.csv")
##minimas
archvio="/home/darwin/Descargas/mint.csv"
rutaguradar="/home/darwin/Documentos/Chrips/Tmin/"
t.separaEs(archvio,rutaguradar,"_Tmin.csv")
##medias
archvio="/home/darwin/Descargas/medt.csv"
rutaguradar="/home/darwin/Documentos/Chrips/Tmedia/"
t.separaEs(archvio,rutaguradar,"_Tmed.csv")