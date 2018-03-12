# _*_ coding: utf-8 *_*
#Autor: Darwin Rosero Vaca
#Descripción:
from glob import glob
import pandas as pd
import numpy as np
from src.util import loadNC as fnc
from urllib.request import urlopen
import os.path as pth

class LoadData():
    """ lee textos con pandas a trata como matrices"""
    def __init__(self,):
        """Constructor for LoadData"""

    #Genera un archivo de tablas de valores, normales y anomalias
    def loadRR(self, filePath, sufijo):
        """filtra los archivos por un sufijo"""
        fileList=self.listFile(filePath,sufijo)
        dfs = []
        for a in fileList:
        #for i in range(1):
            #a=fileList[i]
            print("leyendo ... ",a," ....")
            leido=pd.read_csv(a,sep=";",usecols=range(1,15))
            """transforma los años double a int"""
            leido['anio'] = leido['anio'].apply(lambda x: int(x))
            """calcula las normales tomando de la fial 0 a la 29 """
            normal=leido[(leido['anio'] < 2011)]
            normal=normal[normal.columns[2:]]
            normalCal=np.mean(normal)
            #agrega las normales
            for col in normal.columns:
                leido[col+'_N']=normalCal[col]
            #Agrega las variaciones mediante ((valor/normal)-1)*100
            for col in normal.columns:
                print(leido[col+'_N'])
                leido[col+'_V']=((leido[col]/normalCal[col])-1)*100

            print(normalCal)
            print("soy una normal **************")
            print("*****************************")
            print("*****************************")
            print(normal)
            print("fin de la normal *******************")
            print("************************************")
            print("************************************")

            print(leido)
            print("fin de la lectura ##################")
            print("####################################")
            print("####################################")
            dfs.append(leido)
        #Concatenate all data into one DataFrame
        big_frame = pd.concat(dfs, ignore_index=True)
        print(big_frame)
        big_frame.to_csv(filePath+"precip.csv", sep=';',encoding='utf-8',index=False)
        return big_frame

    def loadTempAgmerra(self, filePath, pathnc, añoNc, var, prefijo="nada", sufijo="nada", factor=0):
        """Genera un archivo con normales de temperaturra y valores
        """
        fileList = self.listFile(filePath,prefijo, sufijo)
        print("numero de archivos ",len(fileList))
        """leyeno el nc de agmerra para temperaturas"""
        ncclass = fnc.LoadNC()
        #lnc.readNcAgmipDaily(pathnc, 1980, latEst, lonEst, "tmax")
        #ncSerie = ncclass.readNcAgmipDaily(pathnc,añoNc)
        coorEstaciones = pd.read_csv(filePath+"coor.csv",sep=";")
        dfs = []
        for a in fileList:
            leido = pd.read_csv(a, sep=";", usecols=range(1, 15))
            """transforma los años double a int"""
            leido['anio'] = leido['anio'].apply(lambda x: int(x))
            coorleida=coorEstaciones[coorEstaciones['estacion']==leido.iat[0,0]]
            print(coorleida)
            ncSerie = ncclass.readNcAgmipDaily(pathnc, añoNc,coorleida.iat[0,1],coorleida.iat[0,2],var)
            leido = self.comAños(1981, 2017, leido, ncSerie, factor)
            #print(leido)
            """calcula las normales tomando de la fial 0 a la 29 """
            normal=leido[(leido['anio'] < 2011)]
            normal=normal[normal.columns[2:]]
            normalCal=np.mean(normal)
            #agrega las normales
            for col in normal.columns:
                leido[col+'_N']=np.around(normalCal[col],decimals=1)
            #Agrega las variaciones mediante ((valor/normal)-1)*100
            for col in normal.columns:
                print(leido[col+'_N'])
                leido[col+'_V']=leido[col]-normalCal[col]

            dfs.append(leido)
        big_frame = pd.concat(dfs, ignore_index=True)
        print(big_frame)
        big_frame.to_csv(filePath + "temp"+sufijo.replace("_T","-"), sep=';', encoding='utf-8', index=False)
        return big_frame

    def comAños(self, añoI, añoF, serie, ncSerie, factor=0.0):
        """Completa los años faltantes en el dataframe
        @params
        añoI = año de inicio para los datos
        añoF = año de fin para los datos
        serie = serie original con datos faltantes
        ncSerie = serie a utilizar para el relleno
        factor = factor de multiplicacion o agregacion defecto = 0.0
        """
        print("def comAños(self, añoI, añoF, serie, ncSerie, factor=0.0):")
        head = ["codigo", "anio", "ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
        time = list(range(añoI, añoF + 1))
        # nos asegurameo qeu el dataframe este completo
        meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
        inicio = True
        #print(ncSerie)
        codigoEst  = serie.iat[0,0]
        dfs = []
        for i in time:
            buscado = serie[(serie["anio"] == i)]
            #print(i," i , Buscado ",buscado)
            if buscado.empty:
                datames = [codigoEst, i]
                for j in meses:
                    dnc = ncSerie[(ncSerie["fecha"] == str(i) + '-' + j + '-01')]
                    if dnc.empty:
                        datames.extend([np.nan])
                    else:
                        datames.extend([np.round(dnc.iat[0, 1],decimals=1)+factor])
                #print(datames)
                temdf = pd.DataFrame(data=datames,index=head).T;
                #print(temdf)
            else:
                temdf = buscado
                for k in range(0,12):
                    if np.isnan(buscado.iat[0,(k+2)]):
                        dnc = ncSerie[(ncSerie["fecha"] == str(i) + '-' + meses[k] + '-01')]
                        if dnc.empty != True:
                            buscado.iat[0, (k + 2)]=np.round(dnc.iat[0, 1],decimals=1)+factor

                #print("Frame relleno ",buscado)
            dfs.append(temdf)
            inicio = False
        big_frame = pd.concat(dfs, ignore_index=True)
        #print(big_frame)
        return big_frame

    def loadTempNasa(self, nasaPath,filePath, prefijo="nada", sufijo="nada", var="tmax" ,factor=0, añoI=1981,añoF=2017):
        """Genera un archivo con normales de temperaturra y anomalias
        rellenando con datos obtenidos de la nasa
        @params
        self
        nasaPath
        filePath
        prefijo="nada"
        sufijo="nada"
        var="tmax"
        factor=0
        añoI=1981
        añoF=2017
        """
        print("def ")
        estaciones  = self.listFile(filePath, prefijo, sufijo)
        dfs = []
        for est in estaciones:
            estDf=pd.read_csv(est, sep=";", usecols=range(1, 15))
            nasafile = nasaPath + estDf.iat[0, 0] + "-NASA.csv"
            if pth.exists(nasafile):
                print("existe",nasafile ," Estacion "+ estDf.iat[0, 0])
                nasaDf = pd.read_csv(nasafile, sep=";")
                nasaDf=self.dailyToMountNASA(nasaDf,var)
                #def comAños(self, añoI, añoF, serie, ncSerie, factor=0.0):
                estDf=self.comAños(añoI,añoF,estDf,nasaDf)
                #print(estDf)
                """calcula las normales tomando de la fial 0 a la 29 """
                normal = estDf[(estDf['anio'] < 2011)]
                normal = normal[normal.columns[2:]]
                normalCal = np.mean(normal)
                # agrega las normales
                for col in normal.columns:
                    estDf[col + '_N'] = np.around(normalCal[col], decimals=1)
                # Agrega las variaciones mediante ((valor/normal)-1)*100
                for col in normal.columns:
                    #print(estDf[col + '_N'])
                    estDf[col + '_V'] = estDf[col] - normalCal[col]

                dfs.append(estDf)
        big_frame = pd.concat(dfs, ignore_index=True)
        #print(big_frame)
        big_frame.to_csv(filePath + "temp" + sufijo.replace("_T", "-"), sep=';', encoding='utf-8', index=False)
        print("Archivo ",filePath + "temp" + sufijo.replace("_T", "-"),"Guardado satisfactoriamente")
        return big_frame




    def dailyToMountNASA(self, dailyDF,var):
        """de un dataframe diario genera un dataframe mensual """
        """15:50:00   21:47:00"""
        ##calculamos el numero de mese y años en base a la longitud del df
        añoI= int(dailyDF.iat[0,0])
        años = int(len(dailyDF) / 365)
        mese = años * 12
        cadena = str(añoI) + "-01-01"
        ##Cabia el las fechas en formato entendible
        fecDia=pd.date_range(start=cadena, periods=len(dailyDF), freq="D",name="fecha")
        nDaiDf = pd.DataFrame(fecDia)
        nDaiDf['tmax'] = dailyDF['T2MX']
        nDaiDf['tmin'] = dailyDF['T2MN']
        nDaiDf['tmed'] = dailyDF['T2M']
        fechasM = pd.date_range(start=cadena, periods=mese, freq="MS",name="fecha")
        dfs = []
        print("mensualizando ",var)
        #print(nDaiDf)
        for m in range(0,mese):
            #print(fechasM[m],m,"",mese)
            if m == mese-1:
                mesDato=nDaiDf[(nDaiDf['fecha'] >= fechasM[m] )]
                tmes = np.around(np.mean(mesDato[var]),decimals=1)
            else:
                mesDato=nDaiDf[(nDaiDf['fecha'] >= fechasM[m]) & (nDaiDf['fecha'] < fechasM[m + 1])]
                tmes = np.around(np.mean(mesDato[var]),decimals=1)
            dfs.append(pd.DataFrame({"fecha":[mesDato.iat[0,0]],"valor":[tmes]}))
        big_frame = pd.concat(dfs)
        return big_frame


    def listFile(self,filePath, prefijo="nada", sufijo="nada"):
        """Lee los archivos dado un directorio y un sufijo igual para todo los archivos"""
        #print("def listFile(self,filePath, prefijo=\"nada\", sufijo=\"nada\"):")
        #print(prefijo,"",sufijo)
        if prefijo == "nada":
            return glob(filePath+"*"+sufijo)
        else:
            return glob(filePath+""+prefijo+"*")

    def getNasaTemps(self,path, filePath, añoi =1981,añof=2017):
        """descarga datos desde
        https://power.larc.nasa.gov/cgi-bin/agro.cgi?email=&latmin
        Link de datos
        https://power.larc.nasa.gov/cgi-bin/agro.cgi?email=&step=1&lat=-0.177016666666667&lon=-78.4271861&ms=1&ds=1&ys=2000&me=12&de=31&ye=2017&p=T2M&p=T2MN&p=T2MX&submit=Submit
        @params
        path = ruta para guradar los archivos generados
        filePath = ruta del archivo de informacion de la estacion
        añoi = para el inicio de la descarga, default 1981
        añof = para el fin de la descarga de datos
        """
        #lee las coordenadas de las estaciones
        coorEstaciones = pd.read_csv(filePath + "coor.csv", sep=";")
        for i in range(0,len(coorEstaciones.index)):
            print(i," <==> ",coorEstaciones.iat[i,0]," ",coorEstaciones.iat[i,1],coorEstaciones.iat[i,2])
            link = "https://power.larc.nasa.gov/cgi-bin/agro.cgi?email=&step=1&lat="+ str(coorEstaciones.iat[i, 1])\
                   +"&lon="+str(coorEstaciones.iat[i,2])+"&ms=1&ds=1&ys="+str(añoi)+"&me=12&de=31&ye="+\
                   str(añof)+"&p=T2M&p=T2MN&p=T2MX&submit=Submit"
            print(link)
            #f = urlopen(link)
            #myfile = f.read()
            data = urlopen(link)  # it's a file like object and works just like a file
            cont=0
            df=[]
            escribir = False
            ind=["YEAR","DOY","T2M","T2MN","T2MX"]
            for line in data:  # files are iterable
                l=line.decode()
                if escribir:
                    cont+=1
                    l=l.replace(" - ","NAN")
                    #print(cont,l,l[0:4], l[5:10] , l[11:18] ,l[19:25], l[27:33] )
                    data=[int(l[0:4]), int(l[5:10]), float(l[11:18]), float(l[19:25]), float(l[27:33])]
                    df.append(pd.DataFrame(data,index=ind).T)
                if "-END HEADER-" in l:
                    escribir=True
            big_frame = pd.concat(df)
            big_frame.to_csv(path+ coorEstaciones.iat[i,0]+"-NASA.csv",sep=";",encoding='utf-8',index=False)
        print(big_frame)



ld= LoadData()
filePath="/home/darwin/Documentos/Chrips/Precip/"
#ld.loadRR(filePath, "_RRfill.csv")
"""para temperaturas _Tmax.csv   _Tmin.csv"""

"""Genera estaciones en base a los datos del la NASA"""
#test de descarga de datos
filePath="/home/darwin/Documentos/Chrips/Tmax/"
nasaPath="/home/darwin/Documentos/Chrips/NASA/"
#########funcion para generar los datos#############
ld.getNasaTemps(nasaPath,filePath)
#########

#temeperatura maxima
filePath="/home/darwin/Documentos/Chrips/Tmax/"
ld.loadTempNasa(nasaPath, filePath, sufijo="_Tmax.csv", var="tmax" ,factor=0)
filePath="/home/darwin/Documentos/Chrips/Tmin/"
ld.loadTempNasa(nasaPath, filePath, sufijo="_Tmin.csv", var="tmin" ,factor=0)
filePath="/home/darwin/Documentos/Chrips/Tmedia/"
ld.loadTempNasa(nasaPath, filePath, sufijo="_Tmed.csv", var="tmed" ,factor=0)
######
####temperatura maminima
filePath="/home/darwin/Documentos/Chrips/Tmin/"
#pathnc = "/home/darwin/Documentos/AGMERRA/tminAGM.nc"
#ld.LoadTemp(filePath, pathnc,1980,"tmin",sufijo="_Tmin.csv")
pathnc="/home/darwin/Documentos/AGMERRA/tmedAGM.nc"
#ld.loadTemp(filePath, pathnc,1980,"tavg",sufijo="_Tmin.csv")
#####
#### Temperatura media
filePath="/home/darwin/Documentos/Chrips/Tmedia/"
pathnc="/home/darwin/Documentos/AGMERRA/tmedAGM.nc"
#ld.loadTemp(filePath, pathnc,1980,"tavg",prefijo="v0008",factor=2.5)

