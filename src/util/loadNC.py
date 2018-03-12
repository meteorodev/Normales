# _*_ coding: utf-8 *_*
#Autor: Darwin Rosero Vaca

"""Descripción: genera archivos de datos en base a dataset de netcdf

descarga de datos modo experto

  SOURCES .UCSB .CHIRPS .v2p0 .monthly .global .precipitation
  X (82W) (74W) RANGEEDGES
  Y (3N) (6S) RANGEEDGES
  T (Jan 1981) (Feb 2018) RANGEEDGES
link de descarga

https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0/.monthly/.global/.precipitation/X/%2881.5W%29%2874E%29RANGEEDGES/T/%28Jan%201981%29%28Jan%202017%29RANGEEDGES/Y/%283N%29%286S%29RANGEEDGES/X/%2881.5W%29%2874w%29RANGEEDGES/data.nc

"""
from _threading_local import local

import numpy as np
import netCDF4 as nc
import pandas as pd
from django.template.defaulttags import verbatim


class LoadNC():
    """"""

    def __init__(self,):
        """Constructor for LoadNC"""
    def readNcChirps(self, rutaNC, añoIn, latEst, lonEst, varnc):
        """Lee el archivo NC de datos mesuales chrips de precipitacin y genera una serie mensual
         con las coordenadas de una estacion"""
        dataset=nc.Dataset(rutaNC)
        #print("leyendo el netcdf\n imprimiendo el metadato")
        #print(dataset.file_format)
        #print(dataset.dimensions.keys())
        #print(dataset.dimensions['T'])
        #print(dataset.variables.keys())
        #print(dataset.variables['precipitation'])
        #print(dataset.variables)
        """longitud = X, latitud = Y Precipitacion = precipitation"""
        #dataset.
        lat = dataset.variables['Y'][:]
        lon = dataset.variables['X'][:]
        #rr = dataset.variables['precipitation'][:]
        #rr = rr.filled(np.nan)
        #rr_units = dataset.variables['precipitation'].units
        #print("lat : ",len(lat),"lon: ",len(lon))
        #print("units" ,rr_units)
        #print("rr length ",len(rr))

        ##time serie
        cadena = str(añoIn)+"-01-01"
        fechas = pd.date_range(start=cadena,periods=len(dataset.variables['T']),freq="MS")
        #print(fechas)
        #print("tipo de datos ", type(lon))
        #cordNC = self.findCoor(lat,lon,lonp=-78.17830278,latp=0.1783027778)
        cordNC = self.findCoor(lat, lon, latEst,lonEst)
        #print(cordNC.items())
        print("coordenadas encontradas  ==> ",cordNC["coor"], " posiciones ==> ", cordNC["pos"])
        #get all values for this lat and lon
        rr=dataset.variables[varnc][:,cordNC["pos"][0],cordNC["pos"][1]]
        #print("rr length ",len(rr),"\n valores de RR \n",rr)
        #self.getDataAsfile(rr, lat, lon)
        data={"fecha":fechas,"valor":rr}
        return pd.DataFrame(data)


    def findCoor(self,latnc, lonnc, latp, lonp):
        """Retorna un serie de tiempo desde el netcdf dada un latitud y longitug"""

        #print(latp," metodo findCoor ", lonp)
        ncmx = np.where(latnc >= latp)
        mx=len(ncmx[0])
        latncb =[latnc[mx-1],latnc[mx]]
        #print("latitudes ", latncb)
        a = abs(latncb[0]) - abs(latp)
        b = abs(latp) - abs(latncb[1])
        corfin=[]
        pos=[]
        if a > b:
            corfin.append(latncb[1])
            pos.append(mx)
        else:
            corfin.append(latncb[0])
            pos.append(mx-1)
        #print("********************************")
        #print(lonnc)
        ncmx = np.where(lonnc <= lonp)
        #print(lonnc[ncmx])
        mx = len(ncmx[0])
        lonncb = [lonnc[mx-1], lonnc[mx]]
        a = abs(lonncb[0]) - abs(lonp)
        b = abs(lonp) - abs(lonncb[1])
        if a > b:
            corfin.append(lonncb[1])
            pos.append(mx)
        else:
            corfin.append(lonncb[0])
            pos.append(mx-1)
        #print("longitudes ",lonncb)
        #print("############################################")
        return {"coor":corfin,"pos":pos}

    def readNcAgmipDaily(self, rutaNC,añoNc, latEst, lonEst,varnc):
        """Lee el archivo NC de datos mesuales chrips de precipitacin y genera una serie mensual
                 con las coordenadas de una estacion"""
        dataset = nc.Dataset(rutaNC)
        print("leyendo el netcdf")
        """longitud = X, latitud = Y"""
        # dataset.
        lat = dataset.variables['Y'][:]
        lon = dataset.variables['X'][:]
        datanc = dataset.variables[varnc][:]
        ##time serie
        cadena = str(añoNc) + "-01-01"
        fechas = pd.date_range(start=cadena, periods=len(dataset.variables['T']), freq="D")
        #print(fechas)
        #findCoor(lat,lon,lonp=-78.17830278,latp=0.1783027778)
        cordNC = self.findCoor(lat, lon, latEst, lonEst)
        # print(cordNC.items())
        print("coordenadas encontradas  ==> ", cordNC["coor"], " posiciones ==> ", cordNC["pos"])
        # get all values for this lat and lon
        datanc = dataset.variables[varnc][:, cordNC["pos"][0], cordNC["pos"][1]]
        #print("datanc length ",len(datanc),"\nvalores de datanc \n",datanc)
        # self.getDataAsfile(rr, lat, lon)
        data = {"fecha": fechas, "valor": datanc}
        return self.dailyToMount(pd.DataFrame(data),añoNc)


    def dailyToMount(self, dailyDF,añoNc):
        """de un dataframe diario genera un dataframe mensual """
        """15:50:00   21:47:00"""
        ##calculamos el numero de mese y años en base a la longitud del df
        #print("def dailyToM ")
        años = int(len(dailyDF) / 365)
        mese = años * 12
        cadena = str(añoNc) + "-01-01"
        fechasM = pd.date_range(start=cadena, periods=mese, freq="MS",name="fecha")
        dfs = []
        #print(fechasM)
        for m in range(0,mese):
            #print(fechasM[m],m,"",mese)
            if m == mese-1:
                mesDato=dailyDF[(dailyDF['fecha'] >= fechasM[m] )]
                tmes = np.mean(mesDato['valor'])
            else:
                mesDato=dailyDF[(dailyDF['fecha'] >= fechasM[m]) & (dailyDF['fecha'] < fechasM[m + 1])]
                tmes = np.mean(mesDato['valor'])
            #print(mesDato.iat[0,0]," - ",mesDato,"\nmensual ",tmes,"\n_____________________________________________________________")
            #dfT={"fecha":[mesDato.iat[0,0]],"valor":[tmes]}
            dfs.append(pd.DataFrame({"fecha":[mesDato.iat[0,0]],"valor":[tmes]}))
        big_frame = pd.concat(dfs)
        return big_frame


#lonEst= -78.17830278
#latEst=0.1783027778

#lnc= LoadNC()
#print(lnc.readNc("/home/drosero/Descargas/rrchirps.nc",1981,latEst, lonEst,"precipitation"))
#lonEst= -78.17830278
#latEst= 0.25
#pathnc = "/home/darwin/Documentos/AGMERRA/TmaxAGM.nc"
#lnc.readNcAgmipDaily(pathnc,1980,latEst,lonEst,"tmax")