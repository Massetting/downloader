# -*- coding: utf-8 -*-
"""
Created on Thu Jan 25 11:22:14 2018

@author: amassett
"""
import urllib
import os
import multiprocessing

desired_dates=[f for f in range(1980,2000)]
landsat=5
working=r"E:\GOULD\Landsat"
#longx=-16
#laty=-39 

###above moved here for the sake of the multiprocessing
def ncdf_down(landsat,working,longx,laty,desired_dates):
    base="http://dapds00.nci.org.au/thredds/fileServer/rs0/datacube/002/"
    basecat=r"http://dapds00.nci.org.au/thredds/catalog/rs0/datacube/002/"
    if landsat==5:
        sat=r"LS5_TM_NBAR/"
    satfile=os.path.join(base,sat)
    satcat=os.path.join(basecat,sat)
    location=r"{}_{}/".format(longx,laty)
    locationcat=r"{}_{}/catalog.html".format(longx,laty)
    parentcat=os.path.join(satcat,locationcat)    
    parent=os.path.join(satfile,location) #!!!!#LS5_TM_NBAR_3577_-16_-39_1986_v1496739200.nc
    catalog = urllib.urlopen(parentcat)
    site_data = catalog.read() #!!!!!
    
    piece=r"""<a href='catalog.html?dataset=""" 
    recompose_piece1="{}{}".format(piece,sat)
    recompose_piece2=os.path.join(recompose_piece1,location)
    filelist=[l[len(recompose_piece2):].split("'")[0] for l in site_data.split('\n') if l[:len(recompose_piece2)]==recompose_piece2]
    dates={}    
    for n in filelist:
        dates[int(n[25:29])]=n
    for year in desired_dates:
        if year in dates.keys():
            get_this=os.path.join(parent,dates[year])
            downloader(get_this,working)   
        else:    
            print("year {} not available".format(year))
        

            
def downloader(urly,working):
    if not os.path.exists(working):
        os.mkdir(working)  
    os.chdir(working)
    filen = urllib.URLopener()
    file_name = urly.split('/')[-1]
    if os.path.isfile(file_name)==False:    
        try:        
            filen.retrieve(urly, file_name)
            print("downloading " + file_name)
        except IOError:
            print("download aborted, not existing url: {}".format(urly))
            
    else:
        print("{} already present in current directory and won't be downloaded".format(file_name))                

def partial(a):
    return ncdf_down(landsat,working,a[0],a[1],desired_dates)    
    
if __name__=="__main__":
    listoflists=[[-16,-39],[-16,-38],[-15,-38],[-15,-37]]
    p = multiprocessing.Pool(4)
    p.map(partial,listoflists)
    