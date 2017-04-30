import os

def getCitiBikeCSV(datestring):
    '''The function downloads a CSV file into the PUIDATA directory from the Citibike database for the given datestring
    Author: vys217 lifting code from 
    https://github.com/fedhere/PUI2016_fb55/blob/master/HW3_fb55/citibikes_gender.ipynb
    '''
    if not os.path.isdir("../../Data/Citibike"):
        os.system("mkdir ../../Data/Citibike") 
    
    if os.path.isfile( "../../Data/Citibike/" + datestring + "-citibike-tripdata.csv"):
        print "File "+ datestring + "-citibike-tripdata.csv is in Data/Citibike folder already" 
    else:
        os.system("curl -O https://s3.amazonaws.com/tripdata/" + datestring + "-citibike-tripdata.zip")
        os.system("mv " + datestring + "-citibike-tripdata.zip ../../Data/Citibike/" )
            ### unzip the csv 
        os.system("unzip ../../Data/Citibike/" + datestring + "-citibike-tripdata.zip")
        print("unzip ../../Data/Citibike/" + datestring + "-citibike-tripdata.zip")
        
        os.system("mv " + datestring + "-citibike-tripdata.csv ../../Data/Citibike/")
        
            ## NOTE: old csv citibike data had a different name structure. 
        if ('2013' in datestring) | ('2014' in datestring):
            os.system("mv " + datestring[:4] + '-' +  datestring[4:] + 
                          "\ -\ Citi\ Bike\ trip\ data.csv " + datestring + "-citibike-tripdata.csv")
            os.system("mv " + datestring + "-citibike-tripdata.csv ../../Data/Citibike/")
    
    ### One final check:
    if not os.path.isfile("../../Data/Citibike/" + datestring + "-citibike-tripdata.csv"):
        print ("WARNING!!! something is wrong: the file is not there!")

    else:
        print ("file in place, you can continue")
		



#Get Data for 2013		
getCitiBikeCSV('201307')
getCitiBikeCSV('201308')
getCitiBikeCSV('201309')
getCitiBikeCSV('201311')
getCitiBikeCSV('201312')		

#Get Data for 2014
getCitiBikeCSV('201401')
getCitiBikeCSV('201402')
getCitiBikeCSV('201403')
getCitiBikeCSV('201404')
getCitiBikeCSV('201405')
getCitiBikeCSV('201406')
getCitiBikeCSV('201407')
getCitiBikeCSV('201408')
getCitiBikeCSV('201409')
getCitiBikeCSV('201410')
getCitiBikeCSV('201411')
getCitiBikeCSV('201412')

#Get Data for 2015
getCitiBikeCSV('201501')
getCitiBikeCSV('201502')
getCitiBikeCSV('201503')
getCitiBikeCSV('201504')
getCitiBikeCSV('201505')
getCitiBikeCSV('201506')
getCitiBikeCSV('201507')
getCitiBikeCSV('201508')
getCitiBikeCSV('201509')
getCitiBikeCSV('201510')
getCitiBikeCSV('201511')
getCitiBikeCSV('201512')

#Get Data for 2016
getCitiBikeCSV('201601')
getCitiBikeCSV('201602')
getCitiBikeCSV('201603')
getCitiBikeCSV('201604')
getCitiBikeCSV('201605')
getCitiBikeCSV('201606')
getCitiBikeCSV('201607')
getCitiBikeCSV('201608')
getCitiBikeCSV('201609')
getCitiBikeCSV('201610')
getCitiBikeCSV('201611')
getCitiBikeCSV('201612')
