from __future__ import division
from typing import Optional, Iterable, TypeVar, List
from datetime import time, timedelta, datetime

import numpy as np
import pandas as pd
import math

#dateNow:datetime = datetime.utcnow()
#print(dateNow)
#dateToday = dateNow.date()
#print(dateToday)
#print((dateNow-datetime.combine(dateToday, datetime.min.time())).total_seconds() / 3600)

outputFile = 'C:\\Users\\dgeller\\Documents\\Projects\\machine_learning\\data\\Prevail2\\PythonCalc.csv'
#data = pd.read_csv('C:\\Users\\dgeller\\Documents\\Projects\\machine_learning\\data\\Prevail2\\Prevail 2 AdditionalFeatures.csv')
data:pd.DataFrame  = pd.read_csv('C:\\Users\\dgeller\\Documents\\Projects\\machine_learning\\data\\Prevail2\\Prevail II - Complete as of 3.30.18.csv')
#data:pd.DataFrame  = pd.read_csv('C:\\Users\\dgeller\\Documents\\Projects\\machine_learning\\data\\Prevail2\\Fixed_Prevail II Data with demo variables - Complete as of 1.12.18.csv')
#data:pd.DataFrame = pd.read_csv('C:\\Users\\dgeller\\Documents\\Projects\\machine_learning\\data\\Prevail2\\Fixed_Prevail II Data with demo variables - Complete as of 1.12.18short.csv')

class SmartTData(object):

     def __init__(self):
         self.ParticipantId: float = None
         self.QuitToday :Optional[float] = None
         self.hrsSnceLstQuit :Optional[float] = None
         self.index :float = None
         self.CigsJustNow :Optional[float] = None
         self.CigsToday1:Optional[float] = None
         self.emaTakenTime :datetime = None
         self.minHrsLastCig :Optional[float] = None
         self.maxHoursSinceLastCig :Optional[float] = None
         self.ScheduledTimeDecimal :Optional[float] = None
         self.StartTimeDecimal :Optional[float] = None
         self.LastSmoke :Optional[float] = None
         self.CigsYest :Optional[float] = None
         self.Day :int = 0 
         self.hoursUntilMinLastCig:Optional[float] = None
         self.fieldUsedForLastCig:str =  None
         self.LapseFlag :bool = False 
         self.RecordTYpe:str = "python"

     def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)
    
     def getHeaders(self):
        return  str(self.__dict__.keys()).replace("dict_keys([","",1) + "\n"

     def getRows(self):
        return  str(self.__dict__.values()).replace("dict_values([","",1)[0:-2] + "\n"

def IsNotNoneOrNan(val) -> bool:
    return (val is not None and (not isinstance(val,float) or not math.isnan(val)))

def SetListEmaTakenTime(dataList:List[SmartTData]): 
    baseTime =datetime.strptime('Jan 1 1900  12:00AM', '%b %d %Y %I:%M%p') 
    indexesToRemove:List[int] = []
    for dlIndex in range(len(dataList)):
        dataList[dlIndex].emaTakenTime = baseTime + (timedelta(days=dataList[dlIndex].Day) + timedelta(seconds=round(dataList[dlIndex].ScheduledTimeDecimal * 60*60, 0)) ) 

def FilterByStartTime(dataList:List[SmartTData]): 
    for el in dataList:
        if(el.StartTimeDecimal is not None): yield el

def CalculateSmartFeatures(dataList:List[SmartTData]) -> List[SmartTData]: 
    #groupedAndOrderedItems:List[SmartTData] = dataList #(from dlItem in dataList group dlItem by dlItem.Subject into grp select grp.ToList()).ToList()
    outerIndex = 1
    haveShown = False
    uniqueParticipantIds =  sorted(set(dItem.ParticipantId for dItem in dataList))
    for uniqueId in uniqueParticipantIds:
        filteredSublist = filter( lambda dItem: dItem.ParticipantId == uniqueId,dataList )
        orderedSublist:List[SmartTData] = sorted(filteredSublist, key = lambda dItem: dItem.emaTakenTime)  
        #= dataListlItem.OrderBy(ga => ga.emaTakenTime).Select((ga, indx) => new  ga, indx ).ToList()
        #region assign hours since last cig and deviation values
        for indx,v in enumerate(orderedSublist):
            currentItem:SmartTData = v 
            currentItem.index = outerIndex
            outerIndex+=1

        
            if (indx > 0):
                priorItem = orderedSublist[indx - 1];
                if (IsNotNoneOrNan(priorItem.hrsSnceLstQuit)):
                    #currentItem.hrsSnceLstQuit = currentItem.emaTakenTime.Subtract(priorItem.emaTakenTime).TotalHours + priorItem.hrsSnceLstQuit;
                    currentItem.hrsSnceLstQuit = (currentItem.emaTakenTime-priorItem.emaTakenTime).total_seconds()/3600  + priorItem.hrsSnceLstQuit

            if (currentItem.Day <= 8):
                #currentItem.DetectingQuitAttempts = false;
                #use 9th day as an assumed quit attempt (commented out as it might be a bad assumption)
                if (indx < len(orderedSublist) - 1 and orderedSublist[indx + 1].Day == 9):
                    emaTakenDate = datetime.combine(currentItem.emaTakenTime.date(), datetime.min.time())
                    nextMidnight = (emaTakenDate  + timedelta(days = 1))
                    newTimeDelta:timedelta =(currentItem.emaTakenTime -nextMidnight) 
                    hrsSinceQuitDelta =newTimeDelta.total_seconds()/3600 
                    #if(not haveShown):
                    #    print(emaTakenDate)
                    #    print(nextMidnight)
                    #    print(newTimeDelta.total_seconds())
                    #    print(newTimeDeltaRev.total_seconds())
                    #    print(hrsSinceQuitDelta)
                    #    haveShown =True 
                    currentItem.hrsSnceLstQuit =  hrsSinceQuitDelta  
                #    currentItem.hrsSnceLstQuit = currentItem.emaTakenTime.Subtract(currentItem.emaTakenTime.Date.AddDays(1)).TotalHours;
                continue
            #else:
            #    currentItem.DetectingQuitAttempts = true;

            #if ( IsNotNoneOrNan(currentItem.QuitToday) and currentItem.QuitToday == 1):
            #    hrsSinceQuitDelta =(currentItem.emaTakenTime - (datetime.combine(currentItem.emaTakenTime.date(), datetime.min.time())  + timedelta(days = 1))).total_seconds()/3600 
            #    currentItem.hrsSnceLstQuit =  hrsSinceQuitDelta  
            #el
            #handle hours since last cig, precendence is CigsJustNow, LastSmoke, CigsYest, or hours from prior assessment
            if (IsNotNoneOrNan(currentItem.CigsJustNow ) and currentItem.CigsJustNow > 0):
                currentItem.fieldUsedForLastCig = "CigsJustNow";
                currentItem.minHrsLastCig = 0
                currentItem.maxHoursSinceLastCig = 0
            elif (IsNotNoneOrNan( currentItem.LastSmoke)and currentItem.LastSmoke > 0):
                currentItem.fieldUsedForLastCig = "lastsmoke1"
                if(currentItem.LastSmoke == 1):
                    currentItem.minHrsLastCig = 0
                    currentItem.maxHoursSinceLastCig = .25
                elif(currentItem.LastSmoke == 2):
                    currentItem.minHrsLastCig = .25
                    currentItem.maxHoursSinceLastCig = .5
                elif(currentItem.LastSmoke == 3):
                    currentItem.minHrsLastCig = .5
                    currentItem.maxHoursSinceLastCig = 1
                elif(currentItem.LastSmoke == 4):
                    currentItem.minHrsLastCig = 1
                    currentItem.maxHoursSinceLastCig = 2
                elif(currentItem.LastSmoke == 5):
                    currentItem.minHrsLastCig = 2
                    currentItem.maxHoursSinceLastCig = 4
                elif(currentItem.LastSmoke == 6):
                    currentItem.minHrsLastCig = 4
                    currentItem.maxHoursSinceLastCig = 8
                elif(currentItem.LastSmoke == 7):
                    currentItem.minHrsLastCig = 8
                    currentItem.maxHoursSinceLastCig = 8
            elif(IsNotNoneOrNan(currentItem.CigsYest) and currentItem.CigsYest > 0):
                currentItem.fieldUsedForLastCig = "CigsYest"
                if (indx > 0):
                    #//get the last answered assessment where we have cigsToday and see if we have the yesterday smoke claimed
                    #priorDaySmokeQuery = {}#(from pdq in orderedSublist where pdq.Day == currentItem.Day - 1 and pdq.ga.CigsToday1 is None select pdq).OrderByDescending(pdq => pdq.indx).ToList()
                    #filteredPriorDaySmokeQuery =filter(lambda dItem: dItem.Day == currentItem.Day - 1 and IsNotNoneOrNan(dItem.CigsToday1),orderedSublist ) 
                    #sortedPriorDaySmokeQuery =sorted(filteredPriorDaySmokeQuery,key = lambda dItem: dItem.emaTakenTime )
                    ##//assume at least midnght of yesterday since last smoke, if in the prior day we have the smoking claimed, use that instead of midnight.
                    #priorItem = orderedSublist[indx - 1]
                    #if(len(sortedPriorDaySmokeQuery) > 0 and sortedPriorDaySmokeQuery[len(sortedPriorDaySmokeQuery)-1].CigsToday1 == 1 and IsNotNoneOrNan(priorItem.minHrsLastCig)):
                    #    hoursSincePriorSmoke = (currentItem.emaTakenTime-priorItem.emaTakenTime).total_seconds()/3600 
                    #    currentItem.minHrsLastCig = hoursSincePriorSmoke + priorItem.minHrsLastCig
                    #    currentItem.maxHoursSinceLastCig = hoursSincePriorSmoke + priorItem.maxHoursSinceLastCig
                    #else:
                    currentItem.minHrsLastCig = (currentItem.emaTakenTime- datetime.combine(currentItem.emaTakenTime.date(),datetime.min.time())).total_seconds()/3600 
                    currentItem.maxHoursSinceLastCig = (currentItem.emaTakenTime-datetime.combine(currentItem.emaTakenTime.date(),datetime.min.time()) ).total_seconds()/3600 
            else:
                currentItem.fieldUsedForLastCig = ""
                if (indx > 0):
                    priorItem = orderedSublist[indx - 1]
                    hoursSincePriorSmoke = (currentItem.emaTakenTime-priorItem.emaTakenTime).total_seconds()/3600 
                    if(not IsNotNoneOrNan(priorItem.minHrsLastCig)):
                        currentItem.minHrsLastCig = None
                    else:
                        currentItem.minHrsLastCig = hoursSincePriorSmoke + priorItem.minHrsLastCig
                        
                    if(not IsNotNoneOrNan(priorItem.maxHoursSinceLastCig)):
                        currentItem.maxHoursSinceLastCig= None
                    else:
                        currentItem.maxHoursSinceLastCig = hoursSincePriorSmoke + priorItem.maxHoursSinceLastCig
                else:
                    #//for unknown
                    currentItem.minHrsLastCig =None 
                    currentItem.maxHoursSinceLastCig =None 

            #//if a person has quit smoking for 24 hours, count them as having been made a quit attempt
            #//ref: https://www.researcte.net/profile/Xiaolei_Zhou2/publication/23679263_Attempts_to_quit_smoking_and_relapse_Factors_associated_with_success_or_failure_from_the_ATTEMPT_cohort_study/links/02bfe50f86ef6cab41000000.pdf
            if (indx > 0 and (not IsNotNoneOrNan( currentItem.hrsSnceLstQuit) or (IsNotNoneOrNan(currentItem.minHrsLastCig) and currentItem.hrsSnceLstQuit > currentItem.minHrsLastCig)) and IsNotNoneOrNan(currentItem.minHrsLastCig) and currentItem.minHrsLastCig > 24 and \
                ((IsNotNoneOrNan(currentItem.CigsToday1) and currentItem.CigsToday1 == 0) or (IsNotNoneOrNan( currentItem.LastSmoke) and currentItem.LastSmoke == 0))):
                currentItem.hrsSnceLstQuit = currentItem.minHrsLastCig
            elif (IsNotNoneOrNan(currentItem.hrsSnceLstQuit) and currentItem.hrsSnceLstQuit > 0 and IsNotNoneOrNan(currentItem.minHrsLastCig) and currentItem.minHrsLastCig > 0 and currentItem.minHrsLastCig < currentItem.hrsSnceLstQuit):
                currentItem.hrsSnceLstQuit =None 
            #endregion

        #region set hoursUntilMinLastCig and lapse flag
        for osIndex in range(len(orderedSublist)-1,0, -1):
            if (osIndex > 0):
                priorItem = orderedSublist[osIndex - 1]
                thisItem = orderedSublist[osIndex]
                #//handle the last day of the study, and set minLastCigDate into the future if we don't have an immediate smoke
                if (osIndex == len(orderedSublist) - 1):
                    if ((IsNotNoneOrNan(thisItem.CigsJustNow) and thisItem.CigsJustNow > 0) or (IsNotNoneOrNan(thisItem.LastSmoke) and thisItem.LastSmoke == 1)):
                        thisItem.hoursUntilMinLastCig = 0
                        thisItem.LapseFlag = False
                    else:
                        thisItem.hoursUntilMinLastCig = None
                        thisItem.LapseFlag = False
                minHrsLastCigTotalHrs = None
                if (IsNotNoneOrNan(thisItem.minHrsLastCig )):
                    minHrsLastCigTotalHrs =  ((thisItem.emaTakenTime-timedelta(seconds=thisItem.minHrsLastCig*3600))-priorItem.emaTakenTime).total_seconds()/3600;
                hoursUntilMinLastCigTotalHrs = None
                if (IsNotNoneOrNan(thisItem.hoursUntilMinLastCig)):
                    hoursUntilMinLastCigTotalHrs = (thisItem.emaTakenTime+ timedelta(seconds = thisItem.hoursUntilMinLastCig*3600)-priorItem.emaTakenTime).total_seconds()/3600;
                closestCig = None
                if (IsNotNoneOrNan(minHrsLastCigTotalHrs)):
                    if (IsNotNoneOrNan(hoursUntilMinLastCigTotalHrs)):
                        closestCig = hoursUntilMinLastCigTotalHrs if(math.fabs(minHrsLastCigTotalHrs) > math.fabs(hoursUntilMinLastCigTotalHrs) ) else minHrsLastCigTotalHrs
                    else:
                        closestCig = minHrsLastCigTotalHrs
                else:
                    closestCig = hoursUntilMinLastCigTotalHrs
                priorItem.hoursUntilMinLastCig = closestCig
                priorItem.LapseFlag = ((IsNotNoneOrNan(priorItem.hoursUntilMinLastCig) and priorItem.hoursUntilMinLastCig <= 4 and priorItem.hoursUntilMinLastCig >= 0) \
                    and (IsNotNoneOrNan(priorItem.hrsSnceLstQuit) and  IsNotNoneOrNan(priorItem.maxHoursSinceLastCig ) and priorItem.maxHoursSinceLastCig >= priorItem.hrsSnceLstQuit and priorItem.hrsSnceLstQuit >= 0))
        #endregion

        #region  iterate through the items in and assign the number of lapses to each record
        for osIndex in range(0, len(orderedSublist)):
            thisItem = orderedSublist[osIndex]
            if (osIndex == 0):
                thisItem.PriorLapseCnt = 0
                continue
            priorItem = orderedSublist[osIndex - 1]
            if ((IsNotNoneOrNan(priorItem.hrsSnceLstQuit) and priorItem.hrsSnceLstQuit <= 0) and (IsNotNoneOrNan(thisItem.hrsSnceLstQuit ) and thisItem.hrsSnceLstQuit >= 0)):
                thisItem.PriorLapseCnt = priorItem.PriorLapseCnt + 1
            elif ((IsNotNoneOrNan(priorItem.hrsSnceLstQuit) and priorItem.hrsSnceLstQuit >= 0) and (IsNotNoneOrNan(thisItem.hrsSnceLstQuit))):
                thisItem.PriorLapseCnt = priorItem.PriorLapseCnt + 1
            else:
                thisItem.PriorLapseCnt = priorItem.PriorLapseCnt

def ReplaceSpaceWithNone(val):
    if(val  == "" or val  == " "):
        return None
    else:
        try: 
            float_val =float(val) 
            return None if(math.isnan(float_val)) else float_val 
        except ValueError:
            return val

dataList:List[SmartTData] = []
for dataItem in data.itertuples():
    newSmartTData = SmartTData()
    newSmartTData.CigsJustNow = dataItem.cigjustnow1 if(not ReplaceSpaceWithNone(dataItem.cigjustnow1) == None) else ReplaceSpaceWithNone(dataItem.cigjustnow2)
    newSmartTData.Day = dataItem.ScheduledDay
    newSmartTData.ScheduledTimeDecimal = dataItem.ScheduledTimeDecimal
    newSmartTData.StartTimeDecimal = ReplaceSpaceWithNone(dataItem.StartTimeDecimal)
    newSmartTData.CigsYest = ReplaceSpaceWithNone(dataItem.cigsyest1)
    newSmartTData.LastSmoke = ReplaceSpaceWithNone(dataItem.lastsmoke1)
    newSmartTData.QuitToday = ReplaceSpaceWithNone(dataItem.quittoday)
    newSmartTData.ParticipantId = ReplaceSpaceWithNone(dataItem.ParticipantId)
    newSmartTData.CigsToday1 =ReplaceSpaceWithNone(dataItem.cigstoday1)
    dataList.append(newSmartTData)

dataList = list(FilterByStartTime(dataList))
SetListEmaTakenTime(dataList)

#for dlInd in range(200):Vj
#    print("%d  - %s" % (dataList[dlInd].ParticipantId,dataList[dlInd].CigsJustNow))
CalculateSmartFeatures(dataList)

#print(len(dataList))

filteredSublist= sorted(dataList, key = lambda dItem: dItem.emaTakenTime)  
filteredSublist2 = sorted(filteredSublist, key = lambda dItem: dItem.Day)  
dataList = sorted(filteredSublist2, key = lambda dItem: dItem.ParticipantId)  
#print(dataList[0].getHeaders())

fh = open("output.csv","w")
#fh.write("ParticipantId,ScheduledDay,ScheduledTimeDecimal,LapseFlag\n")
#fh.write(dataList[0].getHeaders())
fh.write("ParticipantId,QuitToday,hrsSnceLstQuit,index,CigsJustNow,CigsToday1,emaTakenTime,minHrsLastCig,maxHoursSinceLastCig,ScheduledTimeDecimal,StartTimeDecimal,LastSmoke,CigsYest,Day,hoursUntilMinLastCig,fieldUsedForLastCig,LapseFlag,RecordTYpe\n")
for dlInd in range(len(dataList)):
    currentItem  = dataList[dlInd]
    if(currentItem.ParticipantId == 4113 and currentItem.Day == 8 and currentItem.StartTimeDecimal == 20.008):
        print(currentItem.getRows())
    #fh.write("%d,%d,%s,%s\n" % (dataList[dlInd].ParticipantId,dataList[dlInd].Day, dataList[dlInd].ScheduledTimeDecimal, 0 if( dataList[dlInd].LapseFlag == False) else 1 ))
    #fh.write(dataList[dlInd].getRows())
    #fh.write(" %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % 
    fh.write("  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s\n" % 
                (dataList[dlInd].ParticipantId, \
                dataList[dlInd].QuitToday, \
                dataList[dlInd].hrsSnceLstQuit, \
                dataList[dlInd].index, \
                dataList[dlInd].CigsJustNow, \
                dataList[dlInd].CigsToday1, \
                dataList[dlInd].emaTakenTime, \
                dataList[dlInd].minHrsLastCig, \
                dataList[dlInd].maxHoursSinceLastCig, \
                dataList[dlInd].ScheduledTimeDecimal, \
                dataList[dlInd].StartTimeDecimal, \
                dataList[dlInd].LastSmoke, \
                dataList[dlInd].CigsYest, \
                dataList[dlInd].Day, \
                dataList[dlInd].hoursUntilMinLastCig, \
                dataList[dlInd].fieldUsedForLastCig, \
                0 if( dataList[dlInd].LapseFlag == False) else 1, \
                "python"))

fh.close()

